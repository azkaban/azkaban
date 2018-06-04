/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.viewer.hdfs;

import azkaban.security.commons.HadoopSecurityManager;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableName;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/**
 * Our forked version of hadoop sequence file.
 * it is to deal with sequence file possibly keep open connections and snap up
 * ports
 */
@SuppressWarnings("deprecation")
public class AzkabanSequenceFileReader {
  private final static Logger LOG = Logger
      .getLogger(HadoopSecurityManager.class);
  private static final byte BLOCK_COMPRESS_VERSION = (byte) 4;
  private static final byte CUSTOM_COMPRESS_VERSION = (byte) 5;
  private static final byte VERSION_WITH_METADATA = (byte) 6;
  private static final int SYNC_ESCAPE = -1; // "length" of sync entries
  private static final int SYNC_HASH_SIZE = 16; // number of bytes in hash
  private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash
  /** The number of bytes between sync points. */
  public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;
  private static final byte[] VERSION = new byte[]{(byte) 'S', (byte) 'E',
      (byte) 'Q', VERSION_WITH_METADATA};

  private AzkabanSequenceFileReader() {
  } // no public ctor

  /**
   * The compression type used to compress key/value pairs in the
   * {@link AzkabanSequenceFileReader}.
   *
   * @see AzkabanSequenceFileReader.Writer
   */
  public static enum CompressionType {
    /** Do not compress records. */
    NONE,
    /** Compress values only, each separately. */
    RECORD,
    /** Compress sequences of records together in blocks. */
    BLOCK
  }

  private static class UncompressedBytes implements ValueBytes {
    private int dataSize;
    private byte[] data;

    private UncompressedBytes() {
      this.data = null;
      this.dataSize = 0;
    }

    private void reset(final DataInputStream in, final int length) throws IOException {
      this.data = new byte[length];
      this.dataSize = -1;

      in.readFully(this.data);
      this.dataSize = this.data.length;
    }

    @Override
    public int getSize() {
      return this.dataSize;
    }

    @Override
    public void writeUncompressedBytes(final DataOutputStream outStream)
        throws IOException {
      outStream.write(this.data, 0, this.dataSize);
    }

    @Override
    public void writeCompressedBytes(final DataOutputStream outStream)
        throws IllegalArgumentException, IOException {
      throw new IllegalArgumentException(
          "UncompressedBytes cannot be compressed!");
    }

  } // UncompressedBytes

  private static class CompressedBytes implements ValueBytes {
    DataInputBuffer rawData = null;
    CompressionCodec codec = null;
    CompressionInputStream decompressedStream = null;
    private int dataSize;
    private byte[] data;

    private CompressedBytes(final CompressionCodec codec) {
      this.data = null;
      this.dataSize = 0;
      this.codec = codec;
    }

    private void reset(final DataInputStream in, final int length) throws IOException {
      this.data = new byte[length];
      this.dataSize = -1;

      in.readFully(this.data);
      this.dataSize = this.data.length;
    }

    @Override
    public int getSize() {
      return this.dataSize;
    }

    @Override
    public void writeUncompressedBytes(final DataOutputStream outStream)
        throws IOException {
      if (this.decompressedStream == null) {
        this.rawData = new DataInputBuffer();
        this.decompressedStream = this.codec.createInputStream(this.rawData);
      } else {
        this.decompressedStream.resetState();
      }
      this.rawData.reset(this.data, 0, this.dataSize);

      final byte[] buffer = new byte[8192];
      int bytesRead = 0;
      while ((bytesRead = this.decompressedStream.read(buffer, 0, 8192)) != -1) {
        outStream.write(buffer, 0, bytesRead);
      }
    }

    @Override
    public void writeCompressedBytes(final DataOutputStream outStream)
        throws IllegalArgumentException, IOException {
      outStream.write(this.data, 0, this.dataSize);
    }

  } // CompressedBytes

  /** Reads key/value pairs from a sequence-format file. */
  public static class Reader implements Closeable {

    private final Path file;
    private final DataOutputBuffer outBuf = new DataOutputBuffer();
    private final byte[] sync = new byte[SYNC_HASH_SIZE];
    private final byte[] syncCheck = new byte[SYNC_HASH_SIZE];
    private final long end;
    private final Configuration conf;
    private final boolean lazyDecompress = true;
    private FSDataInputStream in;
    private byte version;
    private String keyClassName;
    private String valClassName;
    @SuppressWarnings("rawtypes")
    private Class keyClass;
    @SuppressWarnings("rawtypes")
    private Class valClass;
    private CompressionCodec codec = null;
    private Metadata metadata = null;
    private boolean syncSeen;
    private int keyLength;
    private int recordLength;
    private boolean decompress;
    private boolean blockCompressed;
    private int noBufferedRecords = 0;
    private boolean valuesDecompressed = true;

    private int noBufferedKeys = 0;
    private int noBufferedValues = 0;

    private DataInputBuffer keyLenBuffer = null;
    private CompressionInputStream keyLenInFilter = null;
    private DataInputStream keyLenIn = null;
    private Decompressor keyLenDecompressor = null;
    private DataInputBuffer keyBuffer = null;
    private CompressionInputStream keyInFilter = null;
    private DataInputStream keyIn = null;
    private Decompressor keyDecompressor = null;

    private DataInputBuffer valLenBuffer = null;
    private CompressionInputStream valLenInFilter = null;
    private DataInputStream valLenIn = null;
    private Decompressor valLenDecompressor = null;
    private DataInputBuffer valBuffer = null;
    private CompressionInputStream valInFilter = null;
    private DataInputStream valIn = null;
    private Decompressor valDecompressor = null;

    @SuppressWarnings("rawtypes")
    private Deserializer keyDeserializer;
    @SuppressWarnings("rawtypes")
    private Deserializer valDeserializer;

    /** Open the named file. */
    public Reader(final FileSystem fs, final Path file, final Configuration conf)
        throws IOException {
      this(fs, file, conf.getInt("io.file.buffer.size", 4096), conf, false);
    }

    private Reader(final FileSystem fs, final Path file, final int bufferSize,
        final Configuration conf, final boolean tempReader) throws IOException {
      this(fs, file, bufferSize, 0, fs.getLength(file), conf, tempReader);
    }

    private Reader(final FileSystem fs, final Path file, final int bufferSize, final long start,
        final long length, final Configuration conf, final boolean tempReader) throws IOException {
      this.file = file;

      try {
        this.in = openFile(fs, file, bufferSize, length);
        this.conf = conf;
        seek(start);
        this.end = this.in.getPos() + length;
        init(tempReader);
      } catch (final IOException e) {
        if (this.in != null) {
          this.in.close();
        }
        throw e;
      }
    }

    /**
     * Override this method to specialize the type of {@link FSDataInputStream}
     * returned.
     */
    protected FSDataInputStream openFile(final FileSystem fs, final Path file,
        final int bufferSize, final long length) throws IOException {
      return fs.open(file, bufferSize);
    }

    /**
     * Initialize the {@link Reader}
     *
     * @param tmpReader <code>true</code> if we are constructing a temporary
     *          reader
     *          {@link AzkabanSequenceFileReader.Sorter.cloneFileAttributes},
     *          and hence do not initialize every component; <code>false</code>
     *          otherwise.
     * @throws IOException
     */
    private void init(final boolean tempReader) throws IOException {
      final byte[] versionBlock = new byte[VERSION.length];
      this.in.readFully(versionBlock);

      if ((versionBlock[0] != VERSION[0]) || (versionBlock[1] != VERSION[1])
          || (versionBlock[2] != VERSION[2])) {
        throw new IOException(this.file + " not a SequenceFile");
      }

      // Set 'version'
      this.version = versionBlock[3];
      if (this.version > VERSION[3]) {
        throw new VersionMismatchException(VERSION[3], this.version);
      }

      if (this.version < BLOCK_COMPRESS_VERSION) {
        final UTF8 className = new UTF8();

        className.readFields(this.in);
        this.keyClassName = className.toString(); // key class name

        className.readFields(this.in);
        this.valClassName = className.toString(); // val class name
      } else {
        this.keyClassName = Text.readString(this.in);
        this.valClassName = Text.readString(this.in);
      }

      if (this.version > 2) { // if version > 2
        this.decompress = this.in.readBoolean(); // is compressed?
      } else {
        this.decompress = false;
      }

      if (this.version >= BLOCK_COMPRESS_VERSION) { // if version >= 4
        this.blockCompressed = this.in.readBoolean(); // is block-compressed?
      } else {
        this.blockCompressed = false;
      }

      // if version >= 5
      // setup the compression codec
      if (this.decompress) {
        if (this.version >= CUSTOM_COMPRESS_VERSION) {
          final String codecClassname = Text.readString(this.in);
          try {
            final Class<? extends CompressionCodec> codecClass =
                this.conf.getClassByName(codecClassname).asSubclass(
                    CompressionCodec.class);
            this.codec = ReflectionUtils.newInstance(codecClass, this.conf);
          } catch (final ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("Unknown codec: "
                + codecClassname, cnfe);
          }
        } else {
          this.codec = new DefaultCodec();
          ((Configurable) this.codec).setConf(this.conf);
        }
      }

      this.metadata = new Metadata();
      if (this.version >= VERSION_WITH_METADATA) { // if version >= 6
        this.metadata.readFields(this.in);
      }

      if (this.version > 1) { // if version > 1
        this.in.readFully(this.sync); // read sync bytes
      }

      // Initialize... *not* if this we are constructing a temporary Reader
      if (!tempReader) {
        this.valBuffer = new DataInputBuffer();
        if (this.decompress) {
          this.valDecompressor = CodecPool.getDecompressor(this.codec);
          this.valInFilter = this.codec.createInputStream(this.valBuffer, this.valDecompressor);
          this.valIn = new DataInputStream(this.valInFilter);
        } else {
          this.valIn = this.valBuffer;
        }

        if (this.blockCompressed) {
          this.keyLenBuffer = new DataInputBuffer();
          this.keyBuffer = new DataInputBuffer();
          this.valLenBuffer = new DataInputBuffer();

          this.keyLenDecompressor = CodecPool.getDecompressor(this.codec);
          this.keyLenInFilter =
              this.codec.createInputStream(this.keyLenBuffer, this.keyLenDecompressor);
          this.keyLenIn = new DataInputStream(this.keyLenInFilter);

          this.keyDecompressor = CodecPool.getDecompressor(this.codec);
          this.keyInFilter = this.codec.createInputStream(this.keyBuffer, this.keyDecompressor);
          this.keyIn = new DataInputStream(this.keyInFilter);

          this.valLenDecompressor = CodecPool.getDecompressor(this.codec);
          this.valLenInFilter =
              this.codec.createInputStream(this.valLenBuffer, this.valLenDecompressor);
          this.valLenIn = new DataInputStream(this.valLenInFilter);
        }

        final SerializationFactory serializationFactory =
            new SerializationFactory(this.conf);
        this.keyDeserializer =
            getDeserializer(serializationFactory, getKeyClass());
        if (!this.blockCompressed) {
          this.keyDeserializer.open(this.valBuffer);
        } else {
          this.keyDeserializer.open(this.keyIn);
        }
        this.valDeserializer =
            getDeserializer(serializationFactory, getValueClass());
        this.valDeserializer.open(this.valIn);
      }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Deserializer getDeserializer(final SerializationFactory sf, final Class c) {
      return sf.getDeserializer(c);
    }

    /** Close the file. */
    @Override
    public synchronized void close() throws IOException {
      // Return the decompressors to the pool
      CodecPool.returnDecompressor(this.keyLenDecompressor);
      CodecPool.returnDecompressor(this.keyDecompressor);
      CodecPool.returnDecompressor(this.valLenDecompressor);
      CodecPool.returnDecompressor(this.valDecompressor);
      this.keyLenDecompressor = this.keyDecompressor = null;
      this.valLenDecompressor = this.valDecompressor = null;

      if (this.keyDeserializer != null) {
        this.keyDeserializer.close();
      }
      if (this.valDeserializer != null) {
        this.valDeserializer.close();
      }

      // Close the input-stream
      if (this.in != null) {
        this.in.close();
      }
    }

    /** Returns the name of the key class. */
    public String getKeyClassName() {
      return this.keyClassName;
    }

    /** Returns the class of keys in this file. */
    public synchronized Class<?> getKeyClass() {
      if (null == this.keyClass) {
        try {
          this.keyClass = WritableName.getClass(getKeyClassName(), this.conf);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }
      return this.keyClass;
    }

    /** Returns the name of the value class. */
    public String getValueClassName() {
      return this.valClassName;
    }

    /** Returns the class of values in this file. */
    public synchronized Class<?> getValueClass() {
      if (null == this.valClass) {
        try {
          this.valClass = WritableName.getClass(getValueClassName(), this.conf);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }
      return this.valClass;
    }

    /** Returns true if values are compressed. */
    public boolean isCompressed() {
      return this.decompress;
    }

    /** Returns true if records are block-compressed. */
    public boolean isBlockCompressed() {
      return this.blockCompressed;
    }

    /** Returns the compression codec of data in this file. */
    public CompressionCodec getCompressionCodec() {
      return this.codec;
    }

    /** Returns the metadata object of the file */
    public Metadata getMetadata() {
      return this.metadata;
    }

    /** Returns the configuration used for this file. */
    Configuration getConf() {
      return this.conf;
    }

    /** Read a compressed buffer */
    private synchronized void readBuffer(final DataInputBuffer buffer,
        final CompressionInputStream filter) throws IOException {
      // Read data into a temporary buffer
      final DataOutputBuffer dataBuffer = new DataOutputBuffer();

      try {
        final int dataBufferLength = WritableUtils.readVInt(this.in);
        dataBuffer.write(this.in, dataBufferLength);

        // Set up 'buffer' connected to the input-stream
        buffer.reset(dataBuffer.getData(), 0, dataBuffer.getLength());
      } finally {
        dataBuffer.close();
      }

      // Reset the codec
      filter.resetState();
    }

    /** Read the next 'compressed' block */
    private synchronized void readBlock() throws IOException {
      // Check if we need to throw away a whole block of
      // 'values' due to 'lazy decompression'
      if (this.lazyDecompress && !this.valuesDecompressed) {
        this.in.seek(WritableUtils.readVInt(this.in) + this.in.getPos());
        this.in.seek(WritableUtils.readVInt(this.in) + this.in.getPos());
      }

      // Reset internal states
      this.noBufferedKeys = 0;
      this.noBufferedValues = 0;
      this.noBufferedRecords = 0;
      this.valuesDecompressed = false;

      // Process sync
      if (this.sync != null) {
        this.in.readInt();
        this.in.readFully(this.syncCheck); // read syncCheck
        if (!Arrays.equals(this.sync, this.syncCheck)) // check it
          throw new IOException("File is corrupt!");
      }
      this.syncSeen = true;

      // Read number of records in this block
      this.noBufferedRecords = WritableUtils.readVInt(this.in);

      // Read key lengths and keys
      readBuffer(this.keyLenBuffer, this.keyLenInFilter);
      readBuffer(this.keyBuffer, this.keyInFilter);
      this.noBufferedKeys = this.noBufferedRecords;

      // Read value lengths and values
      if (!this.lazyDecompress) {
        readBuffer(this.valLenBuffer, this.valLenInFilter);
        readBuffer(this.valBuffer, this.valInFilter);
        this.noBufferedValues = this.noBufferedRecords;
        this.valuesDecompressed = true;
      }
    }

    /**
     * Position valLenIn/valIn to the 'value' corresponding to the 'current' key
     */
    private synchronized void seekToCurrentValue() throws IOException {
      if (!this.blockCompressed) {
        if (this.decompress) {
          this.valInFilter.resetState();
        }
        this.valBuffer.reset();
      } else {
        // Check if this is the first value in the 'block' to be read
        if (this.lazyDecompress && !this.valuesDecompressed) {
          // Read the value lengths and values
          readBuffer(this.valLenBuffer, this.valLenInFilter);
          readBuffer(this.valBuffer, this.valInFilter);
          this.noBufferedValues = this.noBufferedRecords;
          this.valuesDecompressed = true;
        }

        // Calculate the no. of bytes to skip
        // Note: 'current' key has already been read!
        int skipValBytes = 0;
        final int currentKey = this.noBufferedKeys + 1;
        for (int i = this.noBufferedValues; i > currentKey; --i) {
          skipValBytes += WritableUtils.readVInt(this.valLenIn);
          --this.noBufferedValues;
        }

        // Skip to the 'val' corresponding to 'current' key
        if (skipValBytes > 0) {
          if (this.valIn.skipBytes(skipValBytes) != skipValBytes) {
            throw new IOException("Failed to seek to " + currentKey
                + "(th) value!");
          }
        }
      }
    }

    /**
     * Get the 'value' corresponding to the last read 'key'.
     *
     * @param val : The 'value' to be read.
     * @throws IOException
     */
    public synchronized void getCurrentValue(final Writable val) throws IOException {
      if (val instanceof Configurable) {
        ((Configurable) val).setConf(this.conf);
      }

      // Position stream to 'current' value
      seekToCurrentValue();

      if (!this.blockCompressed) {
        val.readFields(this.valIn);

        if (this.valIn.read() > 0) {
          LOG.info("available bytes: " + this.valIn.available());
          throw new IOException(val + " read "
              + (this.valBuffer.getPosition() - this.keyLength) + " bytes, should read "
              + (this.valBuffer.getLength() - this.keyLength));
        }
      } else {
        // Get the value
        final int valLength = WritableUtils.readVInt(this.valLenIn);
        val.readFields(this.valIn);

        // Read another compressed 'value'
        --this.noBufferedValues;

        // Sanity check
        if (valLength < 0) {
          LOG.debug(val + " is a zero-length value");
        }
      }

    }

    /**
     * Get the 'value' corresponding to the last read 'key'.
     *
     * @param val : The 'value' to be read.
     * @throws IOException
     */
    public synchronized Object getCurrentValue(Object val) throws IOException {
      if (val instanceof Configurable) {
        ((Configurable) val).setConf(this.conf);
      }

      // Position stream to 'current' value
      seekToCurrentValue();

      if (!this.blockCompressed) {
        val = deserializeValue(val);

        if (this.valIn.read() > 0) {
          LOG.info("available bytes: " + this.valIn.available());
          throw new IOException(val + " read "
              + (this.valBuffer.getPosition() - this.keyLength) + " bytes, should read "
              + (this.valBuffer.getLength() - this.keyLength));
        }
      } else {
        // Get the value
        final int valLength = WritableUtils.readVInt(this.valLenIn);
        val = deserializeValue(val);

        // Read another compressed 'value'
        --this.noBufferedValues;

        // Sanity check
        if (valLength < 0) {
          LOG.debug(val + " is a zero-length value");
        }
      }
      return val;

    }

    @SuppressWarnings("unchecked")
    private Object deserializeValue(final Object val) throws IOException {
      return this.valDeserializer.deserialize(val);
    }

    /**
     * Read the next key in the file into <code>key</code>, skipping its value.
     * True if another entry exists, and false at end of file.
     */
    public synchronized boolean next(final Writable key) throws IOException {
      if (key.getClass() != getKeyClass())
        throw new IOException("wrong key class: " + key.getClass().getName()
            + " is not " + this.keyClass);

      if (!this.blockCompressed) {
        this.outBuf.reset();

        this.keyLength = next(this.outBuf);
        if (this.keyLength < 0)
          return false;

        this.valBuffer.reset(this.outBuf.getData(), this.outBuf.getLength());

        key.readFields(this.valBuffer);
        this.valBuffer.mark(0);
        if (this.valBuffer.getPosition() != this.keyLength) {
          throw new IOException(key + " read " + this.valBuffer.getPosition()
              + " bytes, should read " + this.keyLength);
        }
      } else {
        // Reset syncSeen
        this.syncSeen = false;

        if (this.noBufferedKeys == 0) {
          try {
            readBlock();
          } catch (final EOFException eof) {
            return false;
          }
        }

        final int keyLength = WritableUtils.readVInt(this.keyLenIn);

        // Sanity check
        if (keyLength < 0) {
          return false;
        }

        // Read another compressed 'key'
        key.readFields(this.keyIn);
        --this.noBufferedKeys;
      }

      return true;
    }

    /**
     * Read the next key/value pair in the file into <code>key</code> and
     * <code>val</code>. Returns true if such a pair exists and false when at
     * end of file
     */
    public synchronized boolean next(final Writable key, final Writable val)
        throws IOException {
      if (val.getClass() != getValueClass())
        throw new IOException("wrong value class: " + val + " is not "
            + this.valClass);

      final boolean more = next(key);

      if (more) {
        getCurrentValue(val);
      }

      return more;
    }

    /**
     * Read and return the next record length, potentially skipping over a sync
     * block.
     *
     * @return the length of the next record or -1 if there is no next record
     * @throws IOException
     */
    private synchronized int readRecordLength() throws IOException {
      if (this.in.getPos() >= this.end) {
        return -1;
      }
      int length = this.in.readInt();
      if (this.version > 1 && this.sync != null && length == SYNC_ESCAPE) { // process a
                                                                  // sync entry
        this.in.readFully(this.syncCheck); // read syncCheck
        if (!Arrays.equals(this.sync, this.syncCheck)) // check it
          throw new IOException("File is corrupt!");
        this.syncSeen = true;
        if (this.in.getPos() >= this.end) {
          return -1;
        }
        length = this.in.readInt(); // re-read length
      } else {
        this.syncSeen = false;
      }

      return length;
    }

    /**
     * Read the next key/value pair in the file into <code>buffer</code>.
     * Returns the length of the key read, or -1 if at end of file. The length
     * of the value may be computed by calling buffer.getLength() before and
     * after calls to this method.
     */
    /**
     * @deprecated Call
     *             {@link #nextRaw(DataOutputBuffer,SequenceFile.ValueBytes)}.
     */
    public synchronized int next(final DataOutputBuffer buffer) throws IOException {
      // Unsupported for block-compressed sequence files
      if (this.blockCompressed) {
        throw new IOException(
            "Unsupported call for block-compressed"
                + " SequenceFiles - use SequenceFile.Reader.next(DataOutputStream, ValueBytes)");
      }
      try {
        final int length = readRecordLength();
        if (length == -1) {
          return -1;
        }
        final int keyLength = this.in.readInt();
        buffer.write(this.in, length);
        return keyLength;
      } catch (final ChecksumException e) { // checksum failure
        handleChecksumException(e);
        return next(buffer);
      }
    }

    public ValueBytes createValueBytes() {
      ValueBytes val = null;
      if (!this.decompress || this.blockCompressed) {
        val = new UncompressedBytes();
      } else {
        val = new CompressedBytes(this.codec);
      }
      return val;
    }

    /**
     * Read 'raw' records.
     *
     * @param key - The buffer into which the key is read
     * @param val - The 'raw' value
     * @return Returns the total record length or -1 for end of file
     * @throws IOException
     */
    public synchronized int nextRaw(final DataOutputBuffer key, final ValueBytes val)
        throws IOException {
      if (!this.blockCompressed) {
        final int length = readRecordLength();
        if (length == -1) {
          return -1;
        }
        final int keyLength = this.in.readInt();
        final int valLength = length - keyLength;
        key.write(this.in, keyLength);
        if (this.decompress) {
          final CompressedBytes value = (CompressedBytes) val;
          value.reset(this.in, valLength);
        } else {
          final UncompressedBytes value = (UncompressedBytes) val;
          value.reset(this.in, valLength);
        }

        return length;
      } else {
        // Reset syncSeen
        this.syncSeen = false;

        // Read 'key'
        if (this.noBufferedKeys == 0) {
          if (this.in.getPos() >= this.end)
            return -1;

          try {
            readBlock();
          } catch (final EOFException eof) {
            return -1;
          }
        }
        final int keyLength = WritableUtils.readVInt(this.keyLenIn);
        if (keyLength < 0) {
          throw new IOException("zero length key found!");
        }
        key.write(this.keyIn, keyLength);
        --this.noBufferedKeys;

        // Read raw 'value'
        seekToCurrentValue();
        final int valLength = WritableUtils.readVInt(this.valLenIn);
        final UncompressedBytes rawValue = (UncompressedBytes) val;
        rawValue.reset(this.valIn, valLength);
        --this.noBufferedValues;

        return (keyLength + valLength);
      }

    }

    /**
     * Read 'raw' keys.
     *
     * @param key - The buffer into which the key is read
     * @return Returns the key length or -1 for end of file
     * @throws IOException
     */
    public int nextRawKey(final DataOutputBuffer key) throws IOException {
      if (!this.blockCompressed) {
        this.recordLength = readRecordLength();
        if (this.recordLength == -1) {
          return -1;
        }
        this.keyLength = this.in.readInt();
        key.write(this.in, this.keyLength);
        return this.keyLength;
      } else {
        // Reset syncSeen
        this.syncSeen = false;

        // Read 'key'
        if (this.noBufferedKeys == 0) {
          if (this.in.getPos() >= this.end)
            return -1;

          try {
            readBlock();
          } catch (final EOFException eof) {
            return -1;
          }
        }
        final int keyLength = WritableUtils.readVInt(this.keyLenIn);
        if (keyLength < 0) {
          throw new IOException("zero length key found!");
        }
        key.write(this.keyIn, keyLength);
        --this.noBufferedKeys;

        return keyLength;
      }

    }

    /**
     * Read the next key in the file, skipping its value. Return null at end of
     * file.
     */
    public synchronized Object next(Object key) throws IOException {
      if (key != null && key.getClass() != getKeyClass()) {
        throw new IOException("wrong key class: " + key.getClass().getName()
            + " is not " + this.keyClass);
      }

      if (!this.blockCompressed) {
        this.outBuf.reset();

        this.keyLength = next(this.outBuf);
        if (this.keyLength < 0)
          return null;

        this.valBuffer.reset(this.outBuf.getData(), this.outBuf.getLength());

        key = deserializeKey(key);
        this.valBuffer.mark(0);
        if (this.valBuffer.getPosition() != this.keyLength) {
          throw new IOException(key + " read " + this.valBuffer.getPosition()
              + " bytes, should read " + this.keyLength);
        }
      } else {
        // Reset syncSeen
        this.syncSeen = false;

        if (this.noBufferedKeys == 0) {
          try {
            readBlock();
          } catch (final EOFException eof) {
            return null;
          }
        }

        final int keyLength = WritableUtils.readVInt(this.keyLenIn);

        // Sanity check
        if (keyLength < 0) {
          return null;
        }

        // Read another compressed 'key'
        key = deserializeKey(key);
        --this.noBufferedKeys;
      }

      return key;
    }

    @SuppressWarnings("unchecked")
    private Object deserializeKey(final Object key) throws IOException {
      return this.keyDeserializer.deserialize(key);
    }

    /**
     * Read 'raw' values.
     *
     * @param val - The 'raw' value
     * @return Returns the value length
     * @throws IOException
     */
    public synchronized int nextRawValue(final ValueBytes val) throws IOException {

      // Position stream to current value
      seekToCurrentValue();

      if (!this.blockCompressed) {
        final int valLength = this.recordLength - this.keyLength;
        if (this.decompress) {
          final CompressedBytes value = (CompressedBytes) val;
          value.reset(this.in, valLength);
        } else {
          final UncompressedBytes value = (UncompressedBytes) val;
          value.reset(this.in, valLength);
        }

        return valLength;
      } else {
        final int valLength = WritableUtils.readVInt(this.valLenIn);
        final UncompressedBytes rawValue = (UncompressedBytes) val;
        rawValue.reset(this.valIn, valLength);
        --this.noBufferedValues;
        return valLength;
      }

    }

    private void handleChecksumException(final ChecksumException e)
        throws IOException {
      if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
        LOG.warn("Bad checksum at " + getPosition() + ". Skipping entries.");
        sync(getPosition() + this.conf.getInt("io.bytes.per.checksum", 512));
      } else {
        throw e;
      }
    }

    /**
     * Set the current byte position in the input file.
     *
     * <p>
     * The position passed must be a position returned by
     * {@link AzkabanSequenceFileReader.Writer#getLength()} when writing this
     * file. To seek to an arbitrary position, use
     * {@link Reader#sync(long)}.
     */
    public synchronized void seek(final long position) throws IOException {
      this.in.seek(position);
      if (this.blockCompressed) { // trigger block read
        this.noBufferedKeys = 0;
        this.valuesDecompressed = true;
      }
    }

    /** Seek to the next sync mark past a given position. */
    public synchronized void sync(final long position) throws IOException {
      if (position + SYNC_SIZE >= this.end) {
        seek(this.end);
        return;
      }

      try {
        seek(position + 4); // skip escape
        this.in.readFully(this.syncCheck);
        final int syncLen = this.sync.length;
        for (int i = 0; this.in.getPos() < this.end; i++) {
          int j = 0;
          for (; j < syncLen; j++) {
            if (this.sync[j] != this.syncCheck[(i + j) % syncLen])
              break;
          }
          if (j == syncLen) {
            this.in.seek(this.in.getPos() - SYNC_SIZE); // position before sync
            return;
          }
          this.syncCheck[i % syncLen] = this.in.readByte();
        }
      } catch (final ChecksumException e) { // checksum failure
        handleChecksumException(e);
      }
    }

    /** Returns true iff the previous call to next passed a sync mark. */
    public boolean syncSeen() {
      return this.syncSeen;
    }

    /** Return the current byte position in the input file. */
    public synchronized long getPosition() throws IOException {
      return this.in.getPos();
    }

    @Override
    /** Returns the name of the file. */
    public String toString() {
      return this.file.toString();
    }

  }

} // SequenceFile
