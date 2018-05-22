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

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
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
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;

/**
 * Our forked version of hadoop sequence file.
 * it is to deal with sequence file possibly keep open connections and snap up
 * ports
 */
@SuppressWarnings("deprecation")
public class AzkabanSequenceFileReader {
  private final static Logger LOG = Logger
      .getLogger(HadoopSecurityManager.class);

  private AzkabanSequenceFileReader() {
  } // no public ctor

  private static final byte BLOCK_COMPRESS_VERSION = (byte) 4;
  private static final byte CUSTOM_COMPRESS_VERSION = (byte) 5;
  private static final byte VERSION_WITH_METADATA = (byte) 6;
  private static byte[] VERSION = new byte[] { (byte) 'S', (byte) 'E',
      (byte) 'Q', VERSION_WITH_METADATA };

  private static final int SYNC_ESCAPE = -1; // "length" of sync entries
  private static final int SYNC_HASH_SIZE = 16; // number of bytes in hash
  private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash

  /** The number of bytes between sync points. */
  public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;

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
      data = null;
      dataSize = 0;
    }

    private void reset(DataInputStream in, int length) throws IOException {
      data = new byte[length];
      dataSize = -1;

      in.readFully(data);
      dataSize = data.length;
    }

    public int getSize() {
      return dataSize;
    }

    public void writeUncompressedBytes(DataOutputStream outStream)
        throws IOException {
      outStream.write(data, 0, dataSize);
    }

    public void writeCompressedBytes(DataOutputStream outStream)
        throws IllegalArgumentException, IOException {
      throw new IllegalArgumentException(
          "UncompressedBytes cannot be compressed!");
    }

  } // UncompressedBytes

  private static class CompressedBytes implements ValueBytes {
    private int dataSize;
    private byte[] data;
    DataInputBuffer rawData = null;
    CompressionCodec codec = null;
    CompressionInputStream decompressedStream = null;

    private CompressedBytes(CompressionCodec codec) {
      data = null;
      dataSize = 0;
      this.codec = codec;
    }

    private void reset(DataInputStream in, int length) throws IOException {
      data = new byte[length];
      dataSize = -1;

      in.readFully(data);
      dataSize = data.length;
    }

    public int getSize() {
      return dataSize;
    }

    public void writeUncompressedBytes(DataOutputStream outStream)
        throws IOException {
      if (decompressedStream == null) {
        rawData = new DataInputBuffer();
        decompressedStream = codec.createInputStream(rawData);
      } else {
        decompressedStream.resetState();
      }
      rawData.reset(data, 0, dataSize);

      byte[] buffer = new byte[8192];
      int bytesRead = 0;
      while ((bytesRead = decompressedStream.read(buffer, 0, 8192)) != -1) {
        outStream.write(buffer, 0, bytesRead);
      }
    }

    public void writeCompressedBytes(DataOutputStream outStream)
        throws IllegalArgumentException, IOException {
      outStream.write(data, 0, dataSize);
    }

  } // CompressedBytes

  /** Reads key/value pairs from a sequence-format file. */
  public static class Reader implements Closeable {
    private Path file;
    private FSDataInputStream in;
    private DataOutputBuffer outBuf = new DataOutputBuffer();

    private byte version;

    private String keyClassName;
    private String valClassName;
    @SuppressWarnings("rawtypes")
    private Class keyClass;
    @SuppressWarnings("rawtypes")
    private Class valClass;

    private CompressionCodec codec = null;
    private Metadata metadata = null;

    private byte[] sync = new byte[SYNC_HASH_SIZE];
    private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
    private boolean syncSeen;

    private long end;
    private int keyLength;
    private int recordLength;

    private boolean decompress;
    private boolean blockCompressed;

    private Configuration conf;

    private int noBufferedRecords = 0;
    private boolean lazyDecompress = true;
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
    public Reader(FileSystem fs, Path file, Configuration conf)
        throws IOException {
      this(fs, file, conf.getInt("io.file.buffer.size", 4096), conf, false);
    }

    private Reader(FileSystem fs, Path file, int bufferSize,
        Configuration conf, boolean tempReader) throws IOException {
      this(fs, file, bufferSize, 0, fs.getLength(file), conf, tempReader);
    }

    private Reader(FileSystem fs, Path file, int bufferSize, long start,
        long length, Configuration conf, boolean tempReader) throws IOException {
      this.file = file;

      try {
        this.in = openFile(fs, file, bufferSize, length);
        this.conf = conf;
        seek(start);
        this.end = in.getPos() + length;
        init(tempReader);
      } catch (IOException e) {
        if (this.in != null) {
          in.close();
        }
        throw e;
      }
    }

    /**
     * Override this method to specialize the type of {@link FSDataInputStream}
     * returned.
     */
    protected FSDataInputStream openFile(FileSystem fs, Path file,
        int bufferSize, long length) throws IOException {
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
    private void init(boolean tempReader) throws IOException {
      byte[] versionBlock = new byte[VERSION.length];
      in.readFully(versionBlock);

      if ((versionBlock[0] != VERSION[0]) || (versionBlock[1] != VERSION[1])
          || (versionBlock[2] != VERSION[2]))
        throw new IOException(file + " not a SequenceFile");

      // Set 'version'
      version = versionBlock[3];
      if (version > VERSION[3])
        throw new VersionMismatchException(VERSION[3], version);

      if (version < BLOCK_COMPRESS_VERSION) {
        UTF8 className = new UTF8();

        className.readFields(in);
        keyClassName = className.toString(); // key class name

        className.readFields(in);
        valClassName = className.toString(); // val class name
      } else {
        keyClassName = Text.readString(in);
        valClassName = Text.readString(in);
      }

      if (version > 2) { // if version > 2
        this.decompress = in.readBoolean(); // is compressed?
      } else {
        decompress = false;
      }

      if (version >= BLOCK_COMPRESS_VERSION) { // if version >= 4
        this.blockCompressed = in.readBoolean(); // is block-compressed?
      } else {
        blockCompressed = false;
      }

      // if version >= 5
      // setup the compression codec
      if (decompress) {
        if (version >= CUSTOM_COMPRESS_VERSION) {
          String codecClassname = Text.readString(in);
          try {
            Class<? extends CompressionCodec> codecClass =
                conf.getClassByName(codecClassname).asSubclass(
                    CompressionCodec.class);
            this.codec = ReflectionUtils.newInstance(codecClass, conf);
          } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("Unknown codec: "
                + codecClassname, cnfe);
          }
        } else {
          codec = new DefaultCodec();
          ((Configurable) codec).setConf(conf);
        }
      }

      this.metadata = new Metadata();
      if (version >= VERSION_WITH_METADATA) { // if version >= 6
        this.metadata.readFields(in);
      }

      if (version > 1) { // if version > 1
        in.readFully(sync); // read sync bytes
      }

      // Initialize... *not* if this we are constructing a temporary Reader
      if (!tempReader) {
        valBuffer = new DataInputBuffer();
        if (decompress) {
          valDecompressor = CodecPool.getDecompressor(codec);
          valInFilter = codec.createInputStream(valBuffer, valDecompressor);
          valIn = new DataInputStream(valInFilter);
        } else {
          valIn = valBuffer;
        }

        if (blockCompressed) {
          keyLenBuffer = new DataInputBuffer();
          keyBuffer = new DataInputBuffer();
          valLenBuffer = new DataInputBuffer();

          keyLenDecompressor = CodecPool.getDecompressor(codec);
          keyLenInFilter =
              codec.createInputStream(keyLenBuffer, keyLenDecompressor);
          keyLenIn = new DataInputStream(keyLenInFilter);

          keyDecompressor = CodecPool.getDecompressor(codec);
          keyInFilter = codec.createInputStream(keyBuffer, keyDecompressor);
          keyIn = new DataInputStream(keyInFilter);

          valLenDecompressor = CodecPool.getDecompressor(codec);
          valLenInFilter =
              codec.createInputStream(valLenBuffer, valLenDecompressor);
          valLenIn = new DataInputStream(valLenInFilter);
        }

        SerializationFactory serializationFactory =
            new SerializationFactory(conf);
        this.keyDeserializer =
            getDeserializer(serializationFactory, getKeyClass());
        if (!blockCompressed) {
          this.keyDeserializer.open(valBuffer);
        } else {
          this.keyDeserializer.open(keyIn);
        }
        this.valDeserializer =
            getDeserializer(serializationFactory, getValueClass());
        this.valDeserializer.open(valIn);
      }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Deserializer getDeserializer(SerializationFactory sf, Class c) {
      return sf.getDeserializer(c);
    }

    /** Close the file. */
    public synchronized void close() throws IOException {
      // Return the decompressors to the pool
      CodecPool.returnDecompressor(keyLenDecompressor);
      CodecPool.returnDecompressor(keyDecompressor);
      CodecPool.returnDecompressor(valLenDecompressor);
      CodecPool.returnDecompressor(valDecompressor);
      keyLenDecompressor = keyDecompressor = null;
      valLenDecompressor = valDecompressor = null;

      if (keyDeserializer != null) {
        keyDeserializer.close();
      }
      if (valDeserializer != null) {
        valDeserializer.close();
      }

      // Close the input-stream
      if (in != null) {
        in.close();
      }
    }

    /** Returns the name of the key class. */
    public String getKeyClassName() {
      return keyClassName;
    }

    /** Returns the class of keys in this file. */
    public synchronized Class<?> getKeyClass() {
      if (null == keyClass) {
        try {
          keyClass = WritableName.getClass(getKeyClassName(), conf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return keyClass;
    }

    /** Returns the name of the value class. */
    public String getValueClassName() {
      return valClassName;
    }

    /** Returns the class of values in this file. */
    public synchronized Class<?> getValueClass() {
      if (null == valClass) {
        try {
          valClass = WritableName.getClass(getValueClassName(), conf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return valClass;
    }

    /** Returns true if values are compressed. */
    public boolean isCompressed() {
      return decompress;
    }

    /** Returns true if records are block-compressed. */
    public boolean isBlockCompressed() {
      return blockCompressed;
    }

    /** Returns the compression codec of data in this file. */
    public CompressionCodec getCompressionCodec() {
      return codec;
    }

    /** Returns the metadata object of the file */
    public Metadata getMetadata() {
      return this.metadata;
    }

    /** Returns the configuration used for this file. */
    Configuration getConf() {
      return conf;
    }

    /** Read a compressed buffer */
    private synchronized void readBuffer(DataInputBuffer buffer,
        CompressionInputStream filter) throws IOException {
      // Read data into a temporary buffer
      DataOutputBuffer dataBuffer = new DataOutputBuffer();

      try {
        int dataBufferLength = WritableUtils.readVInt(in);
        dataBuffer.write(in, dataBufferLength);

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
      if (lazyDecompress && !valuesDecompressed) {
        in.seek(WritableUtils.readVInt(in) + in.getPos());
        in.seek(WritableUtils.readVInt(in) + in.getPos());
      }

      // Reset internal states
      noBufferedKeys = 0;
      noBufferedValues = 0;
      noBufferedRecords = 0;
      valuesDecompressed = false;

      // Process sync
      if (sync != null) {
        in.readInt();
        in.readFully(syncCheck); // read syncCheck
        if (!Arrays.equals(sync, syncCheck)) // check it
          throw new IOException("File is corrupt!");
      }
      syncSeen = true;

      // Read number of records in this block
      noBufferedRecords = WritableUtils.readVInt(in);

      // Read key lengths and keys
      readBuffer(keyLenBuffer, keyLenInFilter);
      readBuffer(keyBuffer, keyInFilter);
      noBufferedKeys = noBufferedRecords;

      // Read value lengths and values
      if (!lazyDecompress) {
        readBuffer(valLenBuffer, valLenInFilter);
        readBuffer(valBuffer, valInFilter);
        noBufferedValues = noBufferedRecords;
        valuesDecompressed = true;
      }
    }

    /**
     * Position valLenIn/valIn to the 'value' corresponding to the 'current' key
     */
    private synchronized void seekToCurrentValue() throws IOException {
      if (!blockCompressed) {
        if (decompress) {
          valInFilter.resetState();
        }
        valBuffer.reset();
      } else {
        // Check if this is the first value in the 'block' to be read
        if (lazyDecompress && !valuesDecompressed) {
          // Read the value lengths and values
          readBuffer(valLenBuffer, valLenInFilter);
          readBuffer(valBuffer, valInFilter);
          noBufferedValues = noBufferedRecords;
          valuesDecompressed = true;
        }

        // Calculate the no. of bytes to skip
        // Note: 'current' key has already been read!
        int skipValBytes = 0;
        int currentKey = noBufferedKeys + 1;
        for (int i = noBufferedValues; i > currentKey; --i) {
          skipValBytes += WritableUtils.readVInt(valLenIn);
          --noBufferedValues;
        }

        // Skip to the 'val' corresponding to 'current' key
        if (skipValBytes > 0) {
          if (valIn.skipBytes(skipValBytes) != skipValBytes) {
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
    public synchronized void getCurrentValue(Writable val) throws IOException {
      if (val instanceof Configurable) {
        ((Configurable) val).setConf(this.conf);
      }

      // Position stream to 'current' value
      seekToCurrentValue();

      if (!blockCompressed) {
        val.readFields(valIn);

        if (valIn.read() > 0) {
          LOG.info("available bytes: " + valIn.available());
          throw new IOException(val + " read "
              + (valBuffer.getPosition() - keyLength) + " bytes, should read "
              + (valBuffer.getLength() - keyLength));
        }
      } else {
        // Get the value
        int valLength = WritableUtils.readVInt(valLenIn);
        val.readFields(valIn);

        // Read another compressed 'value'
        --noBufferedValues;

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

      if (!blockCompressed) {
        val = deserializeValue(val);

        if (valIn.read() > 0) {
          LOG.info("available bytes: " + valIn.available());
          throw new IOException(val + " read "
              + (valBuffer.getPosition() - keyLength) + " bytes, should read "
              + (valBuffer.getLength() - keyLength));
        }
      } else {
        // Get the value
        int valLength = WritableUtils.readVInt(valLenIn);
        val = deserializeValue(val);

        // Read another compressed 'value'
        --noBufferedValues;

        // Sanity check
        if (valLength < 0) {
          LOG.debug(val + " is a zero-length value");
        }
      }
      return val;

    }

    @SuppressWarnings("unchecked")
    private Object deserializeValue(Object val) throws IOException {
      return valDeserializer.deserialize(val);
    }

    /**
     * Read the next key in the file into <code>key</code>, skipping its value.
     * True if another entry exists, and false at end of file.
     */
    public synchronized boolean next(Writable key) throws IOException {
      if (key.getClass() != getKeyClass())
        throw new IOException("wrong key class: " + key.getClass().getName()
            + " is not " + keyClass);

      if (!blockCompressed) {
        outBuf.reset();

        keyLength = next(outBuf);
        if (keyLength < 0)
          return false;

        valBuffer.reset(outBuf.getData(), outBuf.getLength());

        key.readFields(valBuffer);
        valBuffer.mark(0);
        if (valBuffer.getPosition() != keyLength)
          throw new IOException(key + " read " + valBuffer.getPosition()
              + " bytes, should read " + keyLength);
      } else {
        // Reset syncSeen
        syncSeen = false;

        if (noBufferedKeys == 0) {
          try {
            readBlock();
          } catch (EOFException eof) {
            return false;
          }
        }

        int keyLength = WritableUtils.readVInt(keyLenIn);

        // Sanity check
        if (keyLength < 0) {
          return false;
        }

        // Read another compressed 'key'
        key.readFields(keyIn);
        --noBufferedKeys;
      }

      return true;
    }

    /**
     * Read the next key/value pair in the file into <code>key</code> and
     * <code>val</code>. Returns true if such a pair exists and false when at
     * end of file
     */
    public synchronized boolean next(Writable key, Writable val)
        throws IOException {
      if (val.getClass() != getValueClass())
        throw new IOException("wrong value class: " + val + " is not "
            + valClass);

      boolean more = next(key);

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
      if (in.getPos() >= end) {
        return -1;
      }
      int length = in.readInt();
      if (version > 1 && sync != null && length == SYNC_ESCAPE) { // process a
                                                                  // sync entry
        in.readFully(syncCheck); // read syncCheck
        if (!Arrays.equals(sync, syncCheck)) // check it
          throw new IOException("File is corrupt!");
        syncSeen = true;
        if (in.getPos() >= end) {
          return -1;
        }
        length = in.readInt(); // re-read length
      } else {
        syncSeen = false;
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
    public synchronized int next(DataOutputBuffer buffer) throws IOException {
      // Unsupported for block-compressed sequence files
      if (blockCompressed) {
        throw new IOException(
            "Unsupported call for block-compressed"
                + " SequenceFiles - use SequenceFile.Reader.next(DataOutputStream, ValueBytes)");
      }
      try {
        int length = readRecordLength();
        if (length == -1) {
          return -1;
        }
        int keyLength = in.readInt();
        buffer.write(in, length);
        return keyLength;
      } catch (ChecksumException e) { // checksum failure
        handleChecksumException(e);
        return next(buffer);
      }
    }

    public ValueBytes createValueBytes() {
      ValueBytes val = null;
      if (!decompress || blockCompressed) {
        val = new UncompressedBytes();
      } else {
        val = new CompressedBytes(codec);
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
    public synchronized int nextRaw(DataOutputBuffer key, ValueBytes val)
        throws IOException {
      if (!blockCompressed) {
        int length = readRecordLength();
        if (length == -1) {
          return -1;
        }
        int keyLength = in.readInt();
        int valLength = length - keyLength;
        key.write(in, keyLength);
        if (decompress) {
          CompressedBytes value = (CompressedBytes) val;
          value.reset(in, valLength);
        } else {
          UncompressedBytes value = (UncompressedBytes) val;
          value.reset(in, valLength);
        }

        return length;
      } else {
        // Reset syncSeen
        syncSeen = false;

        // Read 'key'
        if (noBufferedKeys == 0) {
          if (in.getPos() >= end)
            return -1;

          try {
            readBlock();
          } catch (EOFException eof) {
            return -1;
          }
        }
        int keyLength = WritableUtils.readVInt(keyLenIn);
        if (keyLength < 0) {
          throw new IOException("zero length key found!");
        }
        key.write(keyIn, keyLength);
        --noBufferedKeys;

        // Read raw 'value'
        seekToCurrentValue();
        int valLength = WritableUtils.readVInt(valLenIn);
        UncompressedBytes rawValue = (UncompressedBytes) val;
        rawValue.reset(valIn, valLength);
        --noBufferedValues;

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
    public int nextRawKey(DataOutputBuffer key) throws IOException {
      if (!blockCompressed) {
        recordLength = readRecordLength();
        if (recordLength == -1) {
          return -1;
        }
        keyLength = in.readInt();
        key.write(in, keyLength);
        return keyLength;
      } else {
        // Reset syncSeen
        syncSeen = false;

        // Read 'key'
        if (noBufferedKeys == 0) {
          if (in.getPos() >= end)
            return -1;

          try {
            readBlock();
          } catch (EOFException eof) {
            return -1;
          }
        }
        int keyLength = WritableUtils.readVInt(keyLenIn);
        if (keyLength < 0) {
          throw new IOException("zero length key found!");
        }
        key.write(keyIn, keyLength);
        --noBufferedKeys;

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
            + " is not " + keyClass);
      }

      if (!blockCompressed) {
        outBuf.reset();

        keyLength = next(outBuf);
        if (keyLength < 0)
          return null;

        valBuffer.reset(outBuf.getData(), outBuf.getLength());

        key = deserializeKey(key);
        valBuffer.mark(0);
        if (valBuffer.getPosition() != keyLength)
          throw new IOException(key + " read " + valBuffer.getPosition()
              + " bytes, should read " + keyLength);
      } else {
        // Reset syncSeen
        syncSeen = false;

        if (noBufferedKeys == 0) {
          try {
            readBlock();
          } catch (EOFException eof) {
            return null;
          }
        }

        int keyLength = WritableUtils.readVInt(keyLenIn);

        // Sanity check
        if (keyLength < 0) {
          return null;
        }

        // Read another compressed 'key'
        key = deserializeKey(key);
        --noBufferedKeys;
      }

      return key;
    }

    @SuppressWarnings("unchecked")
    private Object deserializeKey(Object key) throws IOException {
      return keyDeserializer.deserialize(key);
    }

    /**
     * Read 'raw' values.
     *
     * @param val - The 'raw' value
     * @return Returns the value length
     * @throws IOException
     */
    public synchronized int nextRawValue(ValueBytes val) throws IOException {

      // Position stream to current value
      seekToCurrentValue();

      if (!blockCompressed) {
        int valLength = recordLength - keyLength;
        if (decompress) {
          CompressedBytes value = (CompressedBytes) val;
          value.reset(in, valLength);
        } else {
          UncompressedBytes value = (UncompressedBytes) val;
          value.reset(in, valLength);
        }

        return valLength;
      } else {
        int valLength = WritableUtils.readVInt(valLenIn);
        UncompressedBytes rawValue = (UncompressedBytes) val;
        rawValue.reset(valIn, valLength);
        --noBufferedValues;
        return valLength;
      }

    }

    private void handleChecksumException(ChecksumException e)
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
    public synchronized void seek(long position) throws IOException {
      in.seek(position);
      if (blockCompressed) { // trigger block read
        noBufferedKeys = 0;
        valuesDecompressed = true;
      }
    }

    /** Seek to the next sync mark past a given position. */
    public synchronized void sync(long position) throws IOException {
      if (position + SYNC_SIZE >= end) {
        seek(end);
        return;
      }

      try {
        seek(position + 4); // skip escape
        in.readFully(syncCheck);
        int syncLen = sync.length;
        for (int i = 0; in.getPos() < end; i++) {
          int j = 0;
          for (; j < syncLen; j++) {
            if (sync[j] != syncCheck[(i + j) % syncLen])
              break;
          }
          if (j == syncLen) {
            in.seek(in.getPos() - SYNC_SIZE); // position before sync
            return;
          }
          syncCheck[i % syncLen] = in.readByte();
        }
      } catch (ChecksumException e) { // checksum failure
        handleChecksumException(e);
      }
    }

    /** Returns true iff the previous call to next passed a sync mark. */
    public boolean syncSeen() {
      return syncSeen;
    }

    /** Return the current byte position in the input file. */
    public synchronized long getPosition() throws IOException {
      return in.getPos();
    }

    /** Returns the name of the file. */
    public String toString() {
      return file.toString();
    }

  }

} // SequenceFile
