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

import java.util.EnumSet;
import java.util.Set;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.log4j.Logger;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;

/**
 * This class implements a viewer for Parquet files.
 *
 * @author David Z. Chen (dchen@linkedin.com)
 */
public class ParquetFileViewer extends HdfsFileViewer {
  private static Logger logger = Logger.getLogger(ParquetFileViewer.class);

  // Will spend 5 seconds trying to pull data and then stop.
  private final static long STOP_TIME = 2000l;

  private static final String VIEWER_NAME = "Parquet";

  @Override
  public String getName() {
    return VIEWER_NAME;
  }

  @Override
  public Set<Capability> getCapabilities(FileSystem fs, Path path)
      throws AccessControlException {
    if (logger.isDebugEnabled()) {
      logger.debug("Parquet file path: " + path.toUri().getPath());
    }

    AvroParquetReader<GenericRecord> parquetReader = null;
    try {
      parquetReader = new AvroParquetReader<GenericRecord>(path);
    } catch (IOException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(path.toUri().getPath() + " is not a Parquet file.");
        logger.debug("Error in opening Parquet file: "
            + e.getLocalizedMessage());
      }
      return EnumSet.noneOf(Capability.class);
    } finally {
      try {
        if (parquetReader != null) {
          parquetReader.close();
        }
      } catch (IOException e) {
        logger.error(e);
      }
    }
    return EnumSet.of(Capability.READ, Capability.SCHEMA);
  }

  @Override
  public void displayFile(FileSystem fs, Path path, OutputStream outputStream,
      int startLine, int endLine) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("Display Parquet file: " + path.toUri().getPath());
    }

    AvroParquetReader<GenericRecord> parquetReader = null;
    try {
      parquetReader = new AvroParquetReader<GenericRecord>(path);

      // Declare the avroWriter encoder that will be used to output the records
      // as JSON but don't construct them yet because we need the first record
      // in order to get the Schema.
      DatumWriter<GenericRecord> avroWriter = null;
      Encoder encoder = null;

      long endTime = System.currentTimeMillis() + STOP_TIME;
      int line = 1;
      while (line <= endLine && System.currentTimeMillis() <= endTime) {
        GenericRecord record = parquetReader.read();
        if (record == null) {
          break;
        }

        if (avroWriter == null) {
          Schema schema = record.getSchema();
          avroWriter = new GenericDatumWriter<GenericRecord>(schema);
          encoder = EncoderFactory.get().jsonEncoder(schema, outputStream, true);
        }

        if (line >= startLine) {
          String recordStr = "\n\nRecord " + line + ":\n";
          outputStream.write(recordStr.getBytes("UTF-8"));
          avroWriter.write(record, encoder);
          encoder.flush();
        }
        ++line;
      }
    } catch (IOException e) {
      outputStream.write(("Error in displaying Parquet file: " + e
          .getLocalizedMessage()).getBytes("UTF-8"));
      throw e;
    } catch (Throwable t) {
      logger.error(t.getMessage());
      return;
    } finally {
      parquetReader.close();
    }
  }

  @Override
  public String getSchema(FileSystem fs, Path path) {
    String schema = null;
    try {
      AvroParquetReader<GenericRecord> parquetReader =
          new AvroParquetReader<GenericRecord>(path);
      GenericRecord record = parquetReader.read();
      if (record == null) {
        return null;
      }
      Schema avroSchema = record.getSchema();
      AvroSchemaConverter converter = new AvroSchemaConverter();
      schema = converter.convert(avroSchema).toString();
    } catch (IOException e) {
      logger.warn("Cannot get schema for file: " + path.toUri().getPath());
      return null;
    }

    return schema;
  }
}
