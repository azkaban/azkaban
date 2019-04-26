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
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements a viewer of avro files
 *
 * @author lguo
 */
public class AvroFileViewer extends HdfsFileViewer {

  private static final Logger LOG = LoggerFactory.getLogger(AvroFileViewer.class);
  // Will spend 5 seconds trying to pull data and then stop.
  private static long STOP_TIME = 2000l;

  private static final String VIEWER_NAME = "Avro";

  @Override
  public String getName() {
    return VIEWER_NAME;
  }

  @Override
  public Set<Capability> getCapabilities(FileSystem fs, Path path)
      throws AccessControlException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("path:" + path.toUri().getPath());
    }

    DataFileStream<Object> avroDataStream = null;
    try {
      avroDataStream = getAvroDataStream(fs, path);
      Schema schema = avroDataStream.getSchema();
      return (schema != null) ? EnumSet.of(Capability.READ, Capability.SCHEMA)
          : EnumSet.noneOf(Capability.class);
    } catch (AccessControlException e) {
      throw e;
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(path.toUri().getPath() + " is not an avro file.");
        LOG.debug("Error in getting avro schema: ", e);
      }
      return EnumSet.noneOf(Capability.class);
    } finally {
      try {
        if (avroDataStream != null) {
          avroDataStream.close();
        }
      } catch (IOException e) {
        LOG.error("Close Avro Data Stream Failure", e);
      }
    }
  }

  @Override
  public String getSchema(FileSystem fs, Path path) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("path:" + path.toUri().getPath());
    }

    DataFileStream<Object> avroDataStream = null;
    try {
      avroDataStream = getAvroDataStream(fs, path);
      Schema schema = avroDataStream.getSchema();
      return schema.toString(true);
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(path.toUri().getPath() + " is not an avro file.");
        LOG.debug("Error in getting avro schema: ", e);
      }
      return null;
    } finally {
      try {
        if (avroDataStream != null) {
          avroDataStream.close();
        }
      } catch (IOException e) {
        LOG.error("Close Avro Data Stream Failure", e);
      }
    }
  }

  private DataFileStream<Object> getAvroDataStream(FileSystem fs, Path path)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("path:" + path.toUri().getPath());
    }

    GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
    InputStream hdfsInputStream = null;
    try {
      hdfsInputStream = fs.open(path);
    } catch (IOException e) {
      if (hdfsInputStream != null) {
        hdfsInputStream.close();
      }
      throw e;
    }

    DataFileStream<Object> avroDataFileStream = null;
    try {
      avroDataFileStream =
          new DataFileStream<Object>(hdfsInputStream, avroReader);
    } catch (IOException e) {
      if (hdfsInputStream != null) {
        hdfsInputStream.close();
      }
      throw e;
    }

    return avroDataFileStream;
  }

  @Override
  public void displayFile(FileSystem fs, Path path, OutputStream outputStream,
      int startLine, int endLine) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("display avro file:" + path.toUri().getPath());
    }

    DataFileStream<Object> avroDatastream = null;
    JsonGenerator g = null;

    try {
      avroDatastream = getAvroDataStream(fs, path);
      Schema schema = avroDatastream.getSchema();
      DatumWriter<Object> avroWriter = new GenericDatumWriter<Object>(schema);

      g = new JsonFactory().createJsonGenerator(
          outputStream, JsonEncoding.UTF8);
      g.useDefaultPrettyPrinter();
      Encoder encoder = EncoderFactory.get().jsonEncoder(schema, g);

      long endTime = System.currentTimeMillis() + STOP_TIME;
      int lineno = 1; // line number starts from 1
      while (avroDatastream.hasNext() && lineno <= endLine
          && System.currentTimeMillis() <= endTime) {
        Object datum = avroDatastream.next();
        if (lineno >= startLine) {
          String record = "\n\n Record " + lineno + ":\n";
          outputStream.write(record.getBytes("UTF-8"));
          avroWriter.write(datum, encoder);
          encoder.flush();
        }
        lineno++;
      }
    } catch (IOException e) {
      outputStream.write(("Error in display avro file: " + e
          .getLocalizedMessage()).getBytes("UTF-8"));
      throw e;
    } finally {
      if (g != null) {
        g.close();
      }
      avroDatastream.close();
    }
  }
}
