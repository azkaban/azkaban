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

import com.mongodb.util.JSON;
import java.util.EnumSet;
import java.util.Set;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;


/**
 * File viewer for Mongo bson files.
 *
 * @author adilaijaz
 */
public final class BsonFileViewer extends HdfsFileViewer {

  /**
   * The maximum time spent, in milliseconds, while reading records from the
   * file.
   */
  private static long STOP_TIME = 2000l;

  private static final String VIEWER_NAME = "BSON";

  @Override
  public String getName() {
    return VIEWER_NAME;
  }

  @Override
  public Set<Capability> getCapabilities(FileSystem fs, Path path) {
    if (path.getName().endsWith(".bson")) {
      return EnumSet.of(Capability.READ);
    }
    return EnumSet.noneOf(Capability.class);
  }

  @Override
  public void displayFile(FileSystem fs, Path path, OutputStream outStream,
      int startLine, int endLine) throws IOException {

    FSDataInputStream in = null;
    try {
      in = fs.open(path, 16 * 1024 * 1024);

      long endTime = System.currentTimeMillis() + STOP_TIME;

      BasicBSONCallback callback = new BasicBSONCallback();
      BasicBSONDecoder decoder = new BasicBSONDecoder();

      /*
       * keep reading and rendering bsonObjects until one of these conditions is
       * met:
       *
       * a. we have rendered all bsonObjects desired. b. we have run out of
       * time.
       */
      for (int lineno = 1; lineno <= endLine
          && System.currentTimeMillis() <= endTime; lineno++) {
        if (lineno < startLine) {
          continue;
        }

        callback.reset();
        decoder.decode(in, callback);

        BSONObject value = (BSONObject) callback.get();

        StringBuilder bldr = new StringBuilder();
        bldr.append("\n\n Record ");
        bldr.append(lineno);
        bldr.append('\n');
        JSON.serialize(value, bldr);
        outStream.write(bldr.toString().getBytes("UTF-8"));
      }
    } catch (IOException e) {
      outStream
          .write(("Error in display avro file: " + e.getLocalizedMessage())
              .getBytes("UTF-8"));
    } finally {
      if (in != null) {
        in.close();
      }
      outStream.flush();
    }
  }
}
