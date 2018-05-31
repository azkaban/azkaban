///*
// * Copyright 2012 LinkedIn Corp.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//
//package azkaban.viewer.hdfs;
//
//import java.util.EnumSet;
//import java.util.Set;
//import java.io.IOException;
//import java.io.PrintWriter;
//
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.fs.permission.AccessControlException;
//import org.apache.log4j.Logger;
//
//import azkaban.viewer.hdfs.AzkabanSequenceFileReader;
//
//import voldemort.serialization.json.JsonTypeSerializer;
//
//public class JsonSequenceFileViewer extends SequenceFileViewer {
//  private static final String VIEWER_NAME = "JSON Sequence File";
//
//  @Override
//  public String getName() {
//    return VIEWER_NAME;
//  }
//
//  private static Logger logger = Logger.getLogger(JsonSequenceFileViewer.class);
//
//  public Set<Capability> getCapabilities(AzkabanSequenceFileReader.Reader reader) {
//    Text keySchema = reader.getMetadata().get(new Text("key.schema"));
//    Text valueSchema = reader.getMetadata().get(new Text("value.schema"));
//    if (keySchema != null && valueSchema != null) {
//      return EnumSet.of(Capability.READ);
//    }
//    return EnumSet.noneOf(Capability.class);
//  }
//
//  public void displaySequenceFile(AzkabanSequenceFileReader.Reader reader,
//      PrintWriter output, int startLine, int endLine) throws IOException {
//
//    if (logger.isDebugEnabled()) {
//      logger.debug("display json file");
//    }
//
//    BytesWritable keyWritable = new BytesWritable();
//    BytesWritable valueWritable = new BytesWritable();
//    Text keySchema = reader.getMetadata().get(new Text("key.schema"));
//    Text valueSchema = reader.getMetadata().get(new Text("value.schema"));
//
//    JsonTypeSerializer keySerializer =
//        new JsonTypeSerializer(keySchema.toString());
//    JsonTypeSerializer valueSerializer =
//        new JsonTypeSerializer(valueSchema.toString());
//
//    // skip lines before the start line
//    for (int i = 1; i < startLine; i++) {
//      reader.next(keyWritable, valueWritable);
//    }
//
//    // now actually output lines
//    for (int i = startLine; i <= endLine; i++) {
//      boolean readSomething = reader.next(keyWritable, valueWritable);
//      if (!readSomething) {
//        break;
//      }
//      output.write(
//          safeToString(keySerializer.toObject(keyWritable.getBytes())));
//      output.write("\t=>\t");
//      output.write(safeToString(valueSerializer.toObject(valueWritable
//          .getBytes())));
//      output.write("\n");
//      output.flush();
//    }
//  }
//
//  private String safeToString(Object value) {
//    if (value == null) {
//      return "null";
//    } else {
//      return value.toString();
//    }
//  }
//}
