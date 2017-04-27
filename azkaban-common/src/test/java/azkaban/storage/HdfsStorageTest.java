/*
 * Copyright 2017 LinkedIn Corp.
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
 *
 */

package azkaban.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;


public class HdfsStorageTest {
  private static final Logger log = Logger.getLogger(HdfsStorageTest.class);

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration(false);
    conf.addResource(new Path("file:///Users/spyne/hadoop/hadoop-2.6.1/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("file:///Users/spyne/hadoop/hadoop-2.6.1/etc/hadoop/hdfs-site.xml"));
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

    System.out.println(conf);

    Path pt=new Path("hdfs://localhost:9000/test.file");
    FileSystem fs = FileSystem.get(conf);
    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    String line;
    line=br.readLine();
    while (line != null){
      System.out.println(line);
      line=br.readLine();
    }
  }
}
