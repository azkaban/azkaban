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
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;


public class HdfsStorageTest {
  private static final Logger log = Logger.getLogger(HdfsStorageTest.class);

  @Test @Ignore
  public void test1() throws Exception {
    Path pt=new Path("hdfs://npvm11.np.wc1.yellowpages.com:9000/user/john/abc.txt");
    FileSystem fs = FileSystem.get(new Configuration());
    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    String line;
    line=br.readLine();
    while (line != null){
      System.out.println(line);
      line=br.readLine();
    }
  }
}
