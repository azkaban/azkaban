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

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.StorageMetadata;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class HdfsStorageTest {
  private static final Logger log = Logger.getLogger(HdfsStorageTest.class);

  private HdfsStorage hdfsStorage;
  private FileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    hdfs = mock(FileSystem.class);
    AzkabanCommonModuleConfig config = mock(AzkabanCommonModuleConfig.class);
    when(config.getHdfsRootUri()).thenReturn(URI.create("hdfs://localhost:9000/path/to/foo"));

    hdfsStorage = new HdfsStorage(hdfs, config);
  }

  @Test
  public void testGet() throws Exception {
    URI uri = URI.create("1/1-hash.zip");
    hdfsStorage.get(uri);
    verify(hdfs).open(new Path("hdfs://localhost:9000/path/to/foo/1/1-hash.zip"));
  }

  @Test
  public void testPut() throws Exception {
    File file = mock(File.class);
    when(file.getName()).thenReturn("bar.zip");
    String absolutePath = "/path/to/foo/bar.zip";
    when(file.getAbsolutePath()).thenReturn(absolutePath);

    when(hdfs.exists(any(Path.class))).thenReturn(false);

    StorageMetadata metadata = new StorageMetadata(1, 2, "uploader", "hash".getBytes());
    URI uri = hdfsStorage.put(metadata, file);

    verify(hdfs).copyFromLocalFile(new Path(absolutePath), new Path("/path/to/foo/1/1-hash.zip"));

    Assert.assertEquals(URI.create("1/1-hash.zip"), uri);
  }
}
