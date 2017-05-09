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
import azkaban.utils.Md5Hasher;
import java.io.File;
import java.net.URI;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class HdfsStorageTest {
  private HdfsAuth hdfsAuth;
  private HdfsStorage hdfsStorage;
  private FileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    hdfs = mock(FileSystem.class);
    hdfsAuth = mock(HdfsAuth.class);
    AzkabanCommonModuleConfig config = mock(AzkabanCommonModuleConfig.class);
    when(config.getHdfsRootUri()).thenReturn(URI.create("hdfs://localhost:9000/path/to/foo"));

    hdfsStorage = new HdfsStorage(hdfsAuth, hdfs, config);
  }

  @Test
  public void testGet() throws Exception {
    hdfsStorage.get("1/1-hash.zip");
    verify(hdfs).open(new Path("hdfs://localhost:9000/path/to/foo/1/1-hash.zip"));
  }

  @Test
  public void testPut() throws Exception {
    File file = new File(getClass().getClassLoader().getResource("sample_flow_01.zip").getFile());
    final String hash = new String(Hex.encodeHex(Md5Hasher.md5Hash(file)));

    when(hdfs.exists(any(Path.class))).thenReturn(false);

    StorageMetadata metadata = new StorageMetadata(1, 2, "uploader", Md5Hasher.md5Hash(file));
    String key = hdfsStorage.put(metadata, file);

    final String expectedName = String.format("1/1-%s.zip", hash);
    Assert.assertEquals(expectedName, key);

    final String expectedPath = "/path/to/foo/" + expectedName;
    verify(hdfs).copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(expectedPath));
  }
}
