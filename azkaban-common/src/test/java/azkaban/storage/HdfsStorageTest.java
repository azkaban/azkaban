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
import azkaban.spi.Dependency;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.HashUtils;
import java.io.File;
import java.net.URI;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class HdfsStorageTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private HdfsAuth hdfsAuth;
  private HdfsStorage hdfsStorage;
  private DistributedFileSystem hdfs;
  private FileSystem http;
  private AzkabanCommonModuleConfig config;

  private Dependency depA;
  private Path expectedPathDepA;

  private static final String PRJ_ROOT_URI = "hdfs://localhost:9000/path/to/prj/";
  private static final String DEP_ROOT_URI = "chttp://www.someplace.com/path/to/dep/";

  @Before
  public void setUp() throws Exception {
    this.hdfs = mock(DistributedFileSystem.class);
    this.http = mock(FileSystem.class);
    this.hdfsAuth = mock(HdfsAuth.class);
    this.config = mock(AzkabanCommonModuleConfig.class);
    when(config.getHdfsProjectRootUri()).thenReturn(URI.create(PRJ_ROOT_URI));
    when(config.getOriginDependencyRootUri()).thenReturn(URI.create(DEP_ROOT_URI));

    this.hdfsStorage = new HdfsStorage(config, this.hdfsAuth, this.hdfs, this.http);

    depA = ThinArchiveTestUtils.getDepA();
    expectedPathDepA = new Path(DEP_ROOT_URI + ThinArchiveTestUtils.getDepAPath());
  }

  @Test
  public void testSetUpWithoutHttpFileSystem() throws Exception {
    // Ensure we can still instantiate HdfsStorage with the HTTP FileSystem being null.
    // Fetching dependencies will not be supported.
    this.hdfsStorage = new HdfsStorage(this.config, this.hdfsAuth, this.hdfs, null);
    assertFalse(this.hdfsStorage.dependencyFetchingEnabled());

    boolean hitException = false;
    try {
      this.hdfsStorage.getDependency(depA);
    } catch (UnsupportedOperationException e) {
      hitException = true;
    }
    if (!hitException) fail("Expected UnsupportedOperationException when fetching dependency.");
  }

  @Test
  public void testGetDependencyRootPath() {
    assertEquals(DEP_ROOT_URI, this.hdfsStorage.getDependencyRootPath());
  }

  @Test
  public void testGetProject() throws Exception {
    this.hdfsStorage.getProject("1/1-hash.zip");
    verify(this.hdfs).open(new Path(PRJ_ROOT_URI + "/1/1-hash.zip"));
  }

  @Test
  public void testPutProject() throws Exception {
    final File file = new File(
        getClass().getClassLoader().getResource("sample_flow_01.zip").getFile());
    final String hash = new String(Hex.encodeHex(HashUtils.MD5.getHashBytes(file)));

    when(this.hdfs.exists(any(Path.class))).thenReturn(false);

    final ProjectStorageMetadata metadata = new ProjectStorageMetadata(1, 2,
        "uploader", HashUtils.MD5.getHashBytes(file), "111.111.111.111");
    final String key = this.hdfsStorage.putProject(metadata, file);

    final String expectedName = String.format("1/1-%s.zip", hash);
    Assert.assertEquals(expectedName, key);

    final String expectedPath = "/path/to/prj/" + expectedName;
    verify(this.hdfs).copyFromLocalFile(eq(false), eq(true), any(Path.class), any(Path.class));
    verify(this.hdfs).rename(any(Path.class), eq(new Path(expectedPath)), eq(Options.Rename.OVERWRITE));
  }

  @Test
  public void testGetDependency() throws Exception {
    this.hdfsStorage.getDependency(depA);
    verify(this.http).open(expectedPathDepA);
  }

  @Test
  public void testDependencyFetchingEnabled() {
    assertTrue(this.hdfsStorage.dependencyFetchingEnabled());
  }

  @Test
  public void testDeleteProject() throws Exception {
    this.hdfsStorage.deleteProject("1/1-hash.zip");
    verify(this.hdfs).delete(new Path(PRJ_ROOT_URI + "/1/1-hash.zip"), false);
  }
}
