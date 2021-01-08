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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Dependency;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.HashUtils;
import java.io.File;
import java.net.URI;
import java.util.EnumSet;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class HdfsStorageTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private AbstractHdfsAuth hdfsAuth;
  private HdfsStorage hdfsStorage;
  private FileContext hdfsFileContext;
  private FileContext.Util hdfsFileContextUtil;

  private FileSystem http;
  private AzkabanCommonModuleConfig config;

  private Dependency depA;
  private Path expectedPathDepA;

  private static final String PRJ_ROOT_URI = "hdfs://localhost:9000/path/to/prj/";
  private static final String DEP_ROOT_URI = "chttp://www.someplace.com/path/to/dep/";
  private static final String IPv4 = "111.111.111.111";

  @Before
  public void setUp() throws Exception {
    this.hdfsFileContext = mock(FileContext.class);
    this.hdfsFileContextUtil = mock(FileContext.Util.class);
    this.http = mock(FileSystem.class);
    this.hdfsAuth = mock(HdfsAuth.class);
    this.config = mock(AzkabanCommonModuleConfig.class);
    when(hdfsFileContext.util()).thenReturn(this.hdfsFileContextUtil);
    when(config.getHdfsProjectRootUri()).thenReturn(URI.create(PRJ_ROOT_URI));
    when(config.getOriginDependencyRootUri()).thenReturn(URI.create(DEP_ROOT_URI));

    this.hdfsStorage = new HdfsStorage(config, this.hdfsAuth, this.hdfsFileContext, this.http);

    depA = ThinArchiveTestUtils.getDepA();
    expectedPathDepA = new Path(DEP_ROOT_URI + ThinArchiveTestUtils.getDepAPath());
  }

  @Test
  public void testSetUpWithoutHttpFileSystem() throws Exception {
    // Ensure we can still instantiate HdfsStorage with the HTTP FileSystem being null.
    // Fetching dependencies will not be supported.
    this.hdfsStorage = new HdfsStorage(this.config, this.hdfsAuth, this.hdfsFileContext, null);
    assertFalse(this.hdfsStorage.dependencyFetchingEnabled());

    boolean hitException = false;
    try {
      this.hdfsStorage.getDependency(depA);
    } catch (UnsupportedOperationException e) {
      hitException = true;
    }
    if (!hitException) fail("Expected UnsupportedOperationException when fetching dependency.");
  }@Test
  public void testGetDependencyRootPath() {
    assertEquals(DEP_ROOT_URI, this.hdfsStorage.getDependencyRootPath());
  }

  @Test
  public void testGetProject() throws Exception {
    this.hdfsStorage.getProject("1/1-hash.zip");
    verify(this.hdfsFileContext).open(new Path(PRJ_ROOT_URI + "/1/1-hash.zip"));
  }

  @Test
  public void testUploadFile() throws Exception{
    final File sourceFile = new File(
        getClass().getClassLoader().getResource("sample_flow_01.zip").getFile());
    final String sourceHash = new String(Hex.encodeHex(HashUtils.MD5.getHashBytes(sourceFile)));
    final File tmpUploadDir = TEMP_DIR.newFolder("upload_test_dir");
    final File destFile = new File(tmpUploadDir, "tmpfile1");
    final FileContext destFileContext = FileContext.getLocalFSFileContext();

    // A new destination file should be created after this, with same content as the source file.
    HdfsStorage.uploadLocalFile(new Path(sourceFile.getPath()), new Path(destFile.getPath()),
        destFileContext);
    final String destHash = new String(Hex.encodeHex(HashUtils.MD5.getHashBytes(destFile)));
    Assert.assertEquals(sourceHash, destHash);

    // Verify that copying over an existing destination file works as well.
    final byte[] newFileContent = "12345".getBytes(UTF_8);
    final File newSourceFile = new File(tmpUploadDir, "tmpsource1");
    FileUtils.writeByteArrayToFile(newSourceFile, newFileContent, false);

    // The destination file already exists from previous invocation of uploadLocalFile.
    HdfsStorage.uploadLocalFile(new Path(newSourceFile.getPath()), new Path(destFile.getPath()),
        destFileContext);
    final byte[] destFileContent = FileUtils.readFileToByteArray(destFile);
    FileUtils.deleteDirectory(tmpUploadDir);

    Assert.assertArrayEquals(newFileContent, destFileContent);
  }

  @Test
  public void testPutProject() throws Exception {
    final File file = new File(
        getClass().getClassLoader().getResource("sample_flow_01.zip").getFile());
    final String hash = new String(Hex.encodeHex(HashUtils.MD5.getHashBytes(file)));

    // This requires far too much information about the internals for FileContext.create()
    // to make the test succeed. That unfortunately seems to be the price of using a mocking
    // framework. It's worth considering a custom implementation of FileContext for this.
    FSDataOutputStream nullOutputStream = new FSDataOutputStream(new NullOutputStream());
    when(this.hdfsFileContext.create(any(Path.class), any(EnumSet.class)))
        .thenReturn(nullOutputStream);

    final ProjectStorageMetadata metadata = new ProjectStorageMetadata(1, 2,
        "uploader", HashUtils.MD5.getHashBytes(file), IPv4);
    final String key = this.hdfsStorage.putProject(metadata, file);

    final String expectedName = String.format("1/1-%s.zip", hash);
    Assert.assertEquals(expectedName, key);

    final String expectedPath = "/path/to/prj/" + expectedName;

    verify(this.hdfsFileContext).create(any(Path.class), eq(EnumSet.of(CreateFlag.CREATE,
        CreateFlag.OVERWRITE)));
    verify(this.hdfsFileContext)
        .rename(any(Path.class), eq(new Path(expectedPath)), eq(Options.Rename.OVERWRITE));
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
    verify(this.hdfsFileContext).delete(new Path(PRJ_ROOT_URI + "/1/1-hash.zip"), false);
  }
}
