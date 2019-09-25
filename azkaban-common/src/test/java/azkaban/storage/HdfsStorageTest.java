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
import azkaban.spi.FileIOStatus;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.spi.DependencyFile;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.HashUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
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
  private FileSystem hdfs;
  private DistributedFileSystem dfs;

  private Dependency DEP_A;
  private Path EXPECTED_PATH_DEP_A;

  private static final String PRJ_ROOT_URI = "hdfs://localhost:9000/path/to/prj";
  private static final String DEP_ROOT_URI = "hdfs://localhost:9000/path/to/dep";

  @Before
  public void setUp() throws Exception {
    this.hdfs = mock(FileSystem.class);
    this.dfs = mock(DistributedFileSystem.class);
    this.hdfsAuth = mock(HdfsAuth.class);
    final AzkabanCommonModuleConfig config = mock(AzkabanCommonModuleConfig.class);
    when(config.getHdfsProjectRootUri()).thenReturn(URI.create(PRJ_ROOT_URI));
    when(config.getHdfsDependencyRootUri()).thenReturn(URI.create(DEP_ROOT_URI));

    this.hdfsStorage = new HdfsStorage(this.hdfsAuth, this.hdfs, this.dfs, config);

    DEP_A = ThinArchiveTestUtils.getDepA();
    EXPECTED_PATH_DEP_A = new Path(DEP_ROOT_URI + "/"
        + ThinArchiveTestUtils.getDepAPath());
  }

  @Test
  public void testSetUpWithoutDEP_ROOT_URI() throws Exception {
    // Ensure we can still instantiate HdfsStorage without a specified DEP_ROOT_URI.
    // If this is done in production, thin archive uploads will not be supported (they will fail).

    final AzkabanCommonModuleConfig config = mock(AzkabanCommonModuleConfig.class);
    when(config.getHdfsProjectRootUri()).thenReturn(URI.create(PRJ_ROOT_URI));
    when(config.getHdfsDependencyRootUri()).thenReturn(null);

    this.hdfsStorage = new HdfsStorage(this.hdfsAuth, this.hdfs, this.dfs, config);
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

    final ProjectStorageMetadata metadata = new ProjectStorageMetadata(1, 2, "uploader", HashUtils.MD5.getHashBytes(file));
    final String key = this.hdfsStorage.putProject(metadata, file);

    final String expectedName = String.format("1/1-%s.zip", hash);
    Assert.assertEquals(expectedName, key);

    final String expectedPath = "/path/to/prj/" + expectedName;
    verify(this.hdfs).copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(expectedPath));
  }

  @Test
  public void testGetDependency() throws Exception {
    this.hdfsStorage.getDependency(DEP_A);
    verify(this.hdfs).open(EXPECTED_PATH_DEP_A);
  }

  @Test
  public void testDependencyStatus_NON_EXISTANT() throws Exception {
    when(this.dfs.isFileClosed(EXPECTED_PATH_DEP_A)).thenThrow(new FileNotFoundException());
    assertEquals(FileIOStatus.NON_EXISTANT, this.hdfsStorage.dependencyStatus(DEP_A));
  }

  @Test
  public void testDependencyStatus_OPEN() throws Exception {
    when(this.dfs.isFileClosed(EXPECTED_PATH_DEP_A)).thenReturn(false);
    assertEquals(FileIOStatus.OPEN, this.hdfsStorage.dependencyStatus(DEP_A));
  }

  @Test
  public void testDependencyStatus_CLOSED() throws Exception {
    when(this.dfs.isFileClosed(EXPECTED_PATH_DEP_A)).thenReturn(true);
    assertEquals(FileIOStatus.CLOSED, this.hdfsStorage.dependencyStatus(DEP_A));
  }

  @Test
  public void testPutDependencyNotInStorage() throws Exception {
    final File tmpEmptyJar = TEMP_DIR.newFile(DEP_A.getFileName());

    // Indicate that the file does not exist
    when(this.dfs.isFileClosed(EXPECTED_PATH_DEP_A)).thenThrow(new FileNotFoundException());

    DependencyFile depFile = DEP_A.makeDependencyFile(tmpEmptyJar);
    assertEquals(FileIOStatus.CLOSED, this.hdfsStorage.putDependency(depFile));

    verify(this.hdfs).mkdirs(EXPECTED_PATH_DEP_A.getParent());
    verify(this.hdfs).copyFromLocalFile(new Path(tmpEmptyJar.getAbsolutePath()), EXPECTED_PATH_DEP_A);
  }

  @Test
  public void testPutDependencyAlreadyExists() throws Exception {
    final File tmpEmptyJar = TEMP_DIR.newFile(DEP_A.getFileName());

    // Indicate that the file exists and is finalized (closed and not being written to)
    when(this.dfs.isFileClosed(EXPECTED_PATH_DEP_A)).thenReturn(true);

    DependencyFile depFile = DEP_A.makeDependencyFile(tmpEmptyJar);
    assertEquals(FileIOStatus.CLOSED, this.hdfsStorage.putDependency(depFile));

    // Because the dependency already exists, NO attempt should be made to persist it OR create its parent directories.
    verify(this.hdfs, never()).mkdirs(EXPECTED_PATH_DEP_A.getParent());
    verify(this.hdfs, never()).copyFromLocalFile(new Path(tmpEmptyJar.getAbsolutePath()), EXPECTED_PATH_DEP_A);
  }

  @Test
  public void testPutDependencyCurrentlyOpen() throws Exception {
    final File tmpEmptyJar = TEMP_DIR.newFile(DEP_A.getFileName());

    // Indicate that the file exists but is being currently written to
    when(this.dfs.isFileClosed(EXPECTED_PATH_DEP_A)).thenReturn(false);

    DependencyFile depFile = DEP_A.makeDependencyFile(tmpEmptyJar);
    assertEquals(FileIOStatus.OPEN, this.hdfsStorage.putDependency(depFile));

    // Because the dependency already exists, NO attempt should be made to persist it OR create its parent directories.
    verify(this.hdfs, never()).mkdirs(EXPECTED_PATH_DEP_A.getParent());
    verify(this.hdfs, never()).copyFromLocalFile(new Path(tmpEmptyJar.getAbsolutePath()), EXPECTED_PATH_DEP_A);
  }

  @Test
  public void testPutDependencyRaceCondition() throws Exception {
    final File tmpEmptyJar = TEMP_DIR.newFile(DEP_A.getFileName());

    // Indicate that the file does not exist
    when(this.dfs.isFileClosed(EXPECTED_PATH_DEP_A)).thenThrow(new FileNotFoundException());

    // But uh oh....plot twist! When the method attempts to write the file, we throw an exception
    // indicating it DOES actually exist. This could happen if another process began writing to the file
    // AFTER we checked isFileClosed.
    doThrow(new org.apache.hadoop.fs.FileAlreadyExistsException("Uh oh :("))
        .when(this.hdfs).copyFromLocalFile(new Path(tmpEmptyJar.getAbsolutePath()), EXPECTED_PATH_DEP_A);

    DependencyFile depFile = DEP_A.makeDependencyFile(tmpEmptyJar);

    // We expect putDependency to return a FileStatus of OPEN because, while it's POSSIBLE that the
    // other process completed writing to the file and the file status is actually CLOSED, we'll play it safe
    // and assume it is OPEN to ensure consuming methods know not to rely on the file being persisted to HDFS.
    assertEquals(FileIOStatus.OPEN, this.hdfsStorage.putDependency(depFile));

    // We expect an attempt to still be made to create the parent directories for the dependency.
    verify(this.hdfs).mkdirs(EXPECTED_PATH_DEP_A.getParent());
  }

  @Test
  public void testDeleteProject() throws Exception {
    this.hdfsStorage.deleteProject("1/1-hash.zip");
    verify(this.hdfs).delete(new Path(PRJ_ROOT_URI + "/1/1-hash.zip"), false);
  }
}
