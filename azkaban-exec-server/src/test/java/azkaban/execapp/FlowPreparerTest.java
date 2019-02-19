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

package azkaban.execapp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.execapp.FlowPreparer.ProjectCacheMetrics;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;
import azkaban.project.ProjectFileHandler;
import azkaban.storage.StorageManager;
import azkaban.utils.FileIOUtils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class FlowPreparerTest {

  public static final String SAMPLE_FLOW_01 = "sample_flow_01";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File executionsDir;
  private File projectsDir;
  private FlowPreparer instance;

  private StorageManager createMockStorageManager() {
    final ClassLoader classLoader = getClass().getClassLoader();
    final File file = new File(classLoader.getResource(SAMPLE_FLOW_01 + ".zip").getFile());

    final ProjectFileHandler projectFileHandler = mock(ProjectFileHandler.class);
    when(projectFileHandler.getFileType()).thenReturn("zip");
    when(projectFileHandler.getLocalFile()).thenReturn(file);

    final StorageManager storageManager = mock(StorageManager.class);
    when(storageManager.getProjectFile(anyInt(), anyInt())).thenReturn(projectFileHandler);
    return storageManager;
  }

  @Before
  public void setUp() throws Exception {
    this.executionsDir = this.temporaryFolder.newFolder("executions");
    this.projectsDir = this.temporaryFolder.newFolder("projects");

    this.instance = spy(
        new FlowPreparer(createMockStorageManager(), this.executionsDir, this.projectsDir, null));
    doNothing().when(this.instance).updateLastModifiedTime(any());
  }

  @Test
  public void testProjectDirSizeIsSet() throws Exception {
    final ProjectDirectoryMetadata proj = new ProjectDirectoryMetadata(12, 34,
        new File(this.projectsDir, "sample_project_01"));

    final File tmp = this.temporaryFolder.newFolder("tmp");
    this.instance.downloadProjectIfNotExists(proj, tmp);

    final long actualDirSize = 1048835;

    assertThat(proj.getDirSizeInBytes()).isEqualTo(actualDirSize);
    assertThat(FileIOUtils.readNumberFromFile(
        Paths.get(tmp.getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME)))
        .isEqualTo(actualDirSize);
  }

  @Test
  public void testDownloadingProjectIfNotExists() throws Exception {
    final ProjectDirectoryMetadata proj = new ProjectDirectoryMetadata(12, 34,
        new File(this.projectsDir, "sample_project_01"));
    final File tmp = this.temporaryFolder.newFolder("tmp");

    final boolean isDownloaded = this.instance.downloadProjectIfNotExists(proj, tmp);

    final Path projectDirSizeFile = Paths.get(proj.getInstalledDir().getPath(),
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME);

    verify(this.instance, never()).updateLastModifiedTime(projectDirSizeFile);
    assertThat(tmp.list()).contains("sample_flow_01");
    assertThat(isDownloaded).isTrue();
  }

  @Test
  public void testNotDownloadingProjectIfExists() throws Exception {
    final ProjectDirectoryMetadata proj = new ProjectDirectoryMetadata(12, 34,
        new File(this.projectsDir, "sample_project_01"));
    final File tmp = this.temporaryFolder.newFolder("tmp");

    this.instance.downloadProjectIfNotExists(proj, tmp);

    Files.move(tmp.toPath(), proj.getInstalledDir().toPath());

    // Try downloading the same project again
    final boolean isDownloaded = this.instance.downloadProjectIfNotExists(proj, tmp);

    final Path projectDirSizeFile = Paths.get(proj.getInstalledDir().getPath(),
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME);

    verify(this.instance).updateLastModifiedTime(projectDirSizeFile);
    assertThat(tmp.list()).isNullOrEmpty();
    assertThat(isDownloaded).isFalse();
  }

  @Test
  public void testSetupFlow() throws ExecutorManagerException {
    final ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(12345);
    when(executableFlow.getProjectId()).thenReturn(12);
    when(executableFlow.getVersion()).thenReturn(34);

    this.instance.setup(executableFlow);
    final File execDir = new File(this.executionsDir, "12345");
    assertTrue(execDir.exists());
    assertTrue(new File(execDir, SAMPLE_FLOW_01).exists());
  }


  @Test
  public void testProjectsCacheMetricsZeroHit() {
    //given
    final ProjectCacheMetrics cacheMetrics = new ProjectCacheMetrics();

    //when zero hit and zero miss then
    assertThat(cacheMetrics.getHitRatio()).isEqualTo(0);

    //when
    cacheMetrics.incrementCacheMiss();
    //then
    assertThat(cacheMetrics.getHitRatio()).isEqualTo(0);
  }

  @Test
  public void testProjectsCacheMetricsHit() {
    //given
    final ProjectCacheMetrics cacheMetrics = new ProjectCacheMetrics();

    //when one hit
    cacheMetrics.incrementCacheHit();
    //then
    assertThat(cacheMetrics.getHitRatio()).isEqualTo(1);

    //when one miss
    cacheMetrics.incrementCacheMiss();
    //then
    assertThat(cacheMetrics.getHitRatio()).isEqualTo(0.5);
  }
}
