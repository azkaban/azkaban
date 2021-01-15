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

import azkaban.execapp.metric.ProjectCacheHitRatio;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;
import azkaban.project.ProjectFileHandler;
import azkaban.spi.Dependency;
import azkaban.storage.ProjectStorageManager;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.DependencyTransferManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static azkaban.test.executions.ThinArchiveTestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public class FlowPreparerTest {

  public static final String SAMPLE_FLOW_01 = "sample_flow_01";

  public static final Integer FAT_PROJECT_ID = 10;
  public static final Integer THIN_PROJECT_ID = 11;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File executionsDir;
  private File projectsDir;
  private FlowPreparer instance;
  private DependencyTransferManager dependencyTransferManager;

  private ProjectStorageManager createMockStorageManager() throws Exception {
    final ClassLoader classLoader = getClass().getClassLoader();
    final File zipFAT = new File(classLoader.getResource(SAMPLE_FLOW_01 + ".zip").getFile());

    final File thinZipFolder = temporaryFolder.newFolder("thinproj");
    ThinArchiveTestUtils.makeSampleThinProjectDirAB(thinZipFolder);
    File zipTHIN = temporaryFolder.newFile("thinzipproj.zip");
    Utils.zipFolderContent(thinZipFolder, zipTHIN);

    final ProjectFileHandler projectFileHandlerFAT = mock(ProjectFileHandler.class);
    when(projectFileHandlerFAT.getFileType()).thenReturn("zip");
    when(projectFileHandlerFAT.getLocalFile()).thenReturn(zipFAT);
    when(projectFileHandlerFAT.getStartupDependencies()).thenReturn(Collections.emptySet());

    final ProjectFileHandler projectFileHandlerTHIN = mock(ProjectFileHandler.class);
    when(projectFileHandlerTHIN.getFileType()).thenReturn("zip");
    when(projectFileHandlerTHIN.getLocalFile()).thenReturn(zipTHIN);
    when(projectFileHandlerTHIN.getStartupDependencies()).thenReturn(ThinArchiveTestUtils.getDepSetAB());

    final ProjectStorageManager projectStorageManager = mock(ProjectStorageManager.class);
    when(projectStorageManager.getProjectFile(eq(FAT_PROJECT_ID), anyInt())).thenReturn(projectFileHandlerFAT);
    when(projectStorageManager.getProjectFile(eq(THIN_PROJECT_ID), anyInt())).thenReturn(projectFileHandlerTHIN);
    return projectStorageManager;
  }

  private ExecutableFlow mockExecutableFlow(final int execId, final int projectId,
      final int version) {
    final ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(execId);
    when(executableFlow.getProjectId()).thenReturn(projectId);
    when(executableFlow.getVersion()).thenReturn(version);
    return executableFlow;
  }

  @Before
  public void setUp() throws Exception {
    this.executionsDir = this.temporaryFolder.newFolder("executions");
    this.projectsDir = this.temporaryFolder.newFolder("projects");

    this.dependencyTransferManager = mock(DependencyTransferManager.class);

    this.instance = spy(
        new FlowPreparer(createMockStorageManager(), this.dependencyTransferManager, this.projectsDir, null,
            new ProjectCacheHitRatio(), this.executionsDir));
    doNothing().when(this.instance).updateLastModifiedTime(any());
  }

  @Test
  public void testProjectDirSizeIsSet() throws Exception {
    final ProjectDirectoryMetadata proj = new ProjectDirectoryMetadata(FAT_PROJECT_ID, 34,
        new File(this.projectsDir, SAMPLE_FLOW_01));

    final File tmp = this.instance.downloadProjectIfNotExists(proj, 123);

    final long actualDirSize = 1048835;

    assertThat(proj.getDirSizeInByte()).isEqualTo(actualDirSize);
    assertThat(FileIOUtils.readNumberFromFile(
        Paths.get(tmp.getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME)))
        .isEqualTo(actualDirSize);
  }

  @Test
  public void testDownloadingProjectIfNotExists() throws Exception {
    final ProjectDirectoryMetadata proj = new ProjectDirectoryMetadata(FAT_PROJECT_ID, 34,
        new File(this.projectsDir, SAMPLE_FLOW_01));
    final File tmp = this.instance.downloadProjectIfNotExists(proj, 124);

    final Path projectDirSizeFile = Paths.get(proj.getInstalledDir().getPath(),
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME);

    verify(this.instance, never()).updateLastModifiedTime(projectDirSizeFile);
    assertThat(tmp).isNotNull();
    assertThat(tmp.list()).contains(SAMPLE_FLOW_01);
  }

  @Test
  public void testNotDownloadingProjectIfExists() throws Exception {
    final ProjectDirectoryMetadata proj = new ProjectDirectoryMetadata(FAT_PROJECT_ID, 34,
        new File(this.projectsDir, SAMPLE_FLOW_01));
    File tmp = this.instance.downloadProjectIfNotExists(proj, 125);
    Files.move(tmp.toPath(), proj.getInstalledDir().toPath());

    // Try downloading the same project again
    tmp = this.instance.downloadProjectIfNotExists(proj, 126);

    final Path projectDirSizeFile = Paths.get(proj.getInstalledDir().getPath(),
        FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME);

    verify(this.instance).updateLastModifiedTime(projectDirSizeFile);
    assertThat(tmp).isNull();
  }

  @Test
  public void testSetupFlowByMultipleThreads() {
    final int threadNum = 4;

    final ExecutableFlow[] executableFlows = new ExecutableFlow[]{
        mockExecutableFlow(1, FAT_PROJECT_ID, 34),
        mockExecutableFlow(2, FAT_PROJECT_ID, 34),
        mockExecutableFlow(3, FAT_PROJECT_ID, 34),
        mockExecutableFlow(4, FAT_PROJECT_ID, 34)
    };

    final ExecutorService service = Executors.newFixedThreadPool(threadNum);

    final List<Future> futures = new ArrayList<>();
    for (int i = 0; i < threadNum; i++) {
      final int finalI = i;
      futures.add(service.submit(() -> {
        assertThatCode(() -> this.instance.setup(executableFlows[finalI])
        ).doesNotThrowAnyException();
      }));
    }

    for (final Future future : futures) {
      assertThatCode(() -> future.get()).doesNotThrowAnyException();
    }

    service.shutdownNow();
    for (final ExecutableFlow flow : executableFlows) {
      final File execDir = new File(this.executionsDir, String.valueOf(flow.getExecutionId()));
      assertTrue(execDir.exists());
      assertTrue(new File(execDir, SAMPLE_FLOW_01).exists());
    }
  }

  @Test
  public void testSetupFlow() throws ExecutorManagerException {
    final ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(12345);
    when(executableFlow.getProjectId()).thenReturn(FAT_PROJECT_ID);
    when(executableFlow.getVersion()).thenReturn(34);

    this.instance.setup(executableFlow);
    final File execDir = new File(this.executionsDir, "12345");
    assertTrue(execDir.exists());
    assertTrue(new File(execDir, SAMPLE_FLOW_01).exists());
  }

  @Test
  public void testDownloadAndUnzipProjectFAT() throws Exception {
    final ProjectDirectoryMetadata proj = new ProjectDirectoryMetadata(FAT_PROJECT_ID, 34,
        new File(this.projectsDir, SAMPLE_FLOW_01));
    this.instance.downloadProjectIfNotExists(proj, 124);

    // This is a fat zip, so we should not attempt to download anything! (we try to download an empty set of dependencies)
    verify(this.dependencyTransferManager).downloadAllDependencies(eq(Collections.emptySet()));
  }

  @Test
  public void testDownloadAndUnzipProjectTHIN() throws Exception {
    final ProjectDirectoryMetadata proj = new ProjectDirectoryMetadata(THIN_PROJECT_ID, 34,
        new File(this.projectsDir, SAMPLE_FLOW_01));
    this.instance.downloadProjectIfNotExists(proj, 124);

    // This is a thin zip, we expect both dependencies to be downloaded
    Set<Dependency> expectedDownloadedDeps = ThinArchiveTestUtils.getDepSetAB();
    verify(this.dependencyTransferManager).downloadAllDependencies(depSetEq(expectedDownloadedDeps));
  }

  @Test
  public void testsetupContainerizedFlow() throws ExecutorManagerException {
    final ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(12345);
    when(executableFlow.getProjectId()).thenReturn(FAT_PROJECT_ID);
    when(executableFlow.getVersion()).thenReturn(34);

    final File execDir = new File(this.executionsDir, "12345");
    this.instance.setupContainerizedExecution(executableFlow, execDir);
    assertTrue(new File(execDir, SAMPLE_FLOW_01).exists());
  }
}
