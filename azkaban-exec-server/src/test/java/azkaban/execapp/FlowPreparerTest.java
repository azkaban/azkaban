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

import azkaban.executor.ExecutableFlow;
import azkaban.project.ProjectFileHandler;
import azkaban.storage.StorageManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class FlowPreparerTest {

  public static final String SAMPLE_FLOW_01 = "sample_flow_01";

  final File executionsDir = new File("executions");
  final File projectsDir = new File("projects");
  final Map<Pair<Integer, Integer>, ProjectVersion> installedProjects = new HashMap<>();

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
    tearDown();

    this.executionsDir.mkdirs();
    this.projectsDir.mkdirs();

    this.instance = spy(
        new FlowPreparer(createMockStorageManager(), this.executionsDir, this.projectsDir,
            this.installedProjects, null));
    doNothing().when(this.instance).touchIfExists(any());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(this.executionsDir);
    FileUtils.deleteDirectory(this.projectsDir);
  }

  @Test
  public void testSetupProject() throws Exception {
    final ProjectVersion pv = new ProjectVersion(12, 34,
        new File(this.projectsDir, "sample_project_01"));
    this.instance.setupProject(pv);

    final long actualDirSize = 1048835;

    assertThat(pv.getDirSizeInBytes()).isEqualTo(actualDirSize);
    assertThat(FileIOUtils.readNumberFromFile(
        Paths.get(pv.getInstalledDir().getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME)))
        .isEqualTo(actualDirSize);
    assertTrue(pv.getInstalledDir().exists());
    assertTrue(new File(pv.getInstalledDir(), "sample_flow_01").exists());
  }

  @Test
  public void testSetupProjectTouchesTheDirSizeFile() throws Exception {
    //verifies setup project touches project dir size file.
    final ProjectVersion pv = new ProjectVersion(12, 34,
        new File(this.projectsDir, "sample_project_01"));

    //setup project 1st time will not do touch
    this.instance.setupProject(pv);
    verify(this.instance, never()).touchIfExists(
        Paths.get(pv.getInstalledDir().getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME));

    this.instance.setupProject(pv);
    verify(this.instance).touchIfExists(
        Paths.get(pv.getInstalledDir().getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME));
  }

  @Test
  public void testSetupFlow() throws Exception {
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
  public void testProjectCacheDirCleanerNotEnabled() throws IOException {
    final Map<Pair<Integer, Integer>, ProjectVersion> installedProjects = new HashMap<>();

    //given
    final FlowPreparer flowPreparer = new FlowPreparer(createMockStorageManager(),
        this.executionsDir, this.projectsDir, installedProjects, null);

    //when
    final List<File> expectedRemainingFiles = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      final int projectId = i;
      final int version = 1;
      final ProjectVersion pv = new ProjectVersion(projectId, version, null);
      installedProjects.put(new Pair<>(projectId, version), pv);
      flowPreparer.setupProject(pv);
      expectedRemainingFiles.add(pv.getInstalledDir());
    }

    //then
    assertThat(this.projectsDir.listFiles()).containsExactlyInAnyOrder(expectedRemainingFiles
        .toArray(new File[expectedRemainingFiles.size()]));
  }

  @Test
  public void testProjectCacheDirCleaner() throws IOException, InterruptedException {
    final Long projectDirMaxSize = 3L;
    final Map<Pair<Integer, Integer>, ProjectVersion> installedProjects = new HashMap<>();

    //given
    final FlowPreparer flowPreparer = new FlowPreparer(createMockStorageManager(),
        this.executionsDir, this.projectsDir, installedProjects, projectDirMaxSize);

    //when
    final List<File> expectedRemainingFiles = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      final int projectId = i;
      final int version = 1;
      final ProjectVersion pv = new ProjectVersion(projectId, version, null);
      flowPreparer.setupProject(pv);

      if (i >= 2) {
        //the first file will be deleted
        expectedRemainingFiles.add(pv.getInstalledDir());
      }
      // last modified time of millis second granularity of a file is not supported by all file
      // systems, so sleep for 1 second between creation of each project dir to make their last
      // modified time different.
      Thread.sleep(1000);
    }

    //then
    assertThat(this.projectsDir.listFiles()).containsExactlyInAnyOrder(expectedRemainingFiles
        .toArray(new File[expectedRemainingFiles.size()]));
  }
}
