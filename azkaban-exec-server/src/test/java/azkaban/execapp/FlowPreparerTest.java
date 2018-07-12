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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.project.ProjectFileHandler;
import azkaban.storage.StorageManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Pair;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
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

  @Before
  public void setUp() throws Exception {
    tearDown();

    this.executionsDir.mkdirs();
    this.projectsDir.mkdirs();

    final ClassLoader classLoader = getClass().getClassLoader();
    final File file = new File(classLoader.getResource(SAMPLE_FLOW_01 + ".zip").getFile());

    final ProjectFileHandler projectFileHandler = mock(ProjectFileHandler.class);
    when(projectFileHandler.getFileType()).thenReturn("zip");
    when(projectFileHandler.getLocalFile()).thenReturn(file);

    final StorageManager storageManager = mock(StorageManager.class);
    when(storageManager.getProjectFile(12, 34)).thenReturn(projectFileHandler);

    this.instance = new FlowPreparer(storageManager, this.executionsDir, this.projectsDir,
        this.installedProjects);
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

    final long actualDirSize = 259;

    assertThat(pv.getDirSize()).isEqualTo(actualDirSize);
    assertThat(FileIOUtils.readNumberFromFile(
        Paths.get(pv.getInstalledDir().getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME)))
        .isEqualTo(actualDirSize);
    assertTrue(pv.getInstalledDir().exists());
    assertTrue(new File(pv.getInstalledDir(), "sample_flow_01").exists());
  }

  @Test
  public void testSetupProjectTouchesTheDirSizeFile() throws Exception {
    //verifies setup project updates last modified time of project dir size file.
    final ProjectVersion pv = new ProjectVersion(12, 34,
        new File(this.projectsDir, "sample_project_01"));
    this.instance.setupProject(pv);

    final FileTime lastModifiedTime1 = Files.getLastModifiedTime(
        Paths.get(pv.getInstalledDir().getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME));

    Thread.sleep(1000);
    this.instance.setupProject(pv);

    final FileTime lastModifiedTime2 = Files.getLastModifiedTime(
        Paths.get(pv.getInstalledDir().getPath(), FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME));

    assertThat(lastModifiedTime2).isGreaterThan(lastModifiedTime1);
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
}
