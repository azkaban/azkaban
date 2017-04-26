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

import azkaban.executor.ExecutableFlow;
import azkaban.project.ProjectFileHandler;
import azkaban.storage.StorageManager;
import azkaban.utils.Pair;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class FlowPreparerTest {
  public static final String SAMPLE_FLOW_01 = "sample_flow_01";

  final File executionsDir = new File("executions");
  final File projectsDir = new File("projects");
  final Map<Pair<Integer, Integer>, ProjectVersion> installedProjects = new HashMap<>();

  private FlowPreparer instance;

  @Before
  public void setUp() throws Exception {
    tearDown();

    executionsDir.mkdirs();
    projectsDir.mkdirs();

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(SAMPLE_FLOW_01 + ".zip").getFile());

    ProjectFileHandler projectFileHandler = mock(ProjectFileHandler.class);
    when(projectFileHandler.getFileType()).thenReturn("zip");
    when(projectFileHandler.getLocalFile()).thenReturn(file);

    StorageManager storageManager = mock(StorageManager.class);
    when(storageManager.getProjectFile(12, 34)).thenReturn(projectFileHandler);

    instance = new FlowPreparer(storageManager, executionsDir, projectsDir, installedProjects);
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(executionsDir);
    FileUtils.deleteDirectory(projectsDir);
  }

  @Test
  public void testSetupProject() throws Exception {
    ProjectVersion pv = new ProjectVersion(12, 34, new File(projectsDir, "sample_project_01"));
    instance.setupProject(pv);

    assertTrue(pv.getInstalledDir().exists());
    assertTrue(new File(pv.getInstalledDir(), "sample_flow_01").exists());
  }

  @Test
  public void testSetupFlow() throws Exception {
    ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(12345);
    when(executableFlow.getProjectId()).thenReturn(12);
    when(executableFlow.getVersion()).thenReturn(34);

    instance.setup(executableFlow);
    File execDir = new File(executionsDir, "12345");
    assertTrue(execDir.exists());
    assertTrue(new File(execDir, SAMPLE_FLOW_01).exists());
  }
}
