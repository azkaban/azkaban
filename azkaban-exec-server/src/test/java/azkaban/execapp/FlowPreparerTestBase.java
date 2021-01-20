/*
 * Copyright 2021 LinkedIn Corp.
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
import azkaban.storage.ProjectStorageManager;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.DependencyTransferManager;
import azkaban.utils.Utils;
import java.io.File;
import java.util.Collections;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class FlowPreparerTestBase {
  public static final String SAMPLE_FLOW_01 = "sample_flow_01";

  public static final Integer FAT_PROJECT_ID = 10;
  public static final Integer THIN_PROJECT_ID = 11;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  protected DependencyTransferManager dependencyTransferManager;

  protected ProjectStorageManager createMockStorageManager() throws Exception {
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

  protected ExecutableFlow mockExecutableFlow(final int execId, final int projectId,
                                            final int version) {
    final ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(execId);
    when(executableFlow.getProjectId()).thenReturn(projectId);
    when(executableFlow.getVersion()).thenReturn(version);
    return executableFlow;
  }
}
