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

package azkaban.container;

import azkaban.execapp.FlowPreparerTestBase;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;
import azkaban.jobtype.javautils.FileUtils;
import azkaban.utils.DependencyTransferManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ContainerizedFlowPreparerTest extends FlowPreparerTestBase {
  private ContainerizedFlowPreparer instance;
  private final File execDir = new File(String.valueOf(
          ContainerizedFlowPreparer.getCurrentDir()), ContainerizedFlowPreparer.PROJECT_DIR);

  @Before
  public void setUp() throws Exception {
    this.dependencyTransferManager = mock(DependencyTransferManager.class);
    this.instance = spy(
            new ContainerizedFlowPreparer(createMockStorageManager(),
                    this.dependencyTransferManager));
  }

  @Test
  public void testSetupContainerizedFlow() throws ExecutorManagerException {
    final ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(12345);
    when(executableFlow.getProjectId()).thenReturn(FAT_PROJECT_ID);
    when(executableFlow.getVersion()).thenReturn(34);

    this.instance.setup(executableFlow);
    assertTrue(new File(execDir, SAMPLE_FLOW_01).exists());
  }

  @After
  public void cleanUp() {
    FileUtils.deleteFileOrDirectory(execDir);
  }
}
