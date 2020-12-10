/*
 * Copyright 2020 LinkedIn Corp.
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
 */
package azkaban.executor.container;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.Status;
import azkaban.utils.Props;
import java.time.Duration;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;

public class ContainerCleanupManagerTest {

  private Props props;
  private ExecutorLoader executorLoader;
  private ContainerizedImpl containerImpl;
  private final ArrayList<ExecutableFlow> executableFlows = new ArrayList<>();
  private ContainerCleanupManager cleaner;
  private final Duration staleDuration = Duration.ofDays(1);

  @Before
  public void setup() throws Exception {
    this.props = new Props();
    this.executorLoader = mock(ExecutorLoader.class);
    this.containerImpl = mock(ContainerizedImpl.class);
    this.cleaner = new ContainerCleanupManager(this.props, this.executorLoader, this.containerImpl);

    this.executableFlows.add(new ExecutableFlow());
    this.executableFlows.get(0).setExecutionId(1000);
    this.executableFlows.get(0).setStatus(Status.SUCCEEDED);

    this.executableFlows.add(new ExecutableFlow());
    this.executableFlows.get(1).setExecutionId(1001);
    this.executableFlows.get(1).setStatus(Status.FAILED);

    this.executableFlows.add(new ExecutableFlow());
    this.executableFlows.get(2).setExecutionId(1002);
    this.executableFlows.get(2).setStatus(Status.FAILED);
  }

  @Test
  public void testEmptyStaleExecutions() throws Exception {
    // List of stale flows is empty
    when(this.executorLoader.fetchStaleFlows(any())).thenReturn(new ArrayList<>());
    this.cleaner.terminateStaleContainers();
    verify(this.executorLoader).fetchStaleFlows(any());
    verifyZeroInteractions(this.containerImpl);
  }

  @Test
  public void testValidStaleExecutions() throws Exception {
    when(this.executorLoader.fetchStaleFlows(any())).thenReturn(this.executableFlows);
    this.cleaner.terminateStaleContainers();
    // Container deletion should be attempted for all executiosn in the list.
    for (final ExecutableFlow flow : this.executableFlows) {
      verify(this.containerImpl).deleteContainer(flow.getExecutionId());
    }
  }

  @Test
  public void testExceptionInFetchingExecutions() throws Exception {
    // Mock an exception while fetching stale flows.
    doThrow(new RuntimeException("mock runtime exception"))
        .when(this.executorLoader).fetchStaleFlows(any());
    // Verifies that exception is consumed, otherwise this test will fail with exception.
    this.cleaner.terminateStaleContainers();
    // Additionally verify  no invocations for container deletion should take place
    verifyZeroInteractions(this.containerImpl);
  }

  @Test
  public void testExecptionInDeletingContainer() throws Exception {
    when(this.executorLoader.fetchStaleFlows(any())).thenReturn(this.executableFlows);

    // Deleting the first execution container throws and exception.
    doThrow(new RuntimeException("mock runtime exception"))
        .when(this.containerImpl).deleteContainer(this.executableFlows.get(0).getExecutionId());
    this.cleaner.terminateStaleContainers();

    // Subsequent execution containers should still be deleted.
    for (final ExecutableFlow flow : this.executableFlows.subList(1, this.executableFlows.size())) {
      verify(this.containerImpl).deleteContainer(flow.getExecutionId());
    }
  }
}


