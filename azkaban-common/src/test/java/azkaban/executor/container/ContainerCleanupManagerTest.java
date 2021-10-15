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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.utils.Props;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ContainerCleanupManagerTest {

  private Props props;
  private ExecutorLoader executorLoader;
  private ContainerizedImpl containerImpl;
  private ContainerizedDispatchManager containerizedDispatchManager;
  private ContainerCleanupManager cleaner;

  @Before
  public void setup() throws Exception {
    this.props = new Props();
    this.executorLoader = mock(ExecutorLoader.class);
    this.containerImpl = mock(ContainerizedImpl.class);
    this.containerizedDispatchManager = mock(ContainerizedDispatchManager.class);
    this.cleaner = new ContainerCleanupManager(this.props, this.executorLoader,
        this.containerImpl, this.containerizedDispatchManager);
  }

  @Test
  public void testEmptyStaleExecutions() throws Exception {
    // List of stale flows is empty
    when(this.executorLoader.fetchStaleFlowsForStatus(any())).thenReturn(new ArrayList<>());
    this.cleaner.cleanUpStaleFlows();
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.DISPATCHING);
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.PREPARING);
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.RUNNING);
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.PAUSED);
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.KILLING);
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.EXECUTION_STOPPED);
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.FAILED_FINISHING);
    verifyZeroInteractions(this.containerImpl);
  }

  @Test
  public void testExceptionInFetchingExecutions() throws Exception {
    // Mock an exception while fetching stale flows.
    doThrow(new RuntimeException("mock runtime exception"))
        .when(this.executorLoader).fetchStaleFlowsForStatus(any());
    // Verifies that exception is consumed, otherwise this test will fail with exception.
    this.cleaner.cleanUpStaleFlows();
    // Additionally verify  no invocations for container deletion should take place
    verifyZeroInteractions(this.containerImpl);
  }

  @Test
  public void testCleanUpPreparingFlows() throws ExecutorManagerException {
    ArrayList<ExecutableFlow> executableFlows = new ArrayList<>();
    ExecutableFlow flow = new ExecutableFlow();
    flow.setExecutionId(1000);
    flow.setStatus(Status.PREPARING);
    flow.setSubmitUser("goku");
    flow.setExecutionOptions(new ExecutionOptions());
    executableFlows.add(flow);
    when(this.executorLoader.fetchStaleFlowsForStatus(Status.PREPARING))
        .thenReturn(executableFlows);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId()))
        .thenReturn(flow);
    // Mimic the successful cancellation by just setting the flow execution status to Killed.
    doAnswer(e -> {
      flow.setStatus(Status.KILLED);
      return null;
    }).when(this.containerizedDispatchManager).cancelFlow(flow, flow.getSubmitUser());
    this.cleaner.cleanUpStaleFlows(Status.PREPARING);
    Assert.assertEquals(Status.KILLED, flow.getStatus());
    verify(this.containerImpl).deleteContainer(flow.getExecutionId());
    verify(this.executorLoader, times(0)).uploadLogFile(anyInt(), any(), anyInt(), any());

    // Skip the invocation of api gateway and just utilize finalizeFlow when cancelFlow is called.
    // Also throws the exception as it will be thrown if the flow execution is unreachable.
    doAnswer(e -> {
      ExecutionControllerUtils.finalizeFlow(this.executorLoader, null, flow, "", null,
          Status.KILLED);
      throw new ExecutorManagerException("Flow execution is unreachable. Finalizing the flow.");
    }).when(this.containerizedDispatchManager).cancelFlow(flow, flow.getSubmitUser());
    flow.setStatus(Status.PREPARING);
    this.cleaner.cleanUpStaleFlows(Status.PREPARING);
    Assert.assertEquals(Status.KILLED, flow.getStatus());
    // One from the previous call to cleanUpStaleFlows
    verify(this.containerImpl, times(2)).deleteContainer(flow.getExecutionId());
    // Verify that exception is thrown while cleaning up stale flow and logs should be uploaded.
    verify(this.executorLoader).uploadLogFile(anyInt(), any(), anyInt(), any());
  }
}
