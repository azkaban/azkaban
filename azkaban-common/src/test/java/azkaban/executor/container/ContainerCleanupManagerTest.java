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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_MAX_FLOW_RUNNING_MINS;
import static azkaban.Constants.EventReporterConstants.SUBMIT_TIME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.OnContainerizedExecutionEventListener;
import azkaban.executor.Status;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

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
    when(this.executorLoader.fetchStaleFlowsForStatus(any(), any())).thenReturn(new ArrayList<>());
    this.cleaner.cleanUpStaleFlows();
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.DISPATCHING,
        this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.PREPARING, this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.RUNNING, this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.PAUSED, this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.KILLING, this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.FAILED_FINISHING, this.cleaner.getValidityMap());
    verifyZeroInteractions(this.containerImpl);
  }

  @Test
  public void testExceptionInFetchingExecutions() throws Exception {
    // Mock an exception while fetching stale flows.
    doThrow(new RuntimeException("mock runtime exception"))
        .when(this.executorLoader).fetchStaleFlowsForStatus(any(), any());
    // Verifies that exception is consumed, otherwise this test will fail with exception.
    this.cleaner.cleanUpStaleFlows();
    // Additionally verify  no invocations for container deletion should take place
    verifyZeroInteractions(this.containerImpl);
  }

  @Ignore
  @Test
  public void testCleanUpPreparingFlows() throws Exception {
    ArrayList<ExecutableFlow> executableFlows = new ArrayList<>();
    ExecutableFlow flow = new ExecutableFlow();
    flow.setExecutionId(1000);
    flow.setStatus(Status.PREPARING);
    flow.setSubmitUser("goku");
    flow.setExecutionOptions(new ExecutionOptions());
    executableFlows.add(flow);
    when(this.executorLoader
        .fetchStaleFlowsForStatus(Status.PREPARING, this.cleaner.getValidityMap()))
        .thenReturn(executableFlows);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId()))
        .thenReturn(flow);

    // Skip the invocation of api gateway and just utilize finalizeFlow when cancelFlow is called.
    doAnswer(e -> {
      ExecutionControllerUtils.finalizeFlow(this.executorLoader, null, flow, "", null,
          Status.KILLED);
      return null;
    }).when(this.containerizedDispatchManager).cancelFlow(flow, flow.getSubmitUser());

    OnContainerizedExecutionEventListener onExecutionEventListener = mock(
        OnContainerizedExecutionEventListener.class);
    ExecutionControllerUtils.onExecutionEventListener = onExecutionEventListener;

    this.cleaner.cleanUpStaleFlows(Status.PREPARING);
    TimeUnit.MILLISECONDS.sleep(10);
    Assert.assertEquals(Status.KILLED, flow.getStatus());
    verify(this.containerImpl).deleteContainer(flow.getExecutionId());
    // Verify that the flow is indeed retried.
    verify(onExecutionEventListener).onExecutionEvent(flow, Constants.RESTART_FLOW);
  }

  @Test
  public void cleanUpContainersInTerminalStatuses() throws Exception {
    Set<Integer> pods = new HashSet<>();
    pods.add(1000);
    pods.add(1001);
    final ArrayList<ExecutableFlow> executableFlows = new ArrayList<>();
    final ExecutableFlow flow = new ExecutableFlow();
    flow.setExecutionId(1000);
    flow.setStatus(Status.PREPARING);
    flow.setSubmitUser("goku");
    flow.setExecutionOptions(new ExecutionOptions());
    executableFlows.add(flow);
    when(this.executorLoader
        .fetchStaleFlowsForStatus(any(Status.class), any(ImmutableMap.class)))
        .thenReturn(executableFlows);
    when(this.containerImpl.getContainersByDuration(Duration.ZERO)).thenReturn(pods);
    this.cleaner.cleanUpContainersInTerminalStatuses();
    verify(this.containerImpl).deleteContainer(1001);
    verify(this.containerImpl, Mockito.times(0)).deleteContainer(1000);
  }
}
