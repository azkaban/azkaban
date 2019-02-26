/*
* Copyright 2018 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class ExecutionControllerTest {

  private Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new HashMap<>();
  private Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows = new
      HashMap<>();
  private List<Pair<ExecutionReference, ExecutableFlow>> queuedFlows = new
      ArrayList<>();
  private List<Executor> activeExecutors = new ArrayList<>();
  private List<Executor> allExecutors = new ArrayList<>();
  private ExecutionController controller;
  private ExecutorLoader loader;
  private ExecutorApiGateway apiGateway;
  private AlerterHolder alertHolder;
  private ExecutorHealthChecker executorHealthChecker;
  private Props props;
  private final CommonMetrics commonMetrics = new CommonMetrics(
      new MetricsManager(new MetricRegistry()));

  private User user;
  private ExecutableFlow flow1;
  private ExecutableFlow flow2;
  private ExecutableFlow flow3;
  private ExecutableFlow flow4;
  private ExecutionReference ref1;
  private ExecutionReference ref2;
  private ExecutionReference ref3;

  @Before
  public void setup() throws Exception {
    this.props = new Props();
    this.user = TestUtils.getTestUser();
    this.loader = mock(ExecutorLoader.class);
    this.apiGateway = mock(ExecutorApiGateway.class);
    this.props.put(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW, 1);
    this.alertHolder = mock(AlerterHolder.class);
    this.executorHealthChecker = mock(ExecutorHealthChecker.class);
    this.controller = new ExecutionController(this.props, this.loader, this.commonMetrics,
        this.apiGateway, this.alertHolder, this.executorHealthChecker);

    final Executor executor1 = new Executor(1, "localhost", 12345, true);
    final Executor executor2 = new Executor(2, "localhost", 12346, true);
    final Executor executor3 = new Executor(3, "localhost", 12347, false);
    this.activeExecutors = ImmutableList.of(executor1, executor2);
    this.allExecutors = ImmutableList.of(executor1, executor2, executor3);
    when(this.loader.fetchActiveExecutors()).thenReturn(this.activeExecutors);

    this.flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    this.flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow3 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow4 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow1.setExecutionId(1);
    this.flow2.setExecutionId(2);
    this.flow3.setExecutionId(3);
    this.flow4.setExecutionId(4);
    this.ref1 = new ExecutionReference(this.flow1.getExecutionId(), null);
    this.ref2 = new ExecutionReference(this.flow2.getExecutionId(), executor2);
    this.ref3 = new ExecutionReference(this.flow3.getExecutionId(), executor3);

    this.activeFlows = ImmutableMap
        .of(this.flow2.getExecutionId(), new Pair<>(this.ref2, this.flow2),
            this.flow3.getExecutionId(), new Pair<>(this.ref3, this.flow3));
    when(this.loader.fetchActiveFlows()).thenReturn(this.activeFlows);
    this.queuedFlows = ImmutableList.of(new Pair<>(this.ref1, this.flow1));
    when(this.loader.fetchQueuedFlows()).thenReturn(this.queuedFlows);
  }

  @Test
  public void testFetchAllActiveFlows() throws Exception {
    initializeUnfinishedFlows();
    final List<ExecutableFlow> flows = this.controller.getRunningFlows();
    this.unfinishedFlows.values()
        .forEach(pair -> assertThat(flows.contains(pair.getSecond())).isTrue());
  }

  @Test
  public void testFetchAllActiveFlowIds() throws Exception {
    initializeUnfinishedFlows();
    assertThat(this.controller.getRunningFlowIds())
        .isEqualTo(new ArrayList<>(this.unfinishedFlows.keySet()));
  }

  @Test
  public void testFetchAllQueuedFlowIds() throws Exception {
    assertThat(this.controller.getQueuedFlowIds())
        .isEqualTo(ImmutableList.of(this.flow1.getExecutionId()));
  }

  @Test
  public void testFetchQueuedFlowSize() throws Exception {
    assertThat(this.controller.getQueuedFlowSize()).isEqualTo(this.queuedFlows.size());
  }

  @Test
  public void testFetchActiveFlowByProject() throws Exception {
    initializeUnfinishedFlows();
    final List<Integer> executions = this.controller
        .getRunningFlows(this.flow2.getProjectId(), this.flow2.getFlowId());
    assertThat(executions.contains(this.flow2.getExecutionId())).isTrue();
    assertThat(executions.contains(this.flow3.getExecutionId())).isTrue();
    assertThat(this.controller.isFlowRunning(this.flow2.getProjectId(), this.flow2.getFlowId()))
        .isTrue();
    assertThat(this.controller.isFlowRunning(this.flow3.getProjectId(), this.flow3.getFlowId()))
        .isTrue();
  }

  @Test
  public void testFetchActiveFlowWithExecutor() throws Exception {
    initializeUnfinishedFlows();
    final List<Pair<ExecutableFlow, Optional<Executor>>> activeFlowsWithExecutor =
        this.controller.getActiveFlowsWithExecutor();
    this.unfinishedFlows.values().forEach(pair -> assertThat(activeFlowsWithExecutor
        .contains(new Pair<>(pair.getSecond(), pair.getFirst().getExecutor()))).isTrue());
  }

  @Test
  public void testFetchAllActiveExecutorServerHosts() throws Exception {
    final Set<String> activeExecutorServerHosts = this.controller.getAllActiveExecutorServerHosts();
    assertThat(activeExecutorServerHosts.size()).isEqualTo(3);
    this.allExecutors.forEach(executor -> assertThat(
        activeExecutorServerHosts.contains(executor.getHost() + ":" + executor.getPort()))
        .isTrue());
  }

  @Test
  public void testSubmitFlows() throws Exception {
    this.controller.submitExecutableFlow(this.flow1, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(this.flow1);
  }

  @Test
  public void testSubmitFlowsExceedingMaxConcurrentRuns() throws Exception {
    submitFlow(this.flow2, this.ref2);
    submitFlow(this.flow3, this.ref3);
    assertThatThrownBy(() -> this.controller.submitExecutableFlow(this.flow4, this.user.getUserId
        ())).isInstanceOf(ExecutorManagerException.class).hasMessageContaining("Flow " + this
        .flow4.getId() + " has more than 1 concurrent runs. Skipping");
  }

  @Test
  public void testSubmitFlowsWithSkipOption() throws Exception {
    submitFlow(this.flow2, this.ref2);
    this.flow3.getExecutionOptions().setConcurrentOption(ExecutionOptions.CONCURRENT_OPTION_SKIP);
    assertThatThrownBy(
        () -> this.controller.submitExecutableFlow(this.flow3, this.user.getUserId()))
        .isInstanceOf(ExecutorManagerException.class).hasMessageContaining(
        "Flow " + this.flow3.getId() + " is already running. Skipping execution.");
  }

  @Test
  public void testKillQueuedFlow() throws Exception {
    // Flow1 is not assigned to any executor and is in PREPARING status.
    submitFlow(this.flow1, this.ref1);
    this.flow1.setStatus(Status.PREPARING);
    this.controller.cancelFlow(this.flow1, this.user.getUserId());
    // Verify that the status of flow1 is finalized.
    assertThat(this.flow1.getStatus()).isEqualTo(Status.FAILED);
    this.flow1.getExecutableNodes().forEach(node -> {
      assertThat(node.getStatus()).isEqualTo(Status.KILLING);
    });
  }

  @Test
  public void testKillRunningFlow() throws Exception {
    // Flow2 is assigned to executor2 and is in RUNNING status.
    submitFlow(this.flow2, this.ref2);
    this.flow2.setStatus(Status.RUNNING);
    this.controller.cancelFlow(this.flow2, this.user.getUserId());
    // Verify that executor is called to cancel flow2.
    verify(this.apiGateway).callWithReferenceByUser(this.ref2, ConnectorParams.CANCEL_ACTION,
        this.user.getUserId());
  }

  private void submitFlow(final ExecutableFlow flow, final ExecutionReference ref) throws
      Exception {
    when(this.loader.fetchUnfinishedFlows()).thenReturn(this.unfinishedFlows);
    when(this.loader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    this.controller.submitExecutableFlow(flow, this.user.getUserId());
    this.unfinishedFlows.put(flow.getExecutionId(), new Pair<>(ref, flow));
  }

  private void initializeUnfinishedFlows() throws Exception {
    this.unfinishedFlows = ImmutableMap
        .of(this.flow1.getExecutionId(), new Pair<>(this.ref1, this.flow1),
            this.flow2.getExecutionId(), new Pair<>(this.ref2, this.flow2),
            this.flow3.getExecutionId(), new Pair<>(this.ref3, this.flow3));
    when(this.loader.fetchUnfinishedFlows()).thenReturn(this.unfinishedFlows);
  }
}
