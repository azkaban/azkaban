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
package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.executor.container.ContainerizedImplType;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ContainerizedDispatchManagerTest {

  private final CommonMetrics commonMetrics = new CommonMetrics(
      new MetricsManager(new MetricRegistry()));
  private Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new HashMap<>();
  private Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows = new
      HashMap<>();
  private List<Pair<ExecutionReference, ExecutableFlow>> queuedFlows = new
      ArrayList<>();
  private ExecutorLoader loader;
  private ExecutorApiGateway apiGateway;
  private ContainerizedImpl containerizedImpl;
  private ContainerizedDispatchManager containerizedDispatchManager;
  private Props props;
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
    this.containerizedImpl = mock(ContainerizedImpl.class);
    this.props.put(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW, 1);
    this.props.put(ContainerizedDispatchManagerProperties.CONTAINERIZED_IMPL_TYPE,
        ContainerizedImplType.KUBERNETES.name());
    this.flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    this.flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow3 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow4 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow1.setExecutionId(1);
    this.flow2.setExecutionId(2);
    this.flow3.setExecutionId(3);
    this.flow4.setExecutionId(4);
    this.ref1 = new ExecutionReference(this.flow1.getExecutionId(), null);
    this.ref2 = new ExecutionReference(this.flow2.getExecutionId(), null);
    this.ref3 = new ExecutionReference(this.flow3.getExecutionId(), null);

    this.activeFlows = ImmutableMap
        .of(this.flow2.getExecutionId(), new Pair<>(this.ref2, this.flow2),
            this.flow3.getExecutionId(), new Pair<>(this.ref3, this.flow3));
    when(this.loader.fetchActiveFlows()).thenReturn(this.activeFlows);
    this.queuedFlows = ImmutableList.of(new Pair<>(this.ref1, this.flow1));
    when(this.loader.fetchQueuedFlows(Status.READY)).thenReturn(this.queuedFlows);
  }

  @Test
  public void testFetchAllActiveFlows() throws Exception {
    initializeContainerizedDispatchImpl();
    initializeUnfinishedFlows();
    final List<ExecutableFlow> flows = this.containerizedDispatchManager.getRunningFlows();
    this.unfinishedFlows.values()
        .forEach(pair -> assertThat(flows.contains(pair.getSecond())).isTrue());
  }

  @Test
  public void testFetchAllActiveFlowIds() throws Exception {
    initializeContainerizedDispatchImpl();
    initializeUnfinishedFlows();
    assertThat(this.containerizedDispatchManager.getRunningFlowIds())
        .isEqualTo(new ArrayList<>(this.unfinishedFlows.keySet()));
  }

  @Test
  public void testFetchAllQueuedFlowIds() throws Exception {
    initializeContainerizedDispatchImpl();
    assertThat(this.containerizedDispatchManager.getQueuedFlowIds())
        .isEqualTo(ImmutableList.of(this.flow1.getExecutionId()));
  }

  @Test
  public void testFetchQueuedFlowSize() throws Exception {
    initializeContainerizedDispatchImpl();
    assertThat(this.containerizedDispatchManager.getQueuedFlowSize())
        .isEqualTo(this.queuedFlows.size());
  }

  @Test
  public void testFetchActiveFlowByProject() throws Exception {
    initializeContainerizedDispatchImpl();
    initializeUnfinishedFlows();
    final List<Integer> executions = this.containerizedDispatchManager
        .getRunningFlows(this.flow2.getProjectId(), this.flow2.getFlowId());
    assertThat(executions.contains(this.flow2.getExecutionId())).isTrue();
    assertThat(executions.contains(this.flow3.getExecutionId())).isTrue();
    assertThat(this.containerizedDispatchManager
        .isFlowRunning(this.flow2.getProjectId(), this.flow2.getFlowId()))
        .isTrue();
    assertThat(this.containerizedDispatchManager
        .isFlowRunning(this.flow3.getProjectId(), this.flow3.getFlowId()))
        .isTrue();
  }

  @Test
  public void testSubmitFlows() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.submitExecutableFlow(this.flow1, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(this.flow1);
  }

  @Test
  public void testSubmitFlowsExceedingMaxConcurrentRuns() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.disableQueueProcessorThread();
    this.props.put(ConfigurationKeys.CONCURRENT_RUNS_ONEFLOW_WHITELIST, "exectest1,"
        + "exec2,3");
    submitFlow(this.flow2, this.ref2);
    submitFlow(this.flow3, this.ref3);
    assertThatThrownBy(() -> this.containerizedDispatchManager
        .submitExecutableFlow(this.flow4, this.user.getUserId
            ())).isInstanceOf(ExecutorManagerException.class).hasMessageContaining("Flow " + this
        .flow4.getId() + " has more than 1 concurrent runs. Skipping");
  }

  @Test
  public void testSubmitFlowsConcurrentWhitelist() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.disableQueueProcessorThread();
    this.props.put(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW, 1);
    submitFlow(this.flow2, this.ref2);
    submitFlow(this.flow3, this.ref3);
    assertThatThrownBy(() -> this.containerizedDispatchManager
        .submitExecutableFlow(this.flow4, this.user.getUserId
            ())).isInstanceOf(ExecutorManagerException.class).hasMessageContaining("Flow " + this
        .flow4.getId() + " has more than 1 concurrent runs. Skipping");
  }


  @Test
  public void testSubmitFlowsWithSkipOption() throws Exception {
    initializeContainerizedDispatchImpl();
    submitFlow(this.flow2, this.ref2);
    this.flow3.getExecutionOptions().setConcurrentOption(ExecutionOptions.CONCURRENT_OPTION_SKIP);
    assertThatThrownBy(
        () -> this.containerizedDispatchManager
            .submitExecutableFlow(this.flow3, this.user.getUserId()))
        .isInstanceOf(ExecutorManagerException.class).hasMessageContaining(
        "Flow " + this.flow3.getId() + " is already running. Skipping execution.");
  }


  @Test
  public void testSetFlowLock() throws Exception {
    initializeContainerizedDispatchImpl();
    // trying to execute a locked flow should raise an error
    this.flow1.setLocked(true);
    final String msg = this.containerizedDispatchManager
        .submitExecutableFlow(this.flow1, this.user.getUserId());
    assertThat(msg).isEqualTo("Flow derived-member-data for project flow is locked.");

    // should succeed after unlocking the flow
    this.flow1.setLocked(false);
    this.containerizedDispatchManager.submitExecutableFlow(this.flow1, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(this.flow1);
  }

  /* Test disabling queue process thread to pause dispatching */
  @Test
  public void testDisablingQueueProcessThread() throws Exception {
    initializeContainerizedDispatchImpl();
    Assert.assertEquals(this.containerizedDispatchManager.isQueueProcessorThreadActive(), true);
    this.containerizedDispatchManager.disableQueueProcessorThread();
    Assert.assertEquals(this.containerizedDispatchManager.isQueueProcessorThreadActive(), false);
    this.containerizedDispatchManager.enableQueueProcessorThread();
  }

  /* Test renabling queue process thread to pause restart dispatching */
  @Test
  public void testEnablingQueueProcessThread() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.disableQueueProcessorThread();
    Assert.assertEquals(this.containerizedDispatchManager.isQueueProcessorThreadActive(), false);
    this.containerizedDispatchManager.enableQueueProcessorThread();
    Assert.assertEquals(this.containerizedDispatchManager.isQueueProcessorThreadActive(), true);
  }

  private void submitFlow(final ExecutableFlow flow, final ExecutionReference ref) throws
      Exception {
    when(this.loader.fetchUnfinishedFlows()).thenReturn(this.unfinishedFlows);
    when(this.loader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    this.containerizedDispatchManager.submitExecutableFlow(flow, this.user.getUserId());
    this.unfinishedFlows.put(flow.getExecutionId(), new Pair<>(ref, flow));
  }

  private void initializeUnfinishedFlows() throws Exception {
    this.unfinishedFlows = ImmutableMap
        .of(this.flow1.getExecutionId(), new Pair<>(this.ref1, this.flow1),
            this.flow2.getExecutionId(), new Pair<>(this.ref2, this.flow2),
            this.flow3.getExecutionId(), new Pair<>(this.ref3, this.flow3));
    when(this.loader.fetchUnfinishedFlows()).thenReturn(this.unfinishedFlows);
  }

  private void initializeContainerizedDispatchImpl() throws Exception{
    this.containerizedDispatchManager =
        new ContainerizedDispatchManager(this.props, this.loader,
        this.commonMetrics,
        this.apiGateway, this.containerizedImpl);
    this.containerizedDispatchManager.start();
  }

  @After
  public void shutdown() {
    if(this.containerizedDispatchManager != null) {
      this.containerizedDispatchManager.shutdown();
    }
  }
}
