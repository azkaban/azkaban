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
 */

package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import azkaban.alert.Alerter;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for executor manager
 */
public class ExecutorManagerTest {

  private final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new HashMap<>();
  private final CommonMetrics commonMetrics = new CommonMetrics(
      new MetricsManager(new MetricRegistry()));
  private ExecutorManager manager;
  private ExecutorLoader loader;
  private Props props;
  private User user;
  private ExecutableFlow flow1;
  private ExecutableFlow flow2;
  private AlerterHolder alertHolder;
  private ExecutorApiGateway apiGateway;
  private Alerter mailAlerter;

  @Before
  public void setup() {
    this.props = new Props();
    this.mailAlerter = mock(Alerter.class);
    this.alertHolder = mock(AlerterHolder.class);
    when(this.alertHolder.get("email")).thenReturn(this.mailAlerter);
    this.loader = new MockExecutorLoader();
  }

  @After
  public void tearDown() {
    if (this.manager != null) {
      this.manager.shutdown();
    }
  }

  /*
   * Helper method to create a ExecutorManager Instance
   */
  private ExecutorManager createMultiExecutorManagerInstance() throws Exception {
    this.props.put(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, "true");
    this.props.put(Constants.ConfigurationKeys.QUEUEPROCESSING_ENABLED, "false");
    this.loader.addExecutor("localhost", 12345);
    this.loader.addExecutor("localhost", 12346);
    return createExecutorManager();
  }

  /*
   * Test create an executor manager instance without any executor local or
   * remote
   */
  @Test(expected = ExecutorManagerException.class)
  public void testNoExecutorScenario() throws Exception {
    this.props.put(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, "true");
    @SuppressWarnings("unused") final ExecutorManager manager = createExecutorManager();
  }

  /*
   * Test backward compatibility with just local executor
   */
  @Test
  public void testLocalExecutorScenario() throws Exception {
    this.props.put("executor.port", 12345);
    final ExecutorManager manager = createExecutorManager();
    final Set<Executor> activeExecutors =
        new HashSet(manager.getAllActiveExecutors());

    Assert.assertEquals(activeExecutors.size(), 1);
    final Executor executor = activeExecutors.iterator().next();
    Assert.assertEquals(executor.getHost(), "localhost");
    Assert.assertEquals(executor.getPort(), 12345);
    Assert.assertArrayEquals(activeExecutors.toArray(), this.loader
        .fetchActiveExecutors().toArray());
  }

  /*
   * Test executor manager initialization with multiple executors
   */
  @Test
  public void testMultipleExecutorScenario() throws Exception {
    this.props.put(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, "true");
    final Executor executor1 = this.loader.addExecutor("localhost", 12345);
    final Executor executor2 = this.loader.addExecutor("localhost", 12346);

    final ExecutorManager manager = createExecutorManager();
    final Set<Executor> activeExecutors =
        new HashSet(manager.getAllActiveExecutors());
    Assert.assertArrayEquals(activeExecutors.toArray(), new Executor[]{
        executor1, executor2});
  }

  private ExecutorManager createExecutorManager()
      throws ExecutorManagerException {
    return new ExecutorManager(this.props, this.loader, this.alertHolder, this.commonMetrics,
        this.apiGateway);
  }

  /*
   * Test executor manager active executor reload
   */
  @Test
  public void testSetupExecutorsSucess() throws Exception {
    this.props.put(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, "true");
    final Executor executor1 = this.loader.addExecutor("localhost", 12345);
    final ExecutorManager manager = createExecutorManager();
    Assert.assertArrayEquals(manager.getAllActiveExecutors().toArray(),
        new Executor[]{executor1});

    // mark older executor as inactive
    executor1.setActive(false);
    this.loader.updateExecutor(executor1);
    final Executor executor2 = this.loader.addExecutor("localhost", 12346);
    final Executor executor3 = this.loader.addExecutor("localhost", 12347);
    manager.setupExecutors();

    Assert.assertArrayEquals(manager.getAllActiveExecutors().toArray(),
        new Executor[]{executor2, executor3});
  }

  /*
   * Test executor manager active executor reload and resulting in no active
   * executors
   */
  @Test(expected = ExecutorManagerException.class)
  public void testSetupExecutorsException() throws Exception {
    this.props.put(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, "true");
    final Executor executor1 = this.loader.addExecutor("localhost", 12345);
    final ExecutorManager manager = createExecutorManager();
    final Set<Executor> activeExecutors =
        new HashSet(manager.getAllActiveExecutors());
    Assert.assertArrayEquals(activeExecutors.toArray(),
        new Executor[]{executor1});

    // mark older executor as inactive
    executor1.setActive(false);
    this.loader.updateExecutor(executor1);
    manager.setupExecutors();
  }

  /* Test disabling queue process thread to pause dispatching */
  @Test
  public void testDisablingQueueProcessThread() throws Exception {
    final ExecutorManager manager = createMultiExecutorManagerInstance();
    manager.enableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), true);
    manager.disableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), false);
  }

  /* Test renabling queue process thread to pause restart dispatching */
  @Test
  public void testEnablingQueueProcessThread() throws Exception {
    final ExecutorManager manager = createMultiExecutorManagerInstance();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), false);
    manager.enableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), true);
  }

  /* Test submit a non-dispatched flow */
  @Test
  public void testQueuedFlows() throws Exception {
    final ExecutorManager manager = createMultiExecutorManagerInstance();
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    flow1.setExecutionId(1);
    final ExecutableFlow flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    flow2.setExecutionId(2);

    final User testUser = TestUtils.getTestUser();
    manager.submitExecutableFlow(flow1, testUser.getUserId());
    manager.submitExecutableFlow(flow2, testUser.getUserId());

    final List<Integer> testFlows = Arrays.asList(flow1.getExecutionId(), flow2.getExecutionId());

    final List<Pair<ExecutionReference, ExecutableFlow>> queuedFlowsDB =
        this.loader.fetchQueuedFlows();
    Assert.assertEquals(queuedFlowsDB.size(), testFlows.size());
    // Verify things are correctly setup in db
    for (final Pair<ExecutionReference, ExecutableFlow> pair : queuedFlowsDB) {
      Assert.assertTrue(testFlows.contains(pair.getSecond().getExecutionId()));
    }

    // Verify running flows using old definition of "running" flows i.e. a
    // non-dispatched flow is also considered running
    final List<Integer> managerActiveFlows = manager.getRunningFlows()
        .stream().map(ExecutableFlow::getExecutionId).collect(Collectors.toList());
    Assert.assertTrue(managerActiveFlows.containsAll(testFlows)
        && testFlows.containsAll(managerActiveFlows));

    // Verify getQueuedFlowIds method
    Assert.assertEquals("[1, 2]", manager.getQueuedFlowIds());
  }

  /* Test submit duplicate flow when previous instance is not dispatched */
  @Test(expected = ExecutorManagerException.class)
  public void testDuplicateQueuedFlows() throws Exception {
    final ExecutorManager manager = createMultiExecutorManagerInstance();
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    flow1.getExecutionOptions().setConcurrentOption(
        ExecutionOptions.CONCURRENT_OPTION_SKIP);

    final User testUser = TestUtils.getTestUser();
    manager.submitExecutableFlow(flow1, testUser.getUserId());
    manager.submitExecutableFlow(flow1, testUser.getUserId());
  }

  /*
   * Test killing a job in preparation stage at webserver side i.e. a
   * non-dispatched flow
   */
  @Test
  public void testKillQueuedFlow() throws Exception {
    final ExecutorManager manager = createMultiExecutorManagerInstance();
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    final User testUser = TestUtils.getTestUser();
    manager.submitExecutableFlow(flow1, testUser.getUserId());

    manager.cancelFlow(flow1, testUser.getUserId());
    final ExecutableFlow fetchedFlow =
        this.loader.fetchExecutableFlow(flow1.getExecutionId());
    Assert.assertEquals(fetchedFlow.getStatus(), Status.FAILED);

    Assert.assertFalse(manager.getRunningFlows().contains(flow1));
  }

  /* Flow has been running on an executor but is not any more (for example because of restart) */
  @Test
  public void testNotFoundFlows() throws Exception {
    testSetUpForRunningFlows();
    this.manager.start();
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    when(this.loader.fetchExecutableFlow(-1)).thenReturn(flow1);
    mockFlowDoesNotExist();
    this.manager.submitExecutableFlow(flow1, this.user.getUserId());
    final ExecutableFlow fetchedFlow = waitFlowFinished(flow1);
    Assert.assertEquals(fetchedFlow.getStatus(), Status.FAILED);
  }

  /**
   * 1. Executor 1 throws an exception when trying to dispatch to it
   * 2. ExecutorManager should try next executor
   * 3. Executor 2 accepts the dispatched execution
   */
  @Test
  public void testDispatchException() throws Exception {
    testSetUpForRunningFlows();
    this.manager.start();
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    when(this.loader.fetchExecutableFlow(-1)).thenReturn(flow1);
    mockFlowDoesNotExist();
    when(this.apiGateway.callWithExecutable(any(), any(), eq(ConnectorParams.EXECUTE_ACTION)))
        .thenThrow(new ExecutorManagerException("Mocked dispatch exception"))
        .thenReturn(null);
    this.manager.submitExecutableFlow(flow1, this.user.getUserId());
    waitFlowFinished(flow1);
    verify(this.apiGateway)
        .callWithExecutable(flow1, this.manager.fetchExecutor(1), ConnectorParams.EXECUTE_ACTION);
    verify(this.apiGateway)
        .callWithExecutable(flow1, this.manager.fetchExecutor(2), ConnectorParams.EXECUTE_ACTION);
    verify(this.loader, Mockito.times(1)).unassignExecutor(-1);
  }

  /**
   * ExecutorManager should try to dispatch to all executors & when both fail it should remove the
   * execution from queue and finalize it.
   */
  @Test
  public void testDispatchFailed() throws Exception {
    testSetUpForRunningFlows();
    this.manager.start();
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    flow1.getExecutionOptions().setFailureEmails(Arrays.asList("test@example.com"));
    when(this.loader.fetchExecutableFlow(-1)).thenReturn(flow1);
    when(this.apiGateway.callWithExecutable(any(), any(), eq(ConnectorParams.EXECUTE_ACTION)))
        .thenThrow(new ExecutorManagerException("Mocked dispatch exception"));
    this.manager.submitExecutableFlow(flow1, this.user.getUserId());
    waitFlowFinished(flow1);
    verify(this.apiGateway)
        .callWithExecutable(flow1, this.manager.fetchExecutor(1), ConnectorParams.EXECUTE_ACTION);
    verify(this.apiGateway)
        .callWithExecutable(flow1, this.manager.fetchExecutor(2), ConnectorParams.EXECUTE_ACTION);
    verify(this.loader, Mockito.times(2)).unassignExecutor(-1);
    verify(this.mailAlerter).alertOnError(flow1);
  }

  private void mockFlowDoesNotExist() throws Exception {
    mockUpdateResponse(ImmutableMap.of(ConnectorParams.RESPONSE_UPDATED_FLOWS,
        Collections.singletonList(ImmutableMap.of(
            ConnectorParams.UPDATE_MAP_EXEC_ID, -1,
            "error", "Flow does not exist"))));
  }

  // Suppress "unchecked generic array creation for varargs parameter".
  // No way to avoid this when mocking a method with generic varags.
  @SuppressWarnings("unchecked")
  private void mockUpdateResponse(
      final Map<String, List<Map<String, Object>>> map) throws Exception {
    doReturn(map).when(this.apiGateway).callWithExecutionId(
        any(), anyInt(), eq(ConnectorParams.UPDATE_ACTION), any(), any(), any(), any());
  }

  /*
   * Added tests for runningFlows
   * TODO: When removing queuedFlows cache, will refactor rest of the ExecutorManager test cases
   */
  @Test
  public void testSubmitFlows() throws Exception {
    testSetUpForRunningFlows();
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    this.manager.submitExecutableFlow(flow1, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(flow1);
    verify(this.loader).addActiveExecutableReference(any());
  }

  // Too many concurrent flows will fail job submission
  @Test(expected = ExecutorManagerException.class)
  public void testTooManySubmitFlows() throws Exception {
    testSetUpForRunningFlows();
    final ExecutableFlow flow1 = TestUtils
        .createTestExecutableFlowFromYaml("basicyamlshelltest", "bashSleep");
    flow1.setExecutionId(101);
    final ExecutableFlow flow2 = TestUtils
        .createTestExecutableFlowFromYaml("basicyamlshelltest", "bashSleep");
    flow2.setExecutionId(102);
    final ExecutableFlow flow3 = TestUtils
        .createTestExecutableFlowFromYaml("basicyamlshelltest", "bashSleep");
    flow3.setExecutionId(103);
    final ExecutableFlow flow4 = TestUtils
        .createTestExecutableFlowFromYaml("basicyamlshelltest", "bashSleep");
    flow4.setExecutionId(104);
    this.manager.submitExecutableFlow(flow1, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(flow1);
    this.manager.submitExecutableFlow(flow2, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(flow2);
    this.manager.submitExecutableFlow(flow3, this.user.getUserId());
    this.manager.submitExecutableFlow(flow4, this.user.getUserId());
  }

  @Ignore
  @Test
  public void testFetchAllActiveFlows() throws Exception {
    testSetUpForRunningFlows();
    final List<ExecutableFlow> flows = this.manager.getRunningFlows();
    for (final Pair<ExecutionReference, ExecutableFlow> pair : this.activeFlows.values()) {
      Assert.assertTrue(flows.contains(pair.getSecond()));
    }
  }

  @Ignore
  @Test
  public void testFetchActiveFlowByProject() throws Exception {
    testSetUpForRunningFlows();
    final List<Integer> executions = this.manager.getRunningFlows(this.flow1.getProjectId(),
        this.flow1.getFlowId());
    Assert.assertTrue(executions.contains(this.flow1.getExecutionId()));
    Assert
        .assertTrue(this.manager.isFlowRunning(this.flow1.getProjectId(), this.flow1.getFlowId()));
  }

  @Ignore
  @Test
  public void testFetchActiveFlowWithExecutor() throws Exception {
    testSetUpForRunningFlows();
    final List<Pair<ExecutableFlow, Optional<Executor>>> activeFlowsWithExecutor =
        this.manager.getActiveFlowsWithExecutor();
    Assert.assertTrue(activeFlowsWithExecutor.contains(new Pair<>(this.flow1,
        Optional.ofNullable(this.manager.fetchExecutor(this.flow1.getExecutionId())))));
    Assert.assertTrue(activeFlowsWithExecutor.contains(new Pair<>(this.flow2,
        Optional.ofNullable(this.manager.fetchExecutor(this.flow2.getExecutionId())))));
  }

  @Test
  public void testFetchAllActiveExecutorServerHosts() throws Exception {
    testSetUpForRunningFlows();
    final Set<String> activeExecutorServerHosts = this.manager.getAllActiveExecutorServerHosts();
    final Executor executor1 = this.manager.fetchExecutor(this.flow1.getExecutionId());
    final Executor executor2 = this.manager.fetchExecutor(this.flow2.getExecutionId());
    Assert.assertTrue(
        activeExecutorServerHosts.contains(executor1.getHost() + ":" + executor1.getPort()));
    Assert.assertTrue(
        activeExecutorServerHosts.contains(executor2.getHost() + ":" + executor2.getPort()));
  }

  /*
   * TODO: will move below method to setUp() and run before every test for both runningFlows and queuedFlows
   */
  private void testSetUpForRunningFlows() throws Exception {
    this.loader = mock(ExecutorLoader.class);
    this.apiGateway = mock(ExecutorApiGateway.class);
    this.user = TestUtils.getTestUser();
    this.props.put(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, "true");
    //To test runningFlows, AZKABAN_QUEUEPROCESSING_ENABLED should be set to true
    //so that flows will be dispatched to executors.
    this.props.put(Constants.ConfigurationKeys.QUEUEPROCESSING_ENABLED, "true");

    // allow two concurrent runs give one Flow
    this.props.put(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW, 2);

    final List<Executor> executors = new ArrayList<>();
    final Executor executor1 = new Executor(1, "localhost", 12345, true);
    final Executor executor2 = new Executor(2, "localhost", 12346, true);
    executors.add(executor1);
    executors.add(executor2);

    when(this.loader.fetchActiveExecutors()).thenReturn(executors);
    this.manager = createExecutorManager();

    this.flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    this.flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow1.setExecutionId(1);
    this.flow2.setExecutionId(2);
    final ExecutionReference ref1 =
        new ExecutionReference(this.flow1.getExecutionId(), executor1);
    final ExecutionReference ref2 =
        new ExecutionReference(this.flow2.getExecutionId(), executor2);
    this.activeFlows.put(this.flow1.getExecutionId(), new Pair<>(ref1, this.flow1));
    this.activeFlows.put(this.flow2.getExecutionId(), new Pair<>(ref2, this.flow2));
    when(this.loader.fetchActiveFlows()).thenReturn(this.activeFlows);
  }

  private ExecutableFlow waitFlowFinished(final ExecutableFlow flow) throws Exception {
    azkaban.test.TestUtils.await().untilAsserted(() -> assertThat(getFlowStatus(flow))
        .matches(Status::isStatusFinished, "isStatusFinished"));
    return fetchFlow(flow);
  }

  private Status getFlowStatus(final ExecutableFlow flow) throws Exception {
    return fetchFlow(flow) != null ? fetchFlow(flow).getStatus() : null;
  }

  private ExecutableFlow fetchFlow(final ExecutableFlow flow) throws ExecutorManagerException {
    return this.loader.fetchExecutableFlow(flow.getExecutionId());
  }

}
