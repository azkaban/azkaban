/*
 * Copyright 2014 LinkedIn Corp.
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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.user.User;
import azkaban.utils.AbstractMailerTest;
import azkaban.utils.Emailer;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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

  @Before
  public void setup() {
    this.props = AbstractMailerTest.createMailProperties();
    this.alertHolder = new AlerterHolder(this.props, new Emailer(this.props, this.commonMetrics));
  }

  /* Helper method to create a ExecutorManager Instance */
  private ExecutorManager createMultiExecutorManagerInstance()
      throws ExecutorManagerException {
    return createMultiExecutorManagerInstance(new MockExecutorLoader());
  }

  /*
   * Helper method to create a ExecutorManager Instance with the given
   * ExecutorLoader
   */
  private ExecutorManager createMultiExecutorManagerInstance(
      final ExecutorLoader loader) throws ExecutorManagerException {
    this.props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    this.props.put(ExecutorManager.AZKABAN_QUEUEPROCESSING_ENABLED, "false");
    loader.addExecutor("localhost", 12345);
    loader.addExecutor("localhost", 12346);
    return new ExecutorManager(this.props, loader, this.alertHolder, this.commonMetrics);
  }

  /*
   * Test create an executor manager instance without any executor local or
   * remote
   */
  @Test(expected = ExecutorManagerException.class)
  public void testNoExecutorScenario() throws ExecutorManagerException {
    this.props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    final ExecutorLoader loader = new MockExecutorLoader();
    @SuppressWarnings("unused") final ExecutorManager manager =
        new ExecutorManager(this.props, loader, this.alertHolder, this.commonMetrics);
  }

  /*
   * Test backward compatibility with just local executor
   */
  @Test
  public void testLocalExecutorScenario() throws ExecutorManagerException {
    this.props.put("executor.port", 12345);
    final ExecutorLoader loader = new MockExecutorLoader();
    final ExecutorManager manager =
        new ExecutorManager(this.props, loader, this.alertHolder, this.commonMetrics);
    final Set<Executor> activeExecutors =
        new HashSet(manager.getAllActiveExecutors());

    Assert.assertEquals(activeExecutors.size(), 1);
    final Executor executor = activeExecutors.iterator().next();
    Assert.assertEquals(executor.getHost(), "localhost");
    Assert.assertEquals(executor.getPort(), 12345);
    Assert.assertArrayEquals(activeExecutors.toArray(), loader
        .fetchActiveExecutors().toArray());
  }

  /*
   * Test executor manager initialization with multiple executors
   */
  @Test
  public void testMultipleExecutorScenario() throws ExecutorManagerException {
    this.props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    final ExecutorLoader loader = new MockExecutorLoader();
    final Executor executor1 = loader.addExecutor("localhost", 12345);
    final Executor executor2 = loader.addExecutor("localhost", 12346);

    final ExecutorManager manager =
        new ExecutorManager(this.props, loader, this.alertHolder, this.commonMetrics);
    final Set<Executor> activeExecutors =
        new HashSet(manager.getAllActiveExecutors());
    Assert.assertArrayEquals(activeExecutors.toArray(), new Executor[]{
        executor1, executor2});
  }

  /*
   * Test executor manager active executor reload
   */
  @Test
  public void testSetupExecutorsSucess() throws ExecutorManagerException {
    this.props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    final ExecutorLoader loader = new MockExecutorLoader();
    final Executor executor1 = loader.addExecutor("localhost", 12345);
    final ExecutorManager manager =
        new ExecutorManager(this.props, loader, this.alertHolder, this.commonMetrics);
    Assert.assertArrayEquals(manager.getAllActiveExecutors().toArray(),
        new Executor[]{executor1});

    // mark older executor as inactive
    executor1.setActive(false);
    loader.updateExecutor(executor1);
    final Executor executor2 = loader.addExecutor("localhost", 12346);
    final Executor executor3 = loader.addExecutor("localhost", 12347);
    manager.setupExecutors();

    Assert.assertArrayEquals(manager.getAllActiveExecutors().toArray(),
        new Executor[]{executor2, executor3});
  }

  /*
   * Test executor manager active executor reload and resulting in no active
   * executors
   */
  @Test(expected = ExecutorManagerException.class)
  public void testSetupExecutorsException() throws ExecutorManagerException {
    this.props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    final ExecutorLoader loader = new MockExecutorLoader();
    final Executor executor1 = loader.addExecutor("localhost", 12345);
    final ExecutorManager manager =
        new ExecutorManager(this.props, loader, this.alertHolder, this.commonMetrics);
    final Set<Executor> activeExecutors =
        new HashSet(manager.getAllActiveExecutors());
    Assert.assertArrayEquals(activeExecutors.toArray(),
        new Executor[]{executor1});

    // mark older executor as inactive
    executor1.setActive(false);
    loader.updateExecutor(executor1);
    manager.setupExecutors();
  }

  /* Test disabling queue process thread to pause dispatching */
  @Test
  public void testDisablingQueueProcessThread() throws ExecutorManagerException {
    final ExecutorManager manager = createMultiExecutorManagerInstance();
    manager.enableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), true);
    manager.disableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), false);
  }

  /* Test renabling queue process thread to pause restart dispatching */
  @Test
  public void testEnablingQueueProcessThread() throws ExecutorManagerException {
    final ExecutorManager manager = createMultiExecutorManagerInstance();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), false);
    manager.enableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), true);
  }

  /* Test submit a non-dispatched flow */
  @Test
  public void testQueuedFlows() throws ExecutorManagerException, IOException {
    final ExecutorLoader loader = new MockExecutorLoader();
    final ExecutorManager manager = createMultiExecutorManagerInstance(loader);
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    flow1.setExecutionId(1);
    final ExecutableFlow flow2 = TestUtils.createExecutableFlow("exectest1", "exec2");
    flow2.setExecutionId(2);

    final User testUser = TestUtils.getTestUser();
    manager.submitExecutableFlow(flow1, testUser.getUserId());
    manager.submitExecutableFlow(flow2, testUser.getUserId());

    final List<Integer> testFlows = Arrays.asList(flow1.getExecutionId(), flow2.getExecutionId());

    final List<Pair<ExecutionReference, ExecutableFlow>> queuedFlowsDB =
        loader.fetchQueuedFlows();
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
  public void testDuplicateQueuedFlows() throws ExecutorManagerException,
      IOException {
    final ExecutorManager manager = createMultiExecutorManagerInstance();
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
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
  public void testKillQueuedFlow() throws ExecutorManagerException, IOException {
    final ExecutorLoader loader = new MockExecutorLoader();
    final ExecutorManager manager = createMultiExecutorManagerInstance(loader);
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    final User testUser = TestUtils.getTestUser();
    manager.submitExecutableFlow(flow1, testUser.getUserId());

    manager.cancelFlow(flow1, testUser.getUserId());
    final ExecutableFlow fetchedFlow =
        loader.fetchExecutableFlow(flow1.getExecutionId());
    Assert.assertEquals(fetchedFlow.getStatus(), Status.FAILED);

    Assert.assertFalse(manager.getRunningFlows().contains(flow1));
  }

  /*
   * Added tests for runningFlows
   * TODO: When removing queuedFlows cache, will refactor rest of the ExecutorManager test cases
   */
  @Test
  public void testSubmitFlows() throws ExecutorManagerException, IOException {
    testSetUpForRunningFlows();
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.manager.submitExecutableFlow(flow1, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(flow1);
    verify(this.loader).addActiveExecutableReference(any());
  }

  @Ignore
  @Test
  public void testFetchAllActiveFlows() throws ExecutorManagerException, IOException {
    testSetUpForRunningFlows();
    final List<ExecutableFlow> flows = this.manager.getRunningFlows();
    for (final Pair<ExecutionReference, ExecutableFlow> pair : this.activeFlows.values()) {
      Assert.assertTrue(flows.contains(pair.getSecond()));
    }
  }

  @Ignore
  @Test
  public void testFetchActiveFlowByProject() throws ExecutorManagerException, IOException {
    testSetUpForRunningFlows();
    final List<Integer> executions = this.manager.getRunningFlows(this.flow1.getProjectId(),
        this.flow1.getFlowId());
    Assert.assertTrue(executions.contains(this.flow1.getExecutionId()));
    Assert
        .assertTrue(this.manager.isFlowRunning(this.flow1.getProjectId(), this.flow1.getFlowId()));
  }

  @Ignore
  @Test
  public void testFetchActiveFlowWithExecutor() throws ExecutorManagerException, IOException {
    testSetUpForRunningFlows();
    final List<Pair<ExecutableFlow, Executor>> activeFlowsWithExecutor =
        this.manager.getActiveFlowsWithExecutor();
    Assert.assertTrue(activeFlowsWithExecutor.contains(new Pair<>(this.flow1,
        this.manager.fetchExecutor(this.flow1.getExecutionId()))));
    Assert.assertTrue(activeFlowsWithExecutor.contains(new Pair<>(this.flow2,
        this.manager.fetchExecutor(this.flow2.getExecutionId()))));
  }

  @Test
  public void testFetchAllActiveExecutorServerHosts() throws ExecutorManagerException, IOException {
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
  private void testSetUpForRunningFlows()
      throws ExecutorManagerException, IOException {
    this.loader = mock(ExecutorLoader.class);
    this.user = TestUtils.getTestUser();
    this.props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    //To test runningFlows, AZKABAN_QUEUEPROCESSING_ENABLED should be set to true
    //so that flows will be dispatched to executors.
    this.props.put(ExecutorManager.AZKABAN_QUEUEPROCESSING_ENABLED, "true");

    final List<Executor> executors = new ArrayList<>();
    final Executor executor1 = new Executor(1, "localhost", 12345, true);
    final Executor executor2 = new Executor(2, "localhost", 12346, true);
    executors.add(executor1);
    executors.add(executor2);

    when(this.loader.fetchActiveExecutors()).thenReturn(executors);
    this.manager = new ExecutorManager(this.props, this.loader, this.alertHolder,
        this.commonMetrics);

    this.flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.flow2 = TestUtils.createExecutableFlow("exectest1", "exec2");
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
}
