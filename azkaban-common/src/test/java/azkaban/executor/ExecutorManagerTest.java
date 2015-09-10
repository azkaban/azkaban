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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import azkaban.alert.Alerter;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.user.User;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;

/**
 * Test class for executor manager
 */
public class ExecutorManagerTest {

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
    ExecutorLoader loader) throws ExecutorManagerException {
    Props props = new Props();
    props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    props.put(ExecutorManager.AZKABAN_QUEUEPROCESSING_ENABLED, "false");

    loader.addExecutor("localhost", 12345);
    loader.addExecutor("localhost", 12346);
    return new ExecutorManager(props, loader, new HashMap<String, Alerter>());
  }

  /*
   * Test create an executor manager instance without any executor local or
   * remote
   */
  @Test(expected = ExecutorManagerException.class)
  public void testNoExecutorScenario() throws ExecutorManagerException {
    Props props = new Props();
    props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    ExecutorLoader loader = new MockExecutorLoader();
    @SuppressWarnings("unused")
    ExecutorManager manager =
      new ExecutorManager(props, loader, new HashMap<String, Alerter>());
  }

  /*
   * Test backward compatibility with just local executor
   */
  @Test
  public void testLocalExecutorScenario() throws ExecutorManagerException {
    Props props = new Props();
    props.put("executor.port", 12345);

    ExecutorLoader loader = new MockExecutorLoader();
    ExecutorManager manager =
      new ExecutorManager(props, loader, new HashMap<String, Alerter>());
    Set<Executor> activeExecutors =
      new HashSet(manager.getAllActiveExecutors());

    Assert.assertEquals(activeExecutors.size(), 1);
    Executor executor = activeExecutors.iterator().next();
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
    Props props = new Props();
    props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    ExecutorLoader loader = new MockExecutorLoader();
    Executor executor1 = loader.addExecutor("localhost", 12345);
    Executor executor2 = loader.addExecutor("localhost", 12346);

    ExecutorManager manager =
      new ExecutorManager(props, loader, new HashMap<String, Alerter>());
    Set<Executor> activeExecutors =
      new HashSet(manager.getAllActiveExecutors());
    Assert.assertArrayEquals(activeExecutors.toArray(), new Executor[] {
      executor1, executor2 });
  }

  /*
   * Test executor manager active executor reload
   */
  @Test
  public void testSetupExecutorsSucess() throws ExecutorManagerException {
    Props props = new Props();
    props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    ExecutorLoader loader = new MockExecutorLoader();
    Executor executor1 = loader.addExecutor("localhost", 12345);

    ExecutorManager manager =
      new ExecutorManager(props, loader, new HashMap<String, Alerter>());
    Assert.assertArrayEquals(manager.getAllActiveExecutors().toArray(),
      new Executor[] { executor1 });

    // mark older executor as inactive
    executor1.setActive(false);
    loader.updateExecutor(executor1);
    Executor executor2 = loader.addExecutor("localhost", 12346);
    Executor executor3 = loader.addExecutor("localhost", 12347);
    manager.setupExecutors();

    Assert.assertArrayEquals(manager.getAllActiveExecutors().toArray(),
      new Executor[] { executor2, executor3 });
  }

  /*
   * Test executor manager active executor reload and resulting in no active
   * executors
   */
  @Test(expected = ExecutorManagerException.class)
  public void testSetupExecutorsException() throws ExecutorManagerException {
    Props props = new Props();
    props.put(ExecutorManager.AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    ExecutorLoader loader = new MockExecutorLoader();
    Executor executor1 = loader.addExecutor("localhost", 12345);

    ExecutorManager manager =
      new ExecutorManager(props, loader, new HashMap<String, Alerter>());
    Set<Executor> activeExecutors =
      new HashSet(manager.getAllActiveExecutors());
    Assert.assertArrayEquals(activeExecutors.toArray(),
      new Executor[] { executor1 });

    // mark older executor as inactive
    executor1.setActive(false);
    loader.updateExecutor(executor1);
    manager.setupExecutors();
  }

  /* Test disabling queue process thread to pause dispatching */
  @Test
  public void testDisablingQueueProcessThread() throws ExecutorManagerException {
    ExecutorManager manager = createMultiExecutorManagerInstance();
    manager.enableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), true);
    manager.disableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), false);
  }

  /* Test renabling queue process thread to pause restart dispatching */
  @Test
  public void testEnablingQueueProcessThread() throws ExecutorManagerException {
    ExecutorManager manager = createMultiExecutorManagerInstance();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), false);
    manager.enableQueueProcessorThread();
    Assert.assertEquals(manager.isQueueProcessorThreadActive(), true);
  }

  /* Test submit a non-dispatched flow */
  @Test
  public void testQueuedFlows() throws ExecutorManagerException, IOException {
    ExecutorLoader loader = new MockExecutorLoader();
    ExecutorManager manager = createMultiExecutorManagerInstance(loader);
    ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    flow1.setExecutionId(1);
    ExecutableFlow flow2 = TestUtils.createExecutableFlow("exectest1", "exec2");
    flow2.setExecutionId(2);

    User testUser = TestUtils.getTestUser();
    manager.submitExecutableFlow(flow1, testUser.getUserId());
    manager.submitExecutableFlow(flow2, testUser.getUserId());

    List<ExecutableFlow> testFlows = new LinkedList<ExecutableFlow>();
    testFlows.add(flow1);
    testFlows.add(flow2);

    List<Pair<ExecutionReference, ExecutableFlow>> queuedFlowsDB =
      loader.fetchQueuedFlows();
    Assert.assertEquals(queuedFlowsDB.size(), testFlows.size());
    // Verify things are correctly setup in db
    for (Pair<ExecutionReference, ExecutableFlow> pair : queuedFlowsDB) {
      Assert.assertTrue(testFlows.contains(pair.getSecond()));
    }

    // Verify running flows using old definition of "running" flows i.e. a
    // non-dispatched flow is also considered running
    List<ExecutableFlow> managerActiveFlows = manager.getRunningFlows();
    Assert.assertTrue(managerActiveFlows.containsAll(testFlows)
      && testFlows.containsAll(managerActiveFlows));

    // Verify getQueuedFlowIds method
    Assert.assertEquals("[1, 2]", manager.getQueuedFlowIds());
  }

  /* Test submit duplicate flow when previous instance is not dispatched */
  @Test(expected = ExecutorManagerException.class)
  public void testDuplicateQueuedFlows() throws ExecutorManagerException,
    IOException {
    ExecutorManager manager = createMultiExecutorManagerInstance();
    ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    flow1.getExecutionOptions().setConcurrentOption(
      ExecutionOptions.CONCURRENT_OPTION_SKIP);

    User testUser = TestUtils.getTestUser();
    manager.submitExecutableFlow(flow1, testUser.getUserId());
    manager.submitExecutableFlow(flow1, testUser.getUserId());
  }

  /*
   * Test killing a job in preparation stage at webserver side i.e. a
   * non-dispatched flow
   */
  @Test
  public void testKillQueuedFlow() throws ExecutorManagerException, IOException {
    ExecutorLoader loader = new MockExecutorLoader();
    ExecutorManager manager = createMultiExecutorManagerInstance(loader);
    ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    User testUser = TestUtils.getTestUser();
    manager.submitExecutableFlow(flow1, testUser.getUserId());

    manager.cancelFlow(flow1, testUser.getUserId());
    ExecutableFlow fetchedFlow =
      loader.fetchExecutableFlow(flow1.getExecutionId());
    Assert.assertEquals(fetchedFlow.getStatus(), Status.FAILED);

    Assert.assertFalse(manager.getRunningFlows().contains(flow1));
  }
}
