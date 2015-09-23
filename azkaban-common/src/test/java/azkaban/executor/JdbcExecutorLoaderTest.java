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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.database.DataSourceUtils;
import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.user.User;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;

public class JdbcExecutorLoaderTest {
  private static boolean testDBExists;
  /* Directory with serialized description of test flows */
  private static final String UNIT_BASE_DIR =
    "../azkaban-test/src/test/resources/executions";
  // @TODO remove this and turn into local host.
  private static final String host = "localhost";
  private static final int port = 3306;
  private static final String database = "azkaban2";
  private static final String user = "azkaban";
  private static final String password = "azkaban";
  private static final int numConnections = 10;

  @BeforeClass
  public static void setupDB() {
    DataSource dataSource =
        DataSourceUtils.getMySQLDataSource(host, port, database, user,
            password, numConnections);
    testDBExists = true;

    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    CountHandler countHandler = new CountHandler();
    QueryRunner runner = new QueryRunner();
    try {
      runner.query(connection, "SELECT COUNT(1) FROM active_executing_flows",
          countHandler);
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM execution_flows",
          countHandler);
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM execution_jobs",
          countHandler);
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM execution_logs",
          countHandler);
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM executors",
          countHandler);
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM executor_events",
          countHandler);
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    DbUtils.closeQuietly(connection);
  }

  @After
  public void clearDB() {
    if (!testDBExists) {
      return;
    }

    DataSource dataSource =
        DataSourceUtils.getMySQLDataSource(host, port, database, user,
            password, numConnections);
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, "DELETE FROM active_executing_flows");

    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM execution_flows");
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM execution_jobs");
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM execution_logs");
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM executors");
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM executor_events");
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    DbUtils.closeQuietly(connection);
  }

  @Test
  public void testUploadExecutionFlows() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");

    loader.uploadExecutableFlow(flow);

    ExecutableFlow fetchFlow =
        loader.fetchExecutableFlow(flow.getExecutionId());

    // Shouldn't be the same object.
    Assert.assertTrue(flow != fetchFlow);
    Assert.assertEquals(flow.getExecutionId(), fetchFlow.getExecutionId());
    Assert.assertEquals(flow.getEndTime(), fetchFlow.getEndTime());
    Assert.assertEquals(flow.getStartTime(), fetchFlow.getStartTime());
    Assert.assertEquals(flow.getSubmitTime(), fetchFlow.getSubmitTime());
    Assert.assertEquals(flow.getFlowId(), fetchFlow.getFlowId());
    Assert.assertEquals(flow.getProjectId(), fetchFlow.getProjectId());
    Assert.assertEquals(flow.getVersion(), fetchFlow.getVersion());
    Assert.assertEquals(flow.getExecutionOptions().getFailureAction(),
        fetchFlow.getExecutionOptions().getFailureAction());
    Assert.assertEquals(new HashSet<String>(flow.getEndNodes()),
        new HashSet<String>(fetchFlow.getEndNodes()));
  }

  @Test
  public void testUpdateExecutionFlows() throws Exception {
    if (!isTestSetup()) {
      return;
    }

    ExecutorLoader loader = createLoader();
    ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");

    loader.uploadExecutableFlow(flow);

    ExecutableFlow fetchFlow2 =
        loader.fetchExecutableFlow(flow.getExecutionId());

    fetchFlow2.setEndTime(System.currentTimeMillis());
    fetchFlow2.setStatus(Status.SUCCEEDED);
    loader.updateExecutableFlow(fetchFlow2);
    ExecutableFlow fetchFlow =
        loader.fetchExecutableFlow(flow.getExecutionId());

    // Shouldn't be the same object.
    Assert.assertTrue(flow != fetchFlow);
    Assert.assertEquals(flow.getExecutionId(), fetchFlow.getExecutionId());
    Assert.assertEquals(fetchFlow2.getEndTime(), fetchFlow.getEndTime());
    Assert.assertEquals(fetchFlow2.getStatus(), fetchFlow.getStatus());
    Assert.assertEquals(flow.getStartTime(), fetchFlow.getStartTime());
    Assert.assertEquals(flow.getSubmitTime(), fetchFlow.getSubmitTime());
    Assert.assertEquals(flow.getFlowId(), fetchFlow.getFlowId());
    Assert.assertEquals(flow.getProjectId(), fetchFlow.getProjectId());
    Assert.assertEquals(flow.getVersion(), fetchFlow.getVersion());
    Assert.assertEquals(flow.getExecutionOptions().getFailureAction(),
        fetchFlow.getExecutionOptions().getFailureAction());
    Assert.assertEquals(new HashSet<String>(flow.getEndNodes()),
        new HashSet<String>(fetchFlow.getEndNodes()));
  }

  @Test
  public void testUploadExecutableNode() throws Exception {
    if (!isTestSetup()) {
      return;
    }

    ExecutorLoader loader = createLoader();
    ExecutableFlow flow = createExecutableFlow(10, "exec1");
    flow.setExecutionId(10);

    File jobFile = new File(UNIT_BASE_DIR + "/exectest1", "job10.job");
    Props props = new Props(null, jobFile);
    props.put("test", "test2");
    ExecutableNode oldNode = flow.getExecutableNode("job10");
    oldNode.setStartTime(System.currentTimeMillis());
    loader.uploadExecutableNode(oldNode, props);

    ExecutableJobInfo info = loader.fetchJobInfo(10, "job10", 0);
    Assert.assertEquals(flow.getExecutionId(), info.getExecId());
    Assert.assertEquals(flow.getProjectId(), info.getProjectId());
    Assert.assertEquals(flow.getVersion(), info.getVersion());
    Assert.assertEquals(flow.getFlowId(), info.getFlowId());
    Assert.assertEquals(oldNode.getId(), info.getJobId());
    Assert.assertEquals(oldNode.getStatus(), info.getStatus());
    Assert.assertEquals(oldNode.getStartTime(), info.getStartTime());
    Assert.assertEquals("endTime = " + oldNode.getEndTime()
        + " info endTime = " + info.getEndTime(), oldNode.getEndTime(),
        info.getEndTime());

    // Fetch props
    Props outputProps = new Props();
    outputProps.put("hello", "output");
    oldNode.setOutputProps(outputProps);
    oldNode.setEndTime(System.currentTimeMillis());
    loader.updateExecutableNode(oldNode);

    Props fInputProps = loader.fetchExecutionJobInputProps(10, "job10");
    Props fOutputProps = loader.fetchExecutionJobOutputProps(10, "job10");
    Pair<Props, Props> inOutProps = loader.fetchExecutionJobProps(10, "job10");

    Assert.assertEquals(fInputProps.get("test"), "test2");
    Assert.assertEquals(fOutputProps.get("hello"), "output");
    Assert.assertEquals(inOutProps.getFirst().get("test"), "test2");
    Assert.assertEquals(inOutProps.getSecond().get("hello"), "output");

  }

  /* Test exception when unassigning an missing execution */
  @Test
  public void testUnassignExecutorException() throws ExecutorManagerException,
    IOException {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    try {
      loader.unassignExecutor(2);
      Assert.fail("Expecting exception, but didn't get one");
    } catch (ExecutorManagerException ex) {
      System.out.println("Test true");
    }
  }

  /* Test happy case when unassigning executor for a flow execution */
  @Test
  public void testUnassignExecutor() throws ExecutorManagerException,
    IOException {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    String host = "localhost";
    int port = 12345;
    Executor executor = loader.addExecutor(host, port);
    ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow);
    loader.assignExecutor(executor.getId(), flow.getExecutionId());
    Assert.assertEquals(
      loader.fetchExecutorByExecutionId(flow.getExecutionId()), executor);
    loader.unassignExecutor(flow.getExecutionId());
    Assert.assertEquals(
      loader.fetchExecutorByExecutionId(flow.getExecutionId()), null);
  }

  /* Test exception when assigning a non-existent executor to a flow */
  @Test
  public void testAssignExecutorInvalidExecutor()
    throws ExecutorManagerException, IOException {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow);
    try {
      loader.assignExecutor(flow.getExecutionId(), 1);
      Assert.fail("Expecting exception, but didn't get one");
    } catch (ExecutorManagerException ex) {
      System.out.println("Test true");
    }
  }

  /* Test exception when assigning an executor to a non-existent flow execution */
  @Test
  public void testAssignExecutorInvalidExecution()
    throws ExecutorManagerException, IOException {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    String host = "localhost";
    int port = 12345;
    Executor executor = loader.addExecutor(host, port);
    try {
      loader.assignExecutor(2, executor.getId());
      Assert.fail("Expecting exception, but didn't get one");
    } catch (ExecutorManagerException ex) {
      System.out.println("Test true");
    }
  }

  /* Test null return when an invalid execution flows */
  @Test
  public void testFetchMissingExecutorByExecution()
    throws ExecutorManagerException, IOException {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    Assert.assertEquals(loader.fetchExecutorByExecutionId(1), null);
  }

  /* Test null return when for a non-dispatched execution */
  @Test
  public void testFetchExecutorByQueuedExecution()
    throws ExecutorManagerException, IOException {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow);
    Assert.assertEquals(loader.fetchExecutorByExecutionId(flow.getExecutionId()),
      null);
  }

  /* Test happy case when assigning and fetching an executor to a flow execution */
  @Test
  public void testAssignAndFetchExecutor() throws ExecutorManagerException,
    IOException {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    String host = "localhost";
    int port = 12345;
    Executor executor = loader.addExecutor(host, port);
    ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow);
    loader.assignExecutor(executor.getId(), flow.getExecutionId());
    Assert.assertEquals(loader.fetchExecutorByExecutionId(flow.getExecutionId()),
      executor);
  }

  /* Test fetchQueuedFlows when there are no queued flows */
  @Test
  public void testFetchNoQueuedFlows() throws ExecutorManagerException,
    IOException {
    if (!isTestSetup()) {
      return;
    }

    ExecutorLoader loader = createLoader();
    List<Pair<ExecutionReference, ExecutableFlow>> queuedFlows =
      loader.fetchQueuedFlows();

    // no execution flows at all i.e. no running, completed or queued flows
    Assert.assertTrue(queuedFlows.isEmpty());

    String host = "lcoalhost";
    int port = 12345;
    Executor executor = loader.addExecutor(host, port);

    ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow);
    loader.assignExecutor(executor.getId(), flow.getExecutionId());
    // only completed flows
    Assert.assertTrue(queuedFlows.isEmpty());

    ExecutableFlow flow2 = TestUtils.createExecutableFlow("exectest1", "exec2");
    loader.uploadExecutableFlow(flow);
    loader.assignExecutor(executor.getId(), flow.getExecutionId());
    ExecutionReference ref = new ExecutionReference(flow2.getExecutionId());
    loader.addActiveExecutableReference(ref);
    // only running and completed flows
    Assert.assertTrue(queuedFlows.isEmpty());
  }

  /* Test fetchQueuedFlows happy case */
  @Test
  public void testFetchQueuedFlows() throws ExecutorManagerException,
    IOException {
    if (!isTestSetup()) {
      return;
    }

    ExecutorLoader loader = createLoader();
    List<Pair<ExecutionReference, ExecutableFlow>> queuedFlows =
      new LinkedList<Pair<ExecutionReference, ExecutableFlow>>();

    ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow);
    ExecutableFlow flow2 = TestUtils.createExecutableFlow("exectest1", "exec2");
    loader.uploadExecutableFlow(flow);

    ExecutionReference ref2 = new ExecutionReference(flow2.getExecutionId());
    loader.addActiveExecutableReference(ref2);
    ExecutionReference ref = new ExecutionReference(flow.getExecutionId());
    loader.addActiveExecutableReference(ref);

    queuedFlows.add(new Pair<ExecutionReference, ExecutableFlow>(ref, flow));
    queuedFlows.add(new Pair<ExecutionReference, ExecutableFlow>(ref2, flow2));

    // only running and completed flows
    Assert.assertArrayEquals(loader.fetchQueuedFlows().toArray(),
      queuedFlows.toArray());
  }

  /* Test all executors fetch from empty executors */
  @Test
  public void testFetchEmptyExecutors() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    List<Executor> executors = loader.fetchAllExecutors();
    Assert.assertEquals(executors.size(), 0);
  }

  /* Test active executors fetch from empty executors */
  @Test
  public void testFetchEmptyActiveExecutors() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    List<Executor> executors = loader.fetchActiveExecutors();
    Assert.assertEquals(executors.size(), 0);
  }

  /* Test missing executor fetch with search by executor id */
  @Test
  public void testFetchMissingExecutorId() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    Executor executor = loader.fetchExecutor(0);
    Assert.assertEquals(executor, null);
  }

  /* Test missing executor fetch with search by host:port */
  @Test
  public void testFetchMissingExecutorHostPort() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    Executor executor = loader.fetchExecutor("localhost", 12345);
    Assert.assertEquals(executor, null);
  }

  /* Test executor events fetch from with no logged executor */
  @Test
  public void testFetchEmptyExecutorEvents() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    Executor executor = new Executor(1, "localhost", 12345, true);
    List<ExecutorLogEvent> executorEvents =
      loader.getExecutorEvents(executor, 5, 0);
    Assert.assertEquals(executorEvents.size(), 0);
  }

  /* Test logging ExecutorEvents */
  @Test
  public void testExecutorEvents() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    int skip = 1;
    User user = new User("testUser");
    Executor executor = new Executor(1, "localhost", 12345, true);
    String message = "My message ";
    EventType[] events =
      { EventType.CREATED, EventType.HOST_UPDATE, EventType.INACTIVATION };

    for (EventType event : events) {
      loader.postExecutorEvent(executor, event, user.getUserId(),
        message + event.getNumVal());
    }

    List<ExecutorLogEvent> eventLogs =
      loader.getExecutorEvents(executor, 10, skip);
    Assert.assertTrue(eventLogs.size() == 2);

    for (int index = 0; index < eventLogs.size(); ++index) {
      ExecutorLogEvent eventLog = eventLogs.get(index);
      Assert.assertEquals(eventLog.getExecutorId(), executor.getId());
      Assert.assertEquals(eventLog.getUser(), user.getUserId());
      Assert.assertEquals(eventLog.getType(), events[index + skip]);
      Assert.assertEquals(eventLog.getMessage(),
        message + events[index + skip].getNumVal());
    }
  }

  /* Test to add duplicate executors */
  @Test
  public void testDuplicateAddExecutor() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    try {
      String host = "localhost";
      int port = 12345;
      loader.addExecutor(host, port);
      loader.addExecutor(host, port);
      Assert.fail("Expecting exception, but didn't get one");
    } catch (ExecutorManagerException ex) {
      System.out.println("Test true");
    }
  }

  /* Test to try update a non-existent executor */
  @Test
  public void testMissingExecutorUpdate() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    try {
      Executor executor = new Executor(1, "localhost", 1234, true);
      loader.updateExecutor(executor);
      Assert.fail("Expecting exception, but didn't get one");
    } catch (ExecutorManagerException ex) {
      System.out.println("Test true");
    }
    clearDB();
  }

  /* Test add & fetch by Id Executors */
  @Test
  public void testSingleExecutorFetchById() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    List<Executor> executors = addTestExecutors(loader);
    for (Executor executor : executors) {
      Executor fetchedExecutor = loader.fetchExecutor(executor.getId());
      Assert.assertEquals(executor, fetchedExecutor);
    }
  }

  /* Test fetch all executors */
  @Test
  public void testFetchAllExecutors() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    List<Executor> executors = addTestExecutors(loader);

    executors.get(0).setActive(false);
    loader.updateExecutor(executors.get(0));

    List<Executor> fetchedExecutors = loader.fetchAllExecutors();
    Assert.assertEquals(executors.size(), fetchedExecutors.size());

    Assert.assertArrayEquals(executors.toArray(), fetchedExecutors.toArray());
  }

  /* Test fetch only active executors */
  @Test
  public void testFetchActiveExecutors() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    List<Executor> executors = addTestExecutors(loader);

    executors.get(0).setActive(false);
    loader.updateExecutor(executors.get(0));

    List<Executor> fetchedExecutors = loader.fetchActiveExecutors();
    Assert.assertEquals(executors.size(), fetchedExecutors.size() + 1);
    executors.remove(0);

    Assert.assertArrayEquals(executors.toArray(), fetchedExecutors.toArray());
  }

  /* Test add & fetch by host:port Executors */
  @Test
  public void testSingleExecutorFetchHostPort() throws Exception {
    if (!isTestSetup()) {
      return;
    }
    ExecutorLoader loader = createLoader();
    List<Executor> executors = addTestExecutors(loader);
    for (Executor executor : executors) {
      Executor fetchedExecutor =
        loader.fetchExecutor(executor.getHost(), executor.getPort());
      Assert.assertEquals(executor, fetchedExecutor);
    }
  }

  /* Helper method used in methods testing jdbc interface for executors table */
  private List<Executor> addTestExecutors(ExecutorLoader loader)
    throws ExecutorManagerException {
    List<Executor> executors = new ArrayList<Executor>();
    executors.add(loader.addExecutor("localhost1", 12345));
    executors.add(loader.addExecutor("localhost2", 12346));
    executors.add(loader.addExecutor("localhost1", 12347));
    return executors;
  }

  /* Test Executor Inactivation */
  @Test
  public void testExecutorInactivation() throws Exception {
    if (!isTestSetup()) {
      return;
    }

    ExecutorLoader loader = createLoader();
    Executor executor = loader.addExecutor("localhost1", 12345);
    Assert.assertTrue(executor.isActive());

    executor.setActive(false);
    loader.updateExecutor(executor);

    Executor fetchedExecutor = loader.fetchExecutor(executor.getId());

    Assert.assertEquals(executor.getHost(), fetchedExecutor.getHost());
    Assert.assertEquals(executor.getId(), fetchedExecutor.getId());
    Assert.assertEquals(executor.getPort(), fetchedExecutor.getPort());
    Assert.assertFalse(fetchedExecutor.isActive());
  }

  /* Test Executor reactivation */
  @Test
  public void testExecutorActivation() throws Exception {
    if (!isTestSetup()) {
      return;
    }

    ExecutorLoader loader = createLoader();
    Executor executor = loader.addExecutor("localhost1", 12345);
    Assert.assertTrue(executor.isActive());

    executor.setActive(false);
    loader.updateExecutor(executor);
    Executor fetchedExecutor = loader.fetchExecutor(executor.getId());
    Assert.assertFalse(fetchedExecutor.isActive());

    executor.setActive(true);
    loader.updateExecutor(executor);
    fetchedExecutor = loader.fetchExecutor(executor.getId());

    Assert.assertEquals(executor, fetchedExecutor);
  }

  @Test
  public void testActiveReference() throws Exception {
    if (!isTestSetup()) {
      return;
    }

    ExecutorLoader loader = createLoader();
    ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow1);
    Executor executor = new Executor(2, "test", 1, true);
    ExecutionReference ref1 =
        new ExecutionReference(flow1.getExecutionId(), executor);
    loader.addActiveExecutableReference(ref1);

    ExecutableFlow flow2 = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow2);
    ExecutionReference ref2 =
        new ExecutionReference(flow2.getExecutionId(), executor);
    loader.addActiveExecutableReference(ref2);

    ExecutableFlow flow3 = TestUtils.createExecutableFlow("exectest1", "exec1");
    loader.uploadExecutableFlow(flow3);

    Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows1 =
        loader.fetchActiveFlows();
    ExecutableFlow flow1Result =
        activeFlows1.get(flow1.getExecutionId()).getSecond();
    Assert.assertNotNull(flow1Result);
    Assert.assertTrue(flow1 != flow1Result);
    Assert.assertEquals(flow1.getExecutionId(), flow1Result.getExecutionId());
    Assert.assertEquals(flow1.getEndTime(), flow1Result.getEndTime());
    Assert.assertEquals(flow1.getStartTime(), flow1Result.getStartTime());
    Assert.assertEquals(flow1.getSubmitTime(), flow1Result.getSubmitTime());
    Assert.assertEquals(flow1.getFlowId(), flow1Result.getFlowId());
    Assert.assertEquals(flow1.getProjectId(), flow1Result.getProjectId());
    Assert.assertEquals(flow1.getVersion(), flow1Result.getVersion());
    Assert.assertEquals(flow1.getExecutionOptions().getFailureAction(),
        flow1Result.getExecutionOptions().getFailureAction());

    ExecutableFlow flow1Result2 =
        activeFlows1.get(flow2.getExecutionId()).getSecond();
    Assert.assertNotNull(flow1Result2);
    Assert.assertTrue(flow2 != flow1Result2);
    Assert.assertEquals(flow2.getExecutionId(), flow1Result2.getExecutionId());
    Assert.assertEquals(flow2.getEndTime(), flow1Result2.getEndTime());
    Assert.assertEquals(flow2.getStartTime(), flow1Result2.getStartTime());
    Assert.assertEquals(flow2.getSubmitTime(), flow1Result2.getSubmitTime());
    Assert.assertEquals(flow2.getFlowId(), flow1Result2.getFlowId());
    Assert.assertEquals(flow2.getProjectId(), flow1Result2.getProjectId());
    Assert.assertEquals(flow2.getVersion(), flow1Result2.getVersion());
    Assert.assertEquals(flow2.getExecutionOptions().getFailureAction(),
        flow1Result2.getExecutionOptions().getFailureAction());

    loader.removeActiveExecutableReference(flow2.getExecutionId());
    Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows2 =
        loader.fetchActiveFlows();

    Assert.assertTrue(activeFlows2.containsKey(flow1.getExecutionId()));
    Assert.assertFalse(activeFlows2.containsKey(flow3.getExecutionId()));
    Assert.assertFalse(activeFlows2.containsKey(flow2.getExecutionId()));
  }

  @Ignore @Test
  public void testSmallUploadLog() throws ExecutorManagerException {
    File logDir = new File(UNIT_BASE_DIR + "logtest");
    File[] smalllog =
        { new File(logDir, "log1.log"), new File(logDir, "log2.log"),
            new File(logDir, "log3.log") };

    ExecutorLoader loader = createLoader();
    loader.uploadLogFile(1, "smallFiles", 0, smalllog);

    LogData data = loader.fetchLogs(1, "smallFiles", 0, 0, 50000);
    Assert.assertNotNull(data);
    Assert.assertEquals("Logs length is " + data.getLength(), data.getLength(),
        53);

    System.out.println(data.toString());

    LogData data2 = loader.fetchLogs(1, "smallFiles", 0, 10, 20);
    System.out.println(data2.toString());
    Assert.assertNotNull(data2);
    Assert.assertEquals("Logs length is " + data2.getLength(),
        data2.getLength(), 20);

  }

  @Ignore @Test
  public void testLargeUploadLog() throws ExecutorManagerException {
    File logDir = new File(UNIT_BASE_DIR + "logtest");

    // Multiple of 255 for Henry the Eigth
    File[] largelog =
        { new File(logDir, "largeLog1.log"), new File(logDir, "largeLog2.log"),
            new File(logDir, "largeLog3.log") };

    ExecutorLoader loader = createLoader();
    loader.uploadLogFile(1, "largeFiles", 0, largelog);

    LogData logsResult = loader.fetchLogs(1, "largeFiles", 0, 0, 64000);
    Assert.assertNotNull(logsResult);
    Assert.assertEquals("Logs length is " + logsResult.getLength(),
        logsResult.getLength(), 64000);

    LogData logsResult2 = loader.fetchLogs(1, "largeFiles", 0, 1000, 64000);
    Assert.assertNotNull(logsResult2);
    Assert.assertEquals("Logs length is " + logsResult2.getLength(),
        logsResult2.getLength(), 64000);

    LogData logsResult3 = loader.fetchLogs(1, "largeFiles", 0, 330000, 400000);
    Assert.assertNotNull(logsResult3);
    Assert.assertEquals("Logs length is " + logsResult3.getLength(),
        logsResult3.getLength(), 5493);

    LogData logsResult4 = loader.fetchLogs(1, "largeFiles", 0, 340000, 400000);
    Assert.assertNull(logsResult4);

    LogData logsResult5 = loader.fetchLogs(1, "largeFiles", 0, 153600, 204800);
    Assert.assertNotNull(logsResult5);
    Assert.assertEquals("Logs length is " + logsResult5.getLength(),
        logsResult5.getLength(), 181893);

    LogData logsResult6 = loader.fetchLogs(1, "largeFiles", 0, 150000, 250000);
    Assert.assertNotNull(logsResult6);
    Assert.assertEquals("Logs length is " + logsResult6.getLength(),
        logsResult6.getLength(), 185493);
  }

  @SuppressWarnings("static-access")
  @Ignore @Test
  public void testRemoveExecutionLogsByTime() throws ExecutorManagerException,
      IOException, InterruptedException {

    ExecutorLoader loader = createLoader();

    File logDir = new File(UNIT_BASE_DIR + "logtest");

    // Multiple of 255 for Henry the Eigth
    File[] largelog =
        { new File(logDir, "largeLog1.log"), new File(logDir, "largeLog2.log"),
            new File(logDir, "largeLog3.log") };

    DateTime time1 = DateTime.now();
    loader.uploadLogFile(1, "oldlog", 0, largelog);
    // sleep for 5 seconds
    Thread.currentThread().sleep(5000);
    loader.uploadLogFile(2, "newlog", 0, largelog);

    DateTime time2 = time1.plusMillis(2500);

    int count = loader.removeExecutionLogsByTime(time2.getMillis());
    System.out.print("Removed " + count + " records");
    LogData logs = loader.fetchLogs(1, "oldlog", 0, 0, 22222);
    Assert.assertTrue(logs == null);
    logs = loader.fetchLogs(2, "newlog", 0, 0, 22222);
    Assert.assertFalse(logs == null);
  }

  private ExecutableFlow createExecutableFlow(int executionId, String flowName)
    throws IOException {
    ExecutableFlow execFlow =
      TestUtils.createExecutableFlow("exectest1", flowName);
    execFlow.setExecutionId(executionId);
    return execFlow;
  }

  private ExecutorLoader createLoader() {
    Props props = new Props();
    props.put("database.type", "mysql");

    props.put("mysql.host", host);
    props.put("mysql.port", port);
    props.put("mysql.user", user);
    props.put("mysql.database", database);
    props.put("mysql.password", password);
    props.put("mysql.numconnections", numConnections);

    return new JdbcExecutorLoader(props);
  }

  private boolean isTestSetup() {
    if (!testDBExists) {
      System.err.println("Skipping DB test because Db not setup.");
      return false;
    }

    System.out.println("Running DB test because Db setup.");
    return true;
  }

  public static class CountHandler implements ResultSetHandler<Integer> {
    @Override
    public Integer handle(ResultSet rs) throws SQLException {
      int val = 0;
      while (rs.next()) {
        val++;
      }

      return val;
    }
  }
}
