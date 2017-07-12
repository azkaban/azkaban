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

import static azkaban.db.AzDBTestUtility.EmbeddedH2BasicDataSource;

import azkaban.database.AzkabanDatabaseSetup;
import azkaban.db.AzkabanDataSource;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseOperatorImpl;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.QueryRunner;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcExecutorImplTest {

  private static final String UNIT_BASE_DIR =
      "../test/src/test/resources/azkaban/test/executions";
  private static final Props props = new Props();
  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(1);
  private static final Duration FLOW_FINISHED_TIME = Duration.ofMinutes(2);
  private static DatabaseOperator dbOperator;
  private ExecutorLoader loader;

  @BeforeClass
  public static void setUp() throws Exception {
    final AzkabanDataSource dataSource = new EmbeddedH2BasicDataSource();
    dbOperator = new DatabaseOperatorImpl(new QueryRunner(dataSource));

    final String sqlScriptsDir = new File("../azkaban-db/src/main/sql/").getCanonicalPath();
    props.put("database.sql.scripts.dir", sqlScriptsDir);

    // TODO kunkun-tang: Need to refactor AzkabanDatabaseSetup to accept datasource in azkaban-db
    final azkaban.database.AzkabanDataSource dataSourceForSetupDB =
        new azkaban.database.AzkabanConnectionPoolTest.EmbeddedH2BasicDataSource();
    final AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(dataSourceForSetupDB, props);
    setup.loadTableInfo();
    setup.updateDatabase(true, false);
  }

  @AfterClass
  public static void destroyDB() {
    try {
      dbOperator.update("DROP ALL OBJECTS");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  private ExecutableFlow createTestFlow() throws Exception {
    return TestUtils.createExecutableFlow("exectest1", "exec1");
  }

  @Before
  public void setup() {
    this.loader = new JdbcExecutorImpl(dbOperator);
  }

  @Test
  public void testUploadAndFetchExecutionFlows() throws Exception {

    final ExecutableFlow flow = createTestFlow();
    this.loader.uploadExecutableFlow(flow);

    final ExecutableFlow fetchFlow =
        this.loader.fetchExecutableFlow(flow.getExecutionId());

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
    Assert.assertEquals(new HashSet<>(flow.getEndNodes()),
        new HashSet<>(fetchFlow.getEndNodes()));
  }

  @Test
  public void testUpdateExecutionFlows() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    this.loader.uploadExecutableFlow(flow);

    final ExecutableFlow fetchFlow =
        this.loader.fetchExecutableFlow(flow.getExecutionId());

    fetchFlow.setEndTime(System.currentTimeMillis());
    fetchFlow.setStatus(Status.SUCCEEDED);
    this.loader.updateExecutableFlow(fetchFlow);
    final ExecutableFlow fetchFlow2 =
        this.loader.fetchExecutableFlow(flow.getExecutionId());

    Assert.assertEquals(fetchFlow2.getEndTime(), fetchFlow.getEndTime());
    Assert.assertEquals(fetchFlow2.getStatus(), fetchFlow.getStatus());
  }

  /* Test fetchQueuedFlows happy case */
  @Test
  public void testFetchQueuedFlows() throws ExecutorManagerException, IOException {

    final ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    flow.setStatus(Status.PREPARING);
    this.loader.uploadExecutableFlow(flow);
    final ExecutableFlow flow2 = TestUtils.createExecutableFlow("exectest1", "exec2");
    flow2.setStatus(Status.PREPARING);
    this.loader.uploadExecutableFlow(flow2);

    final List<Pair<ExecutionReference, ExecutableFlow>> fetchedQueuedFlows = this.loader.fetchQueuedFlows();
    Assert.assertEquals(2, fetchedQueuedFlows.size());
    final Pair<ExecutionReference, ExecutableFlow> fetchedFlow1 = fetchedQueuedFlows.get(0);
    final Pair<ExecutionReference, ExecutableFlow> fetchedFlow2 = fetchedQueuedFlows.get(1);

    Assert.assertEquals(flow.getExecutionId(), fetchedFlow1.getSecond().getExecutionId());
    Assert.assertEquals(flow.getFlowId(), fetchedFlow1.getSecond().getFlowId());
    Assert.assertEquals(flow.getProjectId(), fetchedFlow1.getSecond().getProjectId());
    Assert.assertEquals(flow2.getExecutionId(), fetchedFlow2.getSecond().getExecutionId());
    Assert.assertEquals(flow2.getFlowId(), fetchedFlow2.getSecond().getFlowId());
    Assert.assertEquals(flow2.getProjectId(), fetchedFlow2.getSecond().getProjectId());
  }

  @Test
  public void testUploadAndFetchExecutableNode() throws Exception {

    final ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    flow.setExecutionId(10);

    final File jobFile = new File(UNIT_BASE_DIR + "/exectest1", "job10.job");
    final Props props = new Props(null, jobFile);
    props.put("test", "test2");
    final ExecutableNode oldNode = flow.getExecutableNode("job10");
    oldNode.setStartTime(System.currentTimeMillis());
    this.loader.uploadExecutableNode(oldNode, props);

    final ExecutableJobInfo info = this.loader.fetchJobInfo(10, "job10", 0);
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
    final Props outputProps = new Props();
    outputProps.put("hello", "output");
    oldNode.setOutputProps(outputProps);
    oldNode.setEndTime(System.currentTimeMillis());
    this.loader.updateExecutableNode(oldNode);

    final Props fInputProps = this.loader.fetchExecutionJobInputProps(10, "job10");
    final Props fOutputProps = this.loader.fetchExecutionJobOutputProps(10, "job10");
    final Pair<Props, Props> inOutProps = this.loader.fetchExecutionJobProps(10, "job10");

    Assert.assertEquals(fInputProps.get("test"), "test2");
    Assert.assertEquals(fOutputProps.get("hello"), "output");
    Assert.assertEquals(inOutProps.getFirst().get("test"), "test2");
    Assert.assertEquals(inOutProps.getSecond().get("hello"), "output");
  }

  @Test(expected = ExecutorManagerException.class)
  public void testUnassignExecutorException() throws Exception {
      this.loader.unassignExecutor(2);
  }

  @Test
  public void testAssignAndUnassignExecutor() throws Exception {
    final String host = "localhost";
    final int port = 12345;
    final Executor executor = this.loader.addExecutor(host, port);
    final ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow);
    this.loader.assignExecutor(executor.getId(), flow.getExecutionId());
    Assert.assertEquals(
        this.loader.fetchExecutorByExecutionId(flow.getExecutionId()), executor);
    this.loader.unassignExecutor(flow.getExecutionId());
    Assert.assertEquals(
        this.loader.fetchExecutorByExecutionId(flow.getExecutionId()), null);
  }

  /* Test exception when assigning a non-existent executor to a flow */
  @Test(expected = ExecutorManagerException.class)
  public void testAssignExecutorInvalidExecutor() throws Exception {
    final ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow);
    this.loader.assignExecutor(flow.getExecutionId(), 1);
  }

  /* Test exception when assigning an executor to a non-existent flow execution */
  @Test(expected = ExecutorManagerException.class)
  public void testAssignExecutorInvalidExecution() throws Exception{
    final String host = "localhost";
    final int port = 12345;
    final Executor executor = this.loader.addExecutor(host, port);
    this.loader.assignExecutor(2, executor.getId());
  }

  /* Test null return when an invalid execution flows */
  @Test
  public void testFetchMissingExecutorByExecution() throws Exception{
    Assert.assertEquals(this.loader.fetchExecutorByExecutionId(1), null);
  }

  /* Test null return when for a non-dispatched execution */
  @Test
  public void testFetchExecutorByQueuedExecution()
      throws ExecutorManagerException, IOException {
    final ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow);
    Assert.assertEquals(this.loader.fetchExecutorByExecutionId(flow.getExecutionId()),
        null);
  }

  /* Test fetchQueuedFlows when there are no queued flows */
  @Test
  public void testFetchNoQueuedFlows() throws ExecutorManagerException,
      IOException {
    final List<Pair<ExecutionReference, ExecutableFlow>> queuedFlows =
        this.loader.fetchQueuedFlows();

    // no execution flows at all i.e. no running, completed or queued flows
    Assert.assertTrue(queuedFlows.isEmpty());

    final String host = "localhost";
    final int port = 12345;
    final Executor executor = this.loader.addExecutor(host, port);

    // When a flow is assigned an executor, it is no longer in queued state
    final ExecutableFlow flow = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow);
    this.loader.assignExecutor(executor.getId(), flow.getExecutionId());
    Assert.assertTrue(queuedFlows.isEmpty());

    // When flow status is finished, it is no longer in queued state
    final ExecutableFlow flow2 = TestUtils.createExecutableFlow("exectest1", "exec2");
    this.loader.uploadExecutableFlow(flow2);
    flow2.setStatus(Status.SUCCEEDED);
    this.loader.updateExecutableFlow(flow2);
    Assert.assertTrue(queuedFlows.isEmpty());
  }

  /* Test all executors fetch from empty executors */
  @Test
  public void testFetchEmptyExecutors() throws Exception {
    final List<Executor> executors = this.loader.fetchAllExecutors();
    Assert.assertEquals(executors.size(), 0);
  }

  /* Test active executors fetch from empty executors */
  @Test
  public void testFetchEmptyActiveExecutors() throws Exception {
    final List<Executor> executors = this.loader.fetchActiveExecutors();
    Assert.assertEquals(executors.size(), 0);
  }

  /* Test missing executor fetch with search by executor id */
  @Test
  public void testFetchMissingExecutorId() throws Exception {
    final Executor executor = this.loader.fetchExecutor(0);
    Assert.assertEquals(executor, null);
  }

  /* Test missing executor fetch with search by host:port */
  @Test
  public void testFetchMissingExecutorHostPort() throws Exception {
    final Executor executor = this.loader.fetchExecutor("localhost", 12345);
    Assert.assertEquals(executor, null);
  }

  /* Test executor events fetch from with no logged executor */
  @Test
  public void testFetchEmptyExecutorEvents() throws Exception {
    final Executor executor = new Executor(1, "localhost", 12345, true);
    final List<ExecutorLogEvent> executorEvents =
        this.loader.getExecutorEvents(executor, 5, 0);
    Assert.assertEquals(executorEvents.size(), 0);
  }

  /* Test to add duplicate executors */
  @Test
  public void testDuplicateAddExecutor() throws Exception {
    try {
      final String host = "localhost";
      final int port = 12345;
      this.loader.addExecutor(host, port);
      this.loader.addExecutor(host, port);
      Assert.fail("Expecting exception, but didn't get one");
    } catch (final ExecutorManagerException ex) {
      System.out.println("Test true");
    }
  }

  /* Test to try update a non-existent executor */
  @Test
  public void testMissingExecutorUpdate() throws Exception {
    try {
      final Executor executor = new Executor(1, "localhost", 1234, true);
      this.loader.updateExecutor(executor);
      Assert.fail("Expecting exception, but didn't get one");
    } catch (final ExecutorManagerException ex) {
      System.out.println("Test true");
    }
    clearDB();
  }

  /* Test add & fetch by Id Executors */
  @Test
  public void testSingleExecutorFetchById() throws Exception {
    final List<Executor> executors = addTestExecutors(this.loader);
    for (final Executor executor : executors) {
      final Executor fetchedExecutor = this.loader.fetchExecutor(executor.getId());
      Assert.assertEquals(executor, fetchedExecutor);
    }
  }

  /* Test fetch all executors */
  @Test
  public void testFetchAllExecutors() throws Exception {
    final List<Executor> executors = addTestExecutors(this.loader);
    executors.get(0).setActive(false);
    this.loader.updateExecutor(executors.get(0));
    final List<Executor> fetchedExecutors = this.loader.fetchAllExecutors();
    Assert.assertEquals(executors.size(), fetchedExecutors.size());
    Assert.assertArrayEquals(executors.toArray(), fetchedExecutors.toArray());
  }

  /* Test fetch only active executors */
  @Test
  public void testFetchActiveExecutors() throws Exception {
    final List<Executor> executors = addTestExecutors(this.loader);

    executors.get(0).setActive(true);
    this.loader.updateExecutor(executors.get(0));
    final List<Executor> fetchedExecutors = this.loader.fetchActiveExecutors();
    Assert.assertEquals(executors.size(), fetchedExecutors.size() + 2);
    Assert.assertEquals(executors.get(0), fetchedExecutors.get(0));
  }

  /* Test add & fetch by host:port Executors */
  @Test
  public void testSingleExecutorFetchHostPort() throws Exception {
    final List<Executor> executors = addTestExecutors(this.loader);
    for (final Executor executor : executors) {
      final Executor fetchedExecutor =
          this.loader.fetchExecutor(executor.getHost(), executor.getPort());
      Assert.assertEquals(executor, fetchedExecutor);
    }
  }

  /* Helper method used in methods testing jdbc interface for executors table */
  private List<Executor> addTestExecutors(final ExecutorLoader loader)
      throws ExecutorManagerException {
    final List<Executor> executors = new ArrayList<>();
    executors.add(loader.addExecutor("localhost1", 12345));
    executors.add(loader.addExecutor("localhost2", 12346));
    executors.add(loader.addExecutor("localhost1", 12347));
    return executors;
  }

  /* Test Removing Executor */
  @Test
  public void testRemovingExecutor() throws Exception {
    final Executor executor = this.loader.addExecutor("localhost1", 12345);
    Assert.assertNotNull(executor);
    this.loader.removeExecutor("localhost1", 12345);
    final Executor fetchedExecutor = this.loader.fetchExecutor("localhost1", 12345);
    Assert.assertNull(fetchedExecutor);
  }

  /* Test Executor reactivation */
  @Test
  public void testExecutorActivation() throws Exception {
    final Executor executor = this.loader.addExecutor("localhost1", 12345);
    Assert.assertFalse(executor.isActive());

    executor.setActive(true);
    this.loader.updateExecutor(executor);
    final Executor fetchedExecutor = this.loader.fetchExecutor(executor.getId());
    Assert.assertTrue(fetchedExecutor.isActive());
  }

  @Test
  public void testFetchActiveFlowsExecutorAssigned() throws Exception {

    // Upload flow1, executor assigned
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow1);
    final Executor executor = this.loader.addExecutor("test", 1);
    this.loader.assignExecutor(executor.getId(), flow1.getExecutionId());

    // Upload flow2, executor not assigned
    final ExecutableFlow flow2 = TestUtils.createExecutableFlow("exectest1", "exec2");
    this.loader.uploadExecutableFlow(flow2);

    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows1 =
        this.loader.fetchActiveFlows();

    Assert.assertTrue(activeFlows1.containsKey(flow1.getExecutionId()));
    Assert.assertFalse(activeFlows1.containsKey(flow2.getExecutionId()));

    final ExecutableFlow flow1Result =
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
  }

  @Test
  public void testFetchActiveFlowsStatusChanged() throws Exception {
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    // Flow status is PREPARING when uploaded, should be in active flows
    this.loader.uploadExecutableFlow(flow1);
    final Executor executor = this.loader.addExecutor("test", 1);
    this.loader.assignExecutor(executor.getId(), flow1.getExecutionId());

    Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows =
        this.loader.fetchActiveFlows();
    Assert.assertTrue(activeFlows.containsKey(flow1.getExecutionId()));

    // When flow status becomes SUCCEEDED/KILLED/FAILED, it should not be in active state
    flow1.setStatus(Status.SUCCEEDED);
    this.loader.updateExecutableFlow(flow1);
    activeFlows = this.loader.fetchActiveFlows();
    Assert.assertFalse(activeFlows.containsKey(flow1.getExecutionId()));

    flow1.setStatus(Status.KILLED);
    this.loader.updateExecutableFlow(flow1);
    activeFlows = this.loader.fetchActiveFlows();
    Assert.assertFalse(activeFlows.containsKey(flow1.getExecutionId()));

    flow1.setStatus(Status.FAILED);
    this.loader.updateExecutableFlow(flow1);
    activeFlows = this.loader.fetchActiveFlows();
    Assert.assertFalse(activeFlows.containsKey(flow1.getExecutionId()));
  }

  @Test
  public void testFetchActiveFlowsReferenceChanged() throws Exception {
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow1);
    final Executor executor = this.loader.addExecutor("test", 1);
    this.loader.assignExecutor(executor.getId(), flow1.getExecutionId());
    final ExecutionReference ref1 =
        new ExecutionReference(flow1.getExecutionId(), executor);
    this.loader.addActiveExecutableReference(ref1);

    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows1 =
        this.loader.fetchActiveFlows();
    Assert.assertTrue(activeFlows1.containsKey(flow1.getExecutionId()));

    // Verify active flows are not fetched from active_executing_flows DB table any more
    this.loader.removeActiveExecutableReference(flow1.getExecutionId());
    Assert.assertTrue(activeFlows1.containsKey(flow1.getExecutionId()));
  }

  @Test
  public void testFetchActiveFlowByExecId() throws Exception {
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow1);
    final Executor executor = this.loader.addExecutor("test", 1);
    this.loader.assignExecutor(executor.getId(), flow1.getExecutionId());

    final Pair<ExecutionReference, ExecutableFlow> activeFlow1 =
        this.loader.fetchActiveFlowByExecId(flow1.getExecutionId());

    final ExecutionReference execRef1 = activeFlow1.getFirst();
    final ExecutableFlow execFlow1 = activeFlow1.getSecond();
    Assert.assertNotNull(execRef1);
    Assert.assertNotNull(execFlow1);
    Assert.assertEquals(flow1.getExecutionId(), execFlow1.getExecutionId());
    Assert.assertEquals(flow1.getFlowId(), execFlow1.getFlowId());
    Assert.assertEquals(flow1.getProjectId(), execFlow1.getProjectId());
    Assert.assertEquals(flow1.getVersion(), execFlow1.getVersion());
  }

  @Test
  public void testFetchRecentlyFinishedFlows() throws Exception {
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow1);
    flow1.setStatus(Status.SUCCEEDED);
    flow1.setEndTime(DateTimeUtils.currentTimeMillis());
    this.loader.updateExecutableFlow(flow1);
    //Flow just finished. Fetch recently finished flows immediately. Should get it.
    final List<ExecutableFlow> flows = this.loader.fetchRecentlyFinishedFlows(
        RECENTLY_FINISHED_LIFETIME);
    Assert.assertEquals(1, flows.size());
    Assert.assertEquals(flow1.getExecutionId(), flows.get(0).getExecutionId());
    Assert.assertEquals(flow1.getProjectName(), flows.get(0).getProjectName());
    Assert.assertEquals(flow1.getFlowId(), flows.get(0).getFlowId());
    Assert.assertEquals(flow1.getVersion(), flows.get(0).getVersion());
  }

  @Test
  public void testFetchEmptyRecentlyFinishedFlows() throws Exception {
    final ExecutableFlow flow1 = TestUtils.createExecutableFlow("exectest1", "exec1");
    this.loader.uploadExecutableFlow(flow1);
    flow1.setStatus(Status.SUCCEEDED);
    flow1.setEndTime(DateTimeUtils.currentTimeMillis());
    this.loader.updateExecutableFlow(flow1);
    //Todo jamiesjc: use java8.java.time api instead of jodatime
    //Mock flow finished time to be 2 min ago.
    DateTimeUtils.setCurrentMillisOffset(-FLOW_FINISHED_TIME.toMillis());
    flow1.setEndTime(DateTimeUtils.currentTimeMillis());
    this.loader.updateExecutableFlow(flow1);
    //Fetch recently finished flows within 1 min. Should be empty.
    final List<ExecutableFlow> flows = this.loader
        .fetchRecentlyFinishedFlows(RECENTLY_FINISHED_LIFETIME);
    Assert.assertTrue(flows.isEmpty());
  }

  @Test
  public void testSmallUploadLog() throws ExecutorManagerException {
    final File logDir = new File(UNIT_BASE_DIR + "/logtest");
    final File[] smalllog =
        { new File(logDir, "log1.log"), new File(logDir, "log2.log"),
            new File(logDir, "log3.log") };

    this.loader.uploadLogFile(1, "smallFiles", 0, smalllog);

    final LogData data = this.loader.fetchLogs(1, "smallFiles", 0, 0, 50000);
    Assert.assertNotNull(data);
    Assert.assertEquals("Logs length is " + data.getLength(), data.getLength(),
        53);

    System.out.println(data.toString());

    final LogData data2 = this.loader.fetchLogs(1, "smallFiles", 0, 10, 20);
    System.out.println(data2.toString());
    Assert.assertNotNull(data2);
    Assert.assertEquals("Logs length is " + data2.getLength(),
        data2.getLength(), 20);

  }

  @Test
  public void testLargeUploadLog() throws ExecutorManagerException {
    final File logDir = new File(UNIT_BASE_DIR + "/logtest");

    // Multiple of 255 for Henry the Eigth
    final File[] largelog =
        { new File(logDir, "largeLog1.log"), new File(logDir, "largeLog2.log"),
            new File(logDir, "largeLog3.log") };

    this.loader.uploadLogFile(1, "largeFiles", 0, largelog);

    final LogData logsResult = this.loader.fetchLogs(1, "largeFiles", 0, 0, 64000);
    Assert.assertNotNull(logsResult);
    Assert.assertEquals("Logs length is " + logsResult.getLength(),
        logsResult.getLength(), 64000);

    final LogData logsResult2 = this.loader.fetchLogs(1, "largeFiles", 0, 1000, 64000);
    Assert.assertNotNull(logsResult2);
    Assert.assertEquals("Logs length is " + logsResult2.getLength(),
        logsResult2.getLength(), 64000);

    final LogData logsResult3 = this.loader.fetchLogs(1, "largeFiles", 0, 330000, 400000);
    Assert.assertNotNull(logsResult3);
    Assert.assertEquals("Logs length is " + logsResult3.getLength(),
        logsResult3.getLength(), 5493);

    final LogData logsResult4 = this.loader.fetchLogs(1, "largeFiles", 0, 340000, 400000);
    Assert.assertNull(logsResult4);

    final LogData logsResult5 = this.loader.fetchLogs(1, "largeFiles", 0, 153600, 204800);
    Assert.assertNotNull(logsResult5);
    Assert.assertEquals("Logs length is " + logsResult5.getLength(),
        logsResult5.getLength(), 181893);

    final LogData logsResult6 = this.loader.fetchLogs(1, "largeFiles", 0, 150000, 250000);
    Assert.assertNotNull(logsResult6);
    Assert.assertEquals("Logs length is " + logsResult6.getLength(),
        logsResult6.getLength(), 185493);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("DELETE FROM executors");
      dbOperator.update("DELETE FROM executor_events");
      dbOperator.update("DELETE FROM execution_logs");
      dbOperator.update("DELETE FROM execution_jobs");
      dbOperator.update("DELETE FROM execution_flows");
      dbOperator.update("DELETE FROM active_executing_flows");
      dbOperator.update("DELETE FROM active_sla");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }
}
