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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.DispatchMethod;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseTransOperator;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.ProjectLoader;
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import azkaban.utils.TimeUtils;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutionFlowDaoTest {

  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(1);
  private static final Duration FLOW_FINISHED_TIME = Duration.ofMinutes(2);
  private static final Props props = new Props();
  private static DatabaseOperator dbOperator;
  private ExecutionFlowDao executionFlowDao;
  private ExecutorDao executorDao;
  private AssignExecutorDao assignExecutor;
  private FetchActiveFlowDao fetchActiveFlowDao;
  private ExecutionJobDao executionJobDao;
  private ProjectLoader loader;
  private MysqlNamedLock mysqlNamedLock;

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = Utils.initTestDB();
  }

  @AfterClass
  public static void destroyDB() throws Exception {
    try {
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() {
    this.mysqlNamedLock = mock(MysqlNamedLock.class);
    this.executionFlowDao = new ExecutionFlowDao(dbOperator, this.mysqlNamedLock);
    this.executorDao = new ExecutorDao(dbOperator);
    this.assignExecutor = new AssignExecutorDao(dbOperator, this.executorDao);
    this.fetchActiveFlowDao = new FetchActiveFlowDao(dbOperator);
    this.executionJobDao = new ExecutionJobDao(dbOperator);
    this.loader = new JdbcProjectImpl(props, dbOperator);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("DELETE FROM execution_flows");
      dbOperator.update("DELETE FROM executors");
      dbOperator.update("DELETE FROM projects");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  private ExecutableFlow createTestFlow() throws Exception {
    return TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.POLL);
  }

  private void createTestProject() {
    final String projectName = "exectest1";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser1");
    this.loader.createNewProject(projectName, projectDescription, user);
  }

  @Test
  public void testUploadAndFetchExecutionFlows() throws Exception {

    final ExecutableFlow flow = createTestFlow();
    flow.setSubmitUser("testUser1");
    flow.setStatus(Status.PREPARING);
    flow.setSubmitTime(System.currentTimeMillis());
    flow.setExecutionId(0);
    flow.setDispatchMethod(DispatchMethod.POLL);
    this.executionFlowDao.uploadExecutableFlow(flow);
    assertThat(flow.getExecutionId()).isNotEqualTo(0);

    final ExecutableFlow fetchFlow =
        this.executionFlowDao.fetchExecutableFlow(flow.getExecutionId());

    assertThat(flow).isNotSameAs(fetchFlow);
    assertTwoFlowSame(flow, fetchFlow);
  }

  @Test
  public void testUpdateExecutableFlow() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDao.uploadExecutableFlow(flow);

    final ExecutableFlow fetchFlow =
        this.executionFlowDao.fetchExecutableFlow(flow.getExecutionId());

    fetchFlow.setEndTime(System.currentTimeMillis());
    fetchFlow.setStatus(Status.SUCCEEDED);
    this.executionFlowDao.updateExecutableFlow(fetchFlow);
    final ExecutableFlow fetchFlow2 =
        this.executionFlowDao.fetchExecutableFlow(flow.getExecutionId());

    assertTwoFlowSame(fetchFlow, fetchFlow2);
  }

  @Test
  public void fetchFlowHistory() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDao.uploadExecutableFlow(flow);
    final List<ExecutableFlow> flowList1 = this.executionFlowDao.fetchFlowHistory(0, 2);
    assertThat(flowList1.size()).isEqualTo(1);

    final List<ExecutableFlow> flowList2 = this.executionFlowDao
        .fetchFlowHistory(flow.getProjectId(), flow.getId(), 0, 2);
    assertThat(flowList2.size()).isEqualTo(1);

    final ExecutableFlow fetchFlow =
        this.executionFlowDao.fetchExecutableFlow(flow.getExecutionId());
    assertTwoFlowSame(flowList1.get(0), flowList2.get(0));
    assertTwoFlowSame(flowList1.get(0), fetchFlow);
  }

  @Test
  public void fetchFlowHistoryWithStartTime() throws Exception {
    final ExecutableFlow flow1 = createExecution(
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 10:00:00"), Status.PREPARING);
    final ExecutableFlow flow2 = createExecution(
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 09:00:00"), Status.PREPARING);
    final ExecutableFlow flow3 = createExecution(
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 09:00:00"), Status.PREPARING);
    final ExecutableFlow flow4 = createExecution(
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 08:00:00"), Status.PREPARING);

    final List<ExecutableFlow> expectedFlows = new ArrayList<>();
    expectedFlows.add(flow1);
    expectedFlows.add(flow2);
    expectedFlows.add(flow3);

    final List<ExecutableFlow> flowList = this.executionFlowDao.fetchFlowHistory(
        flow1.getProjectId(), flow1.getFlowId(),
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 09:00:00"));

    assertThat(flowList).hasSize(3);
    for (int i = 0; i < flowList.size(); i++) {
      assertTwoFlowSame(flowList.get(i), expectedFlows.get(i));
    }
  }

  @Test
  public void testAdvancedFilter() throws Exception {
    createTestProject();
    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDao.uploadExecutableFlow(flow);
    final List<ExecutableFlow> flowList1 = this.executionFlowDao
        .fetchFlowHistory("exectest1", "", "", 0, -1, -1, 0, 16);
    assertThat(flowList1.size()).isEqualTo(1);

    final ExecutableFlow fetchFlow =
        this.executionFlowDao.fetchExecutableFlow(flow.getExecutionId());
    assertTwoFlowSame(flowList1.get(0), fetchFlow);
  }

  @Test
  public void testFetchRecentlyFinishedFlows() throws Exception {
    final ExecutableFlow flow1 = createExecution(System.currentTimeMillis(), Status.SUCCEEDED);
    flow1.setEndTime(System.currentTimeMillis());
    this.executionFlowDao.updateExecutableFlow(flow1);

    //Flow just finished. Fetch recently finished flows immediately. Should get it.
    final List<ExecutableFlow> flows = this.executionFlowDao.fetchRecentlyFinishedFlows(
        RECENTLY_FINISHED_LIFETIME);
    assertThat(flows.size()).isEqualTo(1);
    assertTwoFlowSame(flow1, flows.get(0));
  }

  @Test
  public void testFetchEmptyRecentlyFinishedFlows() throws Exception {
    final long currentTime = System.currentTimeMillis();

    // approximately 2 minutes ago
    final long endTime = currentTime - FLOW_FINISHED_TIME.toMillis();
    final long startTime = currentTime - FLOW_FINISHED_TIME.toMillis() - 10;

    final ExecutableFlow flow1 = createExecution(startTime, Status.SUCCEEDED);
    flow1.setEndTime(endTime);
    this.executionFlowDao.updateExecutableFlow(flow1);

    //Fetch recently finished flows within 1 min. Should be empty.
    final List<ExecutableFlow> flows = this.executionFlowDao
        .fetchRecentlyFinishedFlows(RECENTLY_FINISHED_LIFETIME);
    assertThat(flows).isEmpty();
  }

  @Test
  public void testFetchQueuedFlows() throws Exception {
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", System.currentTimeMillis(),
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, DispatchMethod.POLL);
    final ExecutableFlow flow2 = submitNewFlow("exectest1", "exec2", System.currentTimeMillis(),
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, DispatchMethod.POLL);

    final List<Pair<ExecutionReference, ExecutableFlow>> fetchedQueuedFlows =
        this.executionFlowDao.fetchQueuedFlows(Status.PREPARING);
    assertThat(fetchedQueuedFlows.size()).isEqualTo(2);
    final Pair<ExecutionReference, ExecutableFlow> fetchedFlow1 = fetchedQueuedFlows.get(0);
    final Pair<ExecutionReference, ExecutableFlow> fetchedFlow2 = fetchedQueuedFlows.get(1);

    assertTwoFlowSame(flow1, fetchedFlow1.getSecond());
    assertTwoFlowSame(flow2, fetchedFlow2.getSecond());
  }

  @Test
  public void testFetchStaleFlows() throws Exception {
    long olderThan10Days = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10);
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", olderThan10Days,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, Status.RUNNING, Optional.of(olderThan10Days),
        DispatchMethod.CONTAINERIZED);

    long olderThan5Days = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(5);
    final ExecutableFlow flow2 = submitNewFlow("exectest1", "exec3", olderThan5Days,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, Status.RUNNING, Optional.of(olderThan5Days),
        DispatchMethod.CONTAINERIZED);

    // Only the first flow in the RUNNING state should be returned.
    final List<ExecutableFlow> fetchedStaleFlows =
        this.executionFlowDao.fetchStaleFlowsForStatus(Status.RUNNING);
    assertThat(fetchedStaleFlows.size()).isEqualTo(1);

    final ExecutableFlow fetchedFlow1 = fetchedStaleFlows.get(0);
    assertTwoFlowSame(flow1, fetchedFlow1);
  }

  @Test
  public void testAssignAndUnassignExecutor() throws Exception {
    final String host = "localhost";
    final int port = 12345;
    final Executor executor = this.executorDao.addExecutor(host, port);
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.POLL);
    this.executionFlowDao.uploadExecutableFlow(flow);
    this.assignExecutor.assignExecutor(executor.getId(), flow.getExecutionId());

    final Executor fetchExecutor = this.executorDao
        .fetchExecutorByExecutionId(flow.getExecutionId());
    assertThat(fetchExecutor).isEqualTo(executor);

    this.assignExecutor.unassignExecutor(flow.getExecutionId());
    assertThat(this.executorDao.fetchExecutorByExecutionId(flow.getExecutionId())).isNull();
  }

  /* Test exception when assigning a non-existent executor to a flow */
  @Test
  public void testAssignExecutorInvalidExecutor() throws Exception {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.POLL);
    this.executionFlowDao.uploadExecutableFlow(flow);

    // Since we haven't inserted any executors, 1 should be non-existent executor id.
    assertThatThrownBy(
        () -> this.assignExecutor.assignExecutor(1, flow.getExecutionId()))
        .isInstanceOf(ExecutorManagerException.class)
        .hasMessageContaining("non-existent executor");
  }

  /* Test exception when assigning an executor to a non-existent flow execution */
  @Test
  public void testAssignExecutorInvalidExecution() throws Exception {
    final String host = "localhost";
    final int port = 12345;
    final Executor executor = this.executorDao.addExecutor(host, port);

    // Make 99 a random non-existent execution id.
    assertThatThrownBy(
        () -> this.assignExecutor.assignExecutor(executor.getId(), 99))
        .isInstanceOf(ExecutorManagerException.class)
        .hasMessageContaining("non-existent execution");
  }

  @Test
  public void testFetchActiveFlowsExecutorAssigned() throws Exception {
    final List<ExecutableFlow> flows = createExecutions();
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows =
        this.fetchActiveFlowDao.fetchActiveFlows();
    assertFound(activeFlows, flows.get(0), true);
    assertNotFound(activeFlows, flows.get(1), "Returned a queued execution");
    assertFound(activeFlows, flows.get(2), true);
    assertNotFound(activeFlows, flows.get(3), "Returned an execution with a finished status");
    assertFound(activeFlows, flows.get(4), false);
    assertTwoFlowSame(activeFlows.get(flows.get(0).getExecutionId()).getSecond(), flows.get(0));
  }

  @Test
  public void testFetchUnfinishedFlows() throws Exception {
    final List<ExecutableFlow> flows = createExecutions();
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows =
        this.fetchActiveFlowDao.fetchUnfinishedFlows();
    assertFound(unfinishedFlows, flows.get(0), true);
    assertFound(unfinishedFlows, flows.get(1), false);
    assertFound(unfinishedFlows, flows.get(2), true);
    assertNotFound(unfinishedFlows, flows.get(3), "Returned an execution with a finished status");
    assertFound(unfinishedFlows, flows.get(4), false);
    assertTwoFlowSame(unfinishedFlows.get(flows.get(0).getExecutionId()).getSecond(), flows.get(0));
  }

  @Test
  public void testFetchUnfinishedFlowsMetadata() throws Exception {
    final List<ExecutableFlow> flows = createExecutions();
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows =
        this.fetchActiveFlowDao.fetchUnfinishedFlowsMetadata();
    assertFound(unfinishedFlows, flows.get(0), true);
    assertFound(unfinishedFlows, flows.get(1), false);
    assertFound(unfinishedFlows, flows.get(2), true);
    assertNotFound(unfinishedFlows, flows.get(3), "Returned an execution with a finished status");
    assertFound(unfinishedFlows, flows.get(4), false);
    assertTwoFlowSame(unfinishedFlows.get(flows.get(0).getExecutionId()).getSecond(), flows.get(0),
        false);
    assertTwoFlowSame(unfinishedFlows.get(flows.get(1).getExecutionId()).getSecond(), flows.get(1),
        false);
    assertTwoFlowSame(unfinishedFlows.get(flows.get(2).getExecutionId()).getSecond(), flows.get(2),
        false);
  }

  @Test
  public void testFetchActiveFlowByExecId() throws Exception {
    final List<ExecutableFlow> flows = createExecutions();
    assertTwoFlowSame(
        this.fetchActiveFlowDao.fetchActiveFlowByExecId(flows.get(0).getExecutionId()).getSecond(),
        flows.get(0));
    assertThat(this.fetchActiveFlowDao.fetchActiveFlowByExecId(flows.get(1).getExecutionId()))
        .isNull();
    assertTwoFlowSame(
        this.fetchActiveFlowDao.fetchActiveFlowByExecId(flows.get(2).getExecutionId()).getSecond(),
        flows.get(2));
    assertThat(this.fetchActiveFlowDao.fetchActiveFlowByExecId(flows.get(3).getExecutionId()))
        .isNull();
    assertTwoFlowSame(
        this.fetchActiveFlowDao.fetchActiveFlowByExecId(flows.get(4).getExecutionId()).getSecond(),
        flows.get(4));
  }

  private List<ExecutableFlow> createExecutions() throws Exception {
    final Executor executor = this.executorDao.addExecutor("test", 1);
    final long currentTime = System.currentTimeMillis();

    final ExecutableFlow flow1 = createExecutionAndAssign(Status.PREPARING, executor);

    // flow2 is not assigned
    final ExecutableFlow flow2 = createExecution(currentTime, Status.PREPARING);

    final ExecutableFlow flow3 = createExecutionAndAssign(Status.RUNNING, executor);
    flow3.setStartTime(currentTime + 1);
    this.executionFlowDao.updateExecutableFlow(flow3);

    final ExecutableFlow flow4 = createExecutionAndAssign(Status.SUCCEEDED, executor);
    flow4.setStartTime(currentTime + 2);
    flow4.setEndTime(currentTime + 4);
    this.executionFlowDao.updateExecutableFlow(flow4);

    final Executor executor2 = this.executorDao.addExecutor("test2", 2);
    // flow5 is assigned to an executor that is then removed
    final ExecutableFlow flow5 = createExecutionAndAssign(Status.RUNNING, executor2);
    flow5.setStartTime(currentTime + 6);
    this.executionFlowDao.updateExecutableFlow(flow5);

    this.executorDao.removeExecutor(executor2.getHost(), executor2.getPort());
    return ImmutableList.of(flow1, flow2, flow3, flow4, flow5);
  }

  private void assertNotFound(
      final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows,
      final ExecutableFlow flow, final String failMessage) {
    assertThat(activeFlows.containsKey(flow.getExecutionId())).withFailMessage(failMessage)
        .isFalse();
  }

  private void assertFound(final Map<Integer, Pair<ExecutionReference,
      ExecutableFlow>> activeFlows, final ExecutableFlow flow, final boolean executorPresent) {
    assertThat(activeFlows.containsKey(flow.getExecutionId())).isTrue();
    assertThat(activeFlows.get(flow.getExecutionId()).getFirst().getExecutor().isPresent())
        .isEqualTo(executorPresent);
  }

  private ExecutableFlow createExecutionAndAssign(final Status status, final Executor executor)
      throws Exception {
    final ExecutableFlow flow = createExecution(System.currentTimeMillis(), status);
    this.assignExecutor.assignExecutor(executor.getId(), flow.getExecutionId());
    return flow;
  }

  private ExecutableFlow createExecution(final long startTime, final Status status)
      throws IOException, ExecutorManagerException {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.POLL);
    flow.setSubmitUser("testUser");
    flow.setSubmitTime(startTime - 1);
    flow.setStartTime(startTime);
    flow.setStatus(Status.PREPARING);
    this.executionFlowDao.uploadExecutableFlow(flow);
    flow.setStatus(status);
    this.executionFlowDao.updateExecutableFlow(flow);
    return flow;
  }

  @Test
  public void testFetchActiveFlowsStatusChanged() throws Exception {
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.POLL);
    this.executionFlowDao.uploadExecutableFlow(flow1);
    final Executor executor = this.executorDao.addExecutor("test", 1);
    this.assignExecutor.assignExecutor(executor.getId(), flow1.getExecutionId());

    Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows1 =
        this.fetchActiveFlowDao.fetchActiveFlows();

    assertThat(activeFlows1.containsKey(flow1.getExecutionId())).isTrue();

    // When flow status becomes SUCCEEDED/KILLED/FAILED, it should not be in active state
    flow1.setStatus(Status.SUCCEEDED);
    this.executionFlowDao.updateExecutableFlow(flow1);
    activeFlows1 = this.fetchActiveFlowDao.fetchActiveFlows();
    assertThat(activeFlows1.containsKey(flow1.getExecutionId())).isFalse();

    flow1.setStatus(Status.KILLED);
    this.executionFlowDao.updateExecutableFlow(flow1);
    activeFlows1 = this.fetchActiveFlowDao.fetchActiveFlows();
    assertThat(activeFlows1.containsKey(flow1.getExecutionId())).isFalse();

    flow1.setStatus(Status.FAILED);
    this.executionFlowDao.updateExecutableFlow(flow1);
    activeFlows1 = this.fetchActiveFlowDao.fetchActiveFlows();
    assertThat(activeFlows1.containsKey(flow1.getExecutionId())).isFalse();
  }

  @Test
  public void testUploadAndFetchExecutableNode() throws Exception {

    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.POLL);
    flow.setExecutionId(10);

    final File jobFile = ExecutionsTestUtil.getFlowFile("exectest1", "job10.job");
    final Props props = new Props(null, jobFile);
    props.put("test", "test2");
    final ExecutableNode oldNode = flow.getExecutableNode("job10");
    oldNode.setStartTime(System.currentTimeMillis());
    this.executionJobDao.uploadExecutableNode(oldNode, props);

    final ExecutableJobInfo info = this.executionJobDao.fetchJobInfo(10, "job10", 0);
    assertThat(flow.getEndTime()).isEqualTo(info.getEndTime());
    assertThat(flow.getProjectId()).isEqualTo(info.getProjectId());
    assertThat(flow.getVersion()).isEqualTo(info.getVersion());
    assertThat(flow.getFlowId()).isEqualTo(info.getFlowId());

    assertThat(oldNode.getId()).isEqualTo(info.getJobId());
    assertThat(oldNode.getStatus()).isEqualTo(info.getStatus());
    assertThat(oldNode.getStartTime()).isEqualTo(info.getStartTime());

    // Fetch props
    final Props outputProps = new Props();
    outputProps.put("hello", "output");
    oldNode.setOutputProps(outputProps);
    oldNode.setEndTime(System.currentTimeMillis());
    this.executionJobDao.updateExecutableNode(oldNode);

    final Props fInputProps = this.executionJobDao.fetchExecutionJobInputProps(10, "job10");
    final Props fOutputProps = this.executionJobDao.fetchExecutionJobOutputProps(10, "job10");
    final Pair<Props, Props> inOutProps = this.executionJobDao.fetchExecutionJobProps(10, "job10");

    assertThat(fInputProps.get("test")).isEqualTo("test2");
    assertThat(fOutputProps.get("hello")).isEqualTo("output");
    assertThat(inOutProps.getFirst().get("test")).isEqualTo("test2");
    assertThat(inOutProps.getSecond().get("hello")).isEqualTo("output");
  }

  @Test
  public void testSelectAndUpdateExecution() throws Exception {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.POLL);
    flow.setStatus(Status.PREPARING);
    flow.setSubmitTime(System.currentTimeMillis());
    flow.setDispatchMethod(DispatchMethod.POLL);
    this.executionFlowDao.uploadExecutableFlow(flow);
    final Executor executor = this.executorDao.addExecutor("localhost", 12345);
    assertThat(this.executionFlowDao.selectAndUpdateExecution(executor.getId(), true, DispatchMethod.POLL))
        .isEqualTo(flow.getExecutionId());
    assertThat(this.executorDao.fetchExecutorByExecutionId(flow.getExecutionId())).isEqualTo
        (executor);
  }

  @Test
  public void testLockSuccessSelectAndUpdateExecutionWithLocking() throws Exception {
    when(mysqlNamedLock.getLock(any(DatabaseTransOperator.class), any(String.class), any(Integer.class)))
        .thenReturn(true);
    when(mysqlNamedLock.releaseLock(any(DatabaseTransOperator.class), any(String.class))).thenReturn(true);
    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, DispatchMethod.POLL);
    final Executor executor1 = this.executorDao.addExecutor("localhost", 12345);
    assertThat(this.executionFlowDao.selectAndUpdateExecutionWithLocking(executor1.getId(), true, DispatchMethod.POLL))
        .isEqualTo(flow1.getExecutionId());
  }

  @Test
  public void testLockFailureSelectAndUpdateExecutionWithLocking() throws Exception {
    when(mysqlNamedLock.getLock(any(DatabaseTransOperator.class), any(String.class), any(Integer.class)))
        .thenReturn(false);
    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, DispatchMethod.POLL);
    final Executor executor1 = this.executorDao.addExecutor("localhost", 12345);
    assertThat(this.executionFlowDao.selectAndUpdateExecutionWithLocking(executor1.getId(), true, DispatchMethod.POLL
        )).isEqualTo(-1);
  }

  /**
   * This test method is written to verify that selectAndUpdateExecutionWithLocking is working as
   * expected when batch select is disabled and execution status to select READY.
   *
   * @throws Exception
   */
  @Test
  public void testLockSuccessSelectAndUpdateExecutionWithLockingWithoutBatch() throws Exception {
    when(mysqlNamedLock
        .getLock(any(DatabaseTransOperator.class), any(String.class), any(Integer.class)))
        .thenReturn(true);
    when(mysqlNamedLock.releaseLock(any(DatabaseTransOperator.class), any(String.class)))
        .thenReturn(true);
    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, Status.READY, DispatchMethod.CONTAINERIZED);
    assertThat(
        this.executionFlowDao.selectAndUpdateExecutionWithLocking(false, 2, Status.READY,
            DispatchMethod.CONTAINERIZED).size())
        .isEqualTo(1);
    Set<Integer> expectedSet = new HashSet<>();
    expectedSet.add(flow1.getExecutionId());
    assertThat(this.executionFlowDao.selectAndUpdateExecutionWithLocking(false, 2, Status.READY,
        DispatchMethod.CONTAINERIZED))
        .isEqualTo(expectedSet);
  }

  /**
   * This method is used to verify that selectAndUpdateExecutionWithLocking is working as expected
   * when batch is enabled and execution status to select is READY.
   *
   * @throws Exception
   */
  @Test
  public void testLockSuccessSelectAndUpdateExecutionWithLockingWithBatch() throws Exception {
    when(mysqlNamedLock
        .getLock(any(DatabaseTransOperator.class), any(String.class), any(Integer.class)))
        .thenReturn(true);
    when(mysqlNamedLock.releaseLock(any(DatabaseTransOperator.class), any(String.class)))
        .thenReturn(true);
    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, Status.READY, DispatchMethod.CONTAINERIZED);
    final ExecutableFlow flow2 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, Status.READY, DispatchMethod.CONTAINERIZED);
    assertThat(
        this.executionFlowDao.selectAndUpdateExecutionWithLocking(true, 2, Status.READY, DispatchMethod.CONTAINERIZED).size())
        .isEqualTo(2);
    Set<Integer> expectedSet = new HashSet<>();
    expectedSet.add(flow1.getExecutionId());
    expectedSet.add(flow2.getExecutionId());
    assertThat(this.executionFlowDao.selectAndUpdateExecutionWithLocking(true, 2, Status.READY, DispatchMethod.CONTAINERIZED))
        .isEqualTo(expectedSet);
  }

  @Test
  public void testLockSuccessSelectAndUpdateExecutionWithLockingWithoutLimit() throws Exception {
    when(mysqlNamedLock.getLock(any(DatabaseTransOperator.class), any(String.class), any(Integer.class)))
        .thenReturn(true);
    when(mysqlNamedLock.releaseLock(any(DatabaseTransOperator.class), any(String.class))).thenReturn(true);
    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, Status.READY, DispatchMethod.CONTAINERIZED);
    assertThat(this.executionFlowDao.selectAndUpdateExecutionWithLocking(false,2, Status.READY,
        DispatchMethod.CONTAINERIZED).size())
        .isEqualTo(1);
    Set<Integer> expectedSet = new HashSet<>();
    expectedSet.add(flow1.getExecutionId());
    assertThat(this.executionFlowDao.selectAndUpdateExecutionWithLocking(false,2, Status.READY,
        DispatchMethod.CONTAINERIZED))
        .isEqualTo(expectedSet);
  }

  @Test
  public void testLockSuccessSelectAndUpdateExecutionWithLockingWithLimit() throws Exception {
    when(mysqlNamedLock.getLock(any(DatabaseTransOperator.class), any(String.class), any(Integer.class)))
        .thenReturn(true);
    when(mysqlNamedLock.releaseLock(any(DatabaseTransOperator.class), any(String.class))).thenReturn(true);
    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, Status.READY, DispatchMethod.CONTAINERIZED);
    final ExecutableFlow flow2 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, Status.READY, DispatchMethod.CONTAINERIZED);
    assertThat(this.executionFlowDao.selectAndUpdateExecutionWithLocking(true,2, Status.READY,
        DispatchMethod.CONTAINERIZED).size())
        .isEqualTo(2);
    Set<Integer> expectedSet = new HashSet<>();
    expectedSet.add(flow1.getExecutionId());
    expectedSet.add(flow2.getExecutionId());
    assertThat(this.executionFlowDao.selectAndUpdateExecutionWithLocking(true,2, Status.READY,
        DispatchMethod.CONTAINERIZED))
        .isEqualTo(expectedSet);
  }

  @Test
  public void testSelectAndUpdateExecutionWithPriority() throws Exception {
    // Selecting executions when DB is empty
    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected no execution selected")
        .isEqualTo(-1);

    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow lowPriorityFlow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, DispatchMethod.POLL);

    final ExecutableFlow highPriorityFlow = submitNewFlow("exectest1", "exec1", currentTime + 5,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 5, DispatchMethod.POLL);

    final ExecutableFlow lowPriorityFlow2 = submitNewFlow("exectest1", "exec1", currentTime + 10,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 3, DispatchMethod.POLL);

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected flow with highest priority")
        .isEqualTo(highPriorityFlow.getExecutionId());

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected second flow with highest priority")
        .isEqualTo(lowPriorityFlow2.getExecutionId());

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected flow with lowest priority")
        .isEqualTo(lowPriorityFlow1.getExecutionId());

    // Selecting executions when there are no more submitted flows left
    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected no execution selected")
        .isEqualTo(-1);
  }

  @Test
  public void testSelectAndUpdateExecutionWithSamePriority() throws Exception {
    // Selecting executions when DB is empty
    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected no execution selected")
        .isEqualTo(-1);

    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow submittedFlow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 3, DispatchMethod.POLL);

    final ExecutableFlow submittedFlow2 = submitNewFlow("exectest1", "exec1", currentTime + 5,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 3, DispatchMethod.POLL);

    final ExecutableFlow submittedFlow3 = submitNewFlow("exectest1", "exec1", currentTime + 10,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 3, DispatchMethod.POLL);

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected first flow submitted")
        .isEqualTo(submittedFlow1.getExecutionId());

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected second flow submitted")
        .isEqualTo(submittedFlow2.getExecutionId());

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected last flow submitted")
        .isEqualTo(submittedFlow3.getExecutionId());

    // Selecting executions when there are no more submitted flows left
    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true, DispatchMethod.POLL))
        .as("Expected no execution selected")
        .isEqualTo(-1);
  }

  @Test
  public void testFlowStatusWithFetchExecutableFlows() throws Exception {
    final ExecutableFlow flow = submitNewFlow("exectest1", "exec1",
        System.currentTimeMillis(), ExecutionOptions.DEFAULT_FLOW_PRIORITY, DispatchMethod.POLL);
    int execId = flow.getExecutionId();
    makeFlowStatusInconsistent(execId, Status.FAILED_FINISHING);
    final ExecutableFlow fetchedFlow = this.executionFlowDao.fetchExecutableFlow(execId);
    assertThat(fetchedFlow.getStatus()).isEqualTo(Status.FAILED_FINISHING);
  }

  @Test
  public void testFlowStatusWithFetchFlowHistory() throws Exception {
    final ExecutableFlow flow = submitNewFlow("exectest1", "exec1",
        System.currentTimeMillis(), ExecutionOptions.DEFAULT_FLOW_PRIORITY, DispatchMethod.POLL);
    int execId = flow.getExecutionId();
    makeFlowStatusInconsistent(execId, Status.FAILED_FINISHING);

    // fetch all executions
    final List<ExecutableFlow> flowList1 = this.executionFlowDao.fetchFlowHistory(0, 2);
    assertThat(flowList1).isNotEmpty();
    assertThat(flowList1.get(0).getStatus()).isEqualTo(Status.FAILED_FINISHING);
    assertThat(flowList1.get(0).getExecutionId()).isEqualTo(execId);

    // fetch executions of a flow
    final List<ExecutableFlow> flowList2 = this.executionFlowDao
        .fetchFlowHistory(flow.getProjectId(), flow.getId(), 0, 3);
    assertThat(flowList2).isNotEmpty();
    assertThat(flowList2.get(0).getStatus()).isEqualTo(Status.FAILED_FINISHING);
    assertThat(flowList2.get(0).getExecutionId()).isEqualTo(execId);

    // fetch flow executions by status
    final List<ExecutableFlow> flowList3 = this.executionFlowDao.fetchFlowHistory(
        flow.getProjectId(), flow.getId(), 0, 3, Status.FAILED_FINISHING);
    assertThat(flowList3).isNotEmpty();
    assertThat(flowList3.get(0).getExecutionId()).isEqualTo(execId);

    // fetch flow executions by multiple filters
    final List<ExecutableFlow> flowList4 = this.executionFlowDao
        .fetchFlowHistory("", "data", "", 0, -1, -1, 0, 16);
    assertThat(flowList4).isNotEmpty();
    assertThat(flowList4.get(0).getStatus()).isEqualTo(Status.FAILED_FINISHING);
    assertThat(flowList4.get(0).getExecutionId()).isEqualTo(execId);

    final ExecutableFlow fetchedFlow = this.executionFlowDao.fetchExecutableFlow(execId);
    assertTwoFlowSame(fetchedFlow, flowList1.get(0));
    assertTwoFlowSame(fetchedFlow, flowList2.get(0));
    assertTwoFlowSame(fetchedFlow, flowList3.get(0));
    assertTwoFlowSame(fetchedFlow, flowList4.get(0));

  }

  @Test
  public void fetchFlowStatusWithFlowHistoryByStartTime() throws Exception {
    final ExecutableFlow flow1 = createExecution(
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 10:00:00"), Status.PREPARING);
    final ExecutableFlow flow2 = createExecution(
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 09:00:00"), Status.PREPARING);
    final ExecutableFlow flow3 = createExecution(
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 09:00:00"), Status.PREPARING);
    final ExecutableFlow flow4 = createExecution(
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 08:00:00"), Status.QUEUED);

    makeFlowStatusInconsistent(flow1.getExecutionId(), Status.FAILED_FINISHING);

    final List<ExecutableFlow> flowList = this.executionFlowDao.fetchFlowHistory(
        flow1.getProjectId(), flow1.getFlowId(),
        TimeUtils.convertDateTimeToUTCMillis("2018-09-01 08:00:00"));

    assertThat(flowList).hasSize(4);
    assertThat(flowList.get(0).getExecutionId()).isEqualTo(flow1.getExecutionId());
    assertThat(flowList.get(0).getStatus()).isEqualTo(Status.FAILED_FINISHING);
    assertThat(flowList.get(3).getExecutionId()).isEqualTo(flow4.getExecutionId());
    assertThat(flowList.get(3).getStatus()).isEqualTo(Status.QUEUED);
  }

  @Test
  public void testFlowStatusWithFetchUnfinishedFlows() throws Exception {
    final ExecutableFlow flow1 = createExecutionAndAssign(Status.QUEUED,
        this.executorDao.addExecutor("test", 1));

    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows1 =
        this.fetchActiveFlowDao.fetchUnfinishedFlows();
    assertThat(unfinishedFlows1.containsKey(flow1.getExecutionId())).isTrue();
    assertThat(unfinishedFlows1.get(flow1.getExecutionId()).getSecond().getStatus())
        .isEqualTo(Status.QUEUED);

    makeFlowStatusInconsistent(flow1.getExecutionId(), Status.FAILED_FINISHING);

    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows2 =
        this.fetchActiveFlowDao.fetchUnfinishedFlows();
    assertThat(unfinishedFlows2.containsKey(flow1.getExecutionId())).isTrue();
    assertThat(unfinishedFlows2.get(flow1.getExecutionId()).getSecond().getStatus())
        .isEqualTo(Status.FAILED_FINISHING);
  }

  @Test
  public void testFlowStatusWithFetchActiveFlows() throws Exception {
    final ExecutableFlow flow1 = createExecutionAndAssign(Status.RUNNING,
        this.executorDao.addExecutor("test", 1));

    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows1 =
        this.fetchActiveFlowDao.fetchActiveFlows();
    assertThat(activeFlows1.containsKey(flow1.getExecutionId())).isTrue();
    assertThat(activeFlows1.get(flow1.getExecutionId()).getSecond().getStatus())
        .isEqualTo(Status.RUNNING);

    makeFlowStatusInconsistent(flow1.getExecutionId(), Status.KILLING);

    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows2 =
        this.fetchActiveFlowDao.fetchActiveFlows();
    assertThat(activeFlows2.containsKey(flow1.getExecutionId())).isTrue();
    assertThat(activeFlows2.get(flow1.getExecutionId()).getSecond().getStatus())
        .isEqualTo(Status.KILLING);
  }

  @Test
  public void testFlowStatusWithFetchActiveFlowByExecId() throws Exception {
    final ExecutableFlow flow1 = createExecutionAndAssign(Status.RUNNING,
        this.executorDao.addExecutor("test", 1));
    Pair<ExecutionReference, ExecutableFlow> activeFlow1 =
        this.fetchActiveFlowDao.fetchActiveFlowByExecId(flow1.getExecutionId());
    assertThat(activeFlow1).isNotNull();
    assertTwoFlowSame(activeFlow1.getSecond(), flow1);

    makeFlowStatusInconsistent(flow1.getExecutionId(), Status.KILLING);

    Pair<ExecutionReference, ExecutableFlow> activeFlow2 =
        this.fetchActiveFlowDao.fetchActiveFlowByExecId(flow1.getExecutionId());
    assertThat(activeFlow2).isNotNull();
    assertThat(activeFlow2.getSecond().getStatus()).isEqualTo(Status.KILLING);
  }

  @Test
  public void testFlowStatusWithFetchQueuedFlows() throws Exception {
    final ExecutableFlow flow1 = submitNewFlow("exectest1", "exec1", System.currentTimeMillis(),
        ExecutionOptions.DEFAULT_FLOW_PRIORITY, DispatchMethod.POLL);
    flow1.setStatus(Status.RUNNING);
    this.executionFlowDao.updateExecutableFlow(flow1);

    final List<Pair<ExecutionReference, ExecutableFlow>> fetchedQueuedFlows1 =
        this.executionFlowDao.fetchQueuedFlows(Status.PREPARING);
    assertThat(fetchedQueuedFlows1).isEmpty();

    makeFlowStatusInconsistent(flow1.getExecutionId(), Status.PREPARING);

    final List<Pair<ExecutionReference, ExecutableFlow>> fetchedQueuedFlows2 =
        this.executionFlowDao.fetchQueuedFlows(Status.PREPARING);
    assertThat(fetchedQueuedFlows2).isNotEmpty();
    assertThat(fetchedQueuedFlows2.get(0).getSecond().getStatus()).isEqualTo(Status.PREPARING);
  }

  @Test
  public void testFlowStatusWithFetchRecentlyFinishedFlows() throws Exception {
    final ExecutableFlow flow1 = createExecution(System.currentTimeMillis(), Status.SUCCEEDED);
    flow1.setEndTime(System.currentTimeMillis());
    this.executionFlowDao.updateExecutableFlow(flow1);

    final List<ExecutableFlow> finishedFlows1 = this.executionFlowDao.fetchRecentlyFinishedFlows(
        RECENTLY_FINISHED_LIFETIME);
    assertThat(finishedFlows1).isNotEmpty();
    assertTwoFlowSame(flow1, finishedFlows1.get(0));

    makeFlowStatusInconsistent(flow1.getExecutionId(), Status.FAILED);

    final List<ExecutableFlow> finishedFlows2 = this.executionFlowDao.fetchRecentlyFinishedFlows(
        RECENTLY_FINISHED_LIFETIME);
    assertThat(finishedFlows2).isNotEmpty();
    assertThat(finishedFlows2.get(0).getStatus()).isEqualTo(Status.FAILED);
  }

  /**
   * Test the resiliency of ExecutableFlow when Sla Option is set to NULL.
   * Make sure that the serialization of flow object does not break and the flow proceeds to
   * next valid state.
   */
  @Test
  public void testUpdateExecutableFlowNullSLAOptions() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDao.uploadExecutableFlow(flow);

    final ExecutableFlow fetchFlow =
        this.executionFlowDao.fetchExecutableFlow(flow.getExecutionId());

    // set null sla option
    fetchFlow.getExecutionOptions().setSlaOptions(null);
    // Try updating flow
    try {
      this.executionFlowDao.updateExecutableFlow(fetchFlow);
    } catch (ExecutorManagerException e) {
       assert e.getMessage().contains("NPE");
    }
    // Fetch flow again, the status must be READY not PREPARING as NPE is handled properly when
    //flow object is serialized.
    final ExecutableFlow readyFlow =
        this.executionFlowDao.fetchExecutableFlow(fetchFlow.getExecutionId());

    assertThat(readyFlow.getStatus()).isEqualTo(Status.READY);
  }

  /**
   * Test when an executable flow sets its version set field, the information can be retrieved
   * after updateExecutableFlow and fetchExecutableFlow
   * @throws Exception
   */
  @Test
  public void testUpdateExecutionFlowVersionSet() throws Exception {

    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDao.uploadExecutableFlow(flow);
    flow.setVersionSet(createVersionSet());
    this.executionFlowDao.updateExecutableFlow(flow);

    final ExecutableFlow fetchFlow =
        this.executionFlowDao.fetchExecutableFlow(flow.getExecutionId());

    Assert.assertTrue(fetchFlow.getVersionSet() != null);
    assertThat(flow.getVersionSet().getImageToVersionMap()).isEqualTo(fetchFlow.getVersionSet().getImageToVersionMap());
  }

  /*
   * Updates flow execution status in the DB. After this the value of the status column will be
   * different from the status property in the flow data blob.
   */
  private void makeFlowStatusInconsistent(int executionId, Status status) {
    final String MODIFY_FLOW_STATUS = "UPDATE execution_flows SET status=? WHERE exec_id=?";
    try {
      dbOperator.update(MODIFY_FLOW_STATUS, status.getNumVal(), executionId);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private ExecutableFlow submitNewFlow(final String projectName, final String flowName,
      final long submitTime, final int flowPriority, DispatchMethod dispatchMethod) throws IOException,
      ExecutorManagerException {
    return submitNewFlow(projectName, flowName, submitTime, flowPriority, Status.PREPARING,
        Optional.empty(), dispatchMethod);
  }

  private ExecutableFlow submitNewFlow(final String projectName, final String flowName,
      final long submitTime, final int flowPriority, final Status status, DispatchMethod dispatchMethod) throws IOException,
      ExecutorManagerException {
    return submitNewFlow(projectName, flowName, submitTime, flowPriority, status,
        Optional.empty(), dispatchMethod);
  }

  private ExecutableFlow submitNewFlow(final String projectName, final String flowName,
      final long submitTime, final int flowPriority, final Status status,
      Optional<Long> startTime, DispatchMethod dispatchMethod) throws IOException,
      ExecutorManagerException {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow(projectName, flowName, DispatchMethod.POLL);
    flow.setStatus(status);
    flow.setSubmitTime(submitTime);
    flow.setSubmitUser("testUser");
    flow.setDispatchMethod(dispatchMethod);
    flow.getExecutionOptions().getFlowParameters().put(ExecutionOptions.FLOW_PRIORITY,
        String.valueOf(flowPriority));
    startTime.ifPresent(st -> flow.setStartTime(st));
    this.executionFlowDao.uploadExecutableFlow(flow);
    return flow;
  }

  private void assertTwoFlowSame(final ExecutableFlow flow1, final ExecutableFlow flow2) {
    assertTwoFlowSame(flow1, flow2, true);
  }

  private void assertTwoFlowSame(final ExecutableFlow flow1, final ExecutableFlow flow2,
      final boolean compareFlowData) {
    assertThat(flow1.getExecutionId()).isEqualTo(flow2.getExecutionId());
    assertThat(flow1.getStatus()).isEqualTo(flow2.getStatus());
    assertThat(flow1.getEndTime()).isEqualTo(flow2.getEndTime());
    assertThat(flow1.getStartTime()).isEqualTo(flow2.getStartTime());
    assertThat(flow1.getSubmitTime()).isEqualTo(flow2.getSubmitTime());
    assertThat(flow1.getFlowId()).isEqualTo(flow2.getFlowId());
    assertThat(flow1.getProjectId()).isEqualTo(flow2.getProjectId());
    assertThat(flow1.getVersion()).isEqualTo(flow2.getVersion());
    assertThat(flow1.getSubmitUser()).isEqualTo(flow2.getSubmitUser());
    if (compareFlowData) {
      assertThat(flow1.getExecutionOptions().getFailureAction())
          .isEqualTo(flow2.getExecutionOptions().getFailureAction());
      assertThat(new HashSet<>(flow1.getEndNodes())).isEqualTo(new HashSet<>(flow2.getEndNodes()));
    }
  }

  /**
   * Create a version set from scratch
   * @return a new version set
   */
  private VersionSet createVersionSet(){
    final String testJsonString1 = "{\"azkaban-base\":{\"version\":\"7.0.4\",\"path\":\"path1\","
        + "\"state\":\"ACTIVE\"},\"azkaban-config\":{\"version\":\"9.1.1\",\"path\":\"path2\","
        + "\"state\":\"ACTIVE\"},\"spark\":{\"version\":\"8.0\",\"path\":\"path3\","
        + "\"state\":\"ACTIVE\"}}";
    final String testMd5Hex1 = "43966138aebfdc4438520cc5cd2aefa8";
    return new VersionSet(testJsonString1, testMd5Hex1, 1);
  }

}
