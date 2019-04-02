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

import azkaban.db.DatabaseOperator;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.ProjectLoader;
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.AfterClass;
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
    this.executionFlowDao = new ExecutionFlowDao(dbOperator);
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
    return TestUtils.createTestExecutableFlow("exectest1", "exec1");
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
    class DateUtil {

      private long dateStrToLong(final String dateStr) throws ParseException {
        final SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final Date d = f.parse(dateStr);
        final long milliseconds = d.getTime();
        return milliseconds;
      }
    }

    final DateUtil dateUtil = new DateUtil();
    final ExecutableFlow flow1 = createTestFlow();
    flow1.setStartTime(dateUtil.dateStrToLong("2018-09-01 10:00:00"));
    this.executionFlowDao.uploadExecutableFlow(flow1);

    final ExecutableFlow flow2 = createTestFlow();
    flow2.setStartTime(dateUtil.dateStrToLong("2018-09-01 09:00:00"));
    this.executionFlowDao.uploadExecutableFlow(flow2);

    final ExecutableFlow flow3 = createTestFlow();
    flow3.setStartTime(dateUtil.dateStrToLong("2018-09-01 09:00:00"));
    this.executionFlowDao.uploadExecutableFlow(flow3);

    final ExecutableFlow flow4 = createTestFlow();
    flow4.setStartTime(dateUtil.dateStrToLong("2018-09-01 08:00:00"));
    this.executionFlowDao.uploadExecutableFlow(flow4);

    final List<ExecutableFlow> flowList = this.executionFlowDao.fetchFlowHistory
        (flow1.getProjectId(), flow1.getFlowId(), dateUtil.dateStrToLong("2018-09-01 09:00:00"));
    final List<ExecutableFlow> expected = new ArrayList<>();
    expected.add(flow1);
    expected.add(flow2);
    expected.add(flow3);

    assertThat(flowList).hasSize(3);
    for (int i = 0; i < flowList.size(); i++) {
      assertTwoFlowSame(flowList.get(i), expected.get(i));
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
    final ExecutableFlow flow1 = createTestFlow();
    this.executionFlowDao.uploadExecutableFlow(flow1);
    flow1.setStatus(Status.SUCCEEDED);
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
    final ExecutableFlow flow1 = createTestFlow();
    this.executionFlowDao.uploadExecutableFlow(flow1);
    flow1.setStatus(Status.SUCCEEDED);
    flow1.setEndTime(DateTimeUtils.currentTimeMillis());
    this.executionFlowDao.updateExecutableFlow(flow1);
    //Todo jamiesjc: use java8.java.time api instead of jodatime

    //Mock flow finished time to be 2 min ago.
    DateTimeUtils.setCurrentMillisOffset(-FLOW_FINISHED_TIME.toMillis());
    flow1.setEndTime(DateTimeUtils.currentTimeMillis());
    this.executionFlowDao.updateExecutableFlow(flow1);

    //Fetch recently finished flows within 1 min. Should be empty.
    final List<ExecutableFlow> flows = this.executionFlowDao
        .fetchRecentlyFinishedFlows(RECENTLY_FINISHED_LIFETIME);
    assertThat(flows.size()).isEqualTo(0);
  }

  @Test
  public void testFetchQueuedFlows() throws Exception {

    final ExecutableFlow flow = createTestFlow();
    flow.setStatus(Status.PREPARING);
    this.executionFlowDao.uploadExecutableFlow(flow);
    final ExecutableFlow flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    flow2.setStatus(Status.PREPARING);
    this.executionFlowDao.uploadExecutableFlow(flow2);

    final List<Pair<ExecutionReference, ExecutableFlow>> fetchedQueuedFlows = this.executionFlowDao
        .fetchQueuedFlows();
    assertThat(fetchedQueuedFlows.size()).isEqualTo(2);
    final Pair<ExecutionReference, ExecutableFlow> fetchedFlow1 = fetchedQueuedFlows.get(0);
    final Pair<ExecutionReference, ExecutableFlow> fetchedFlow2 = fetchedQueuedFlows.get(1);

    assertTwoFlowSame(flow, fetchedFlow1.getSecond());
    assertTwoFlowSame(flow2, fetchedFlow2.getSecond());
  }

  @Test
  public void testAssignAndUnassignExecutor() throws Exception {
    final String host = "localhost";
    final int port = 12345;
    final Executor executor = this.executorDao.addExecutor(host, port);
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1");
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
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1");
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
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = this.fetchActiveFlowDao
        .fetchActiveFlows();
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

    final ExecutableFlow flow1 = createExecutionAndAssign(Status.PREPARING, executor);

    // flow2 is not assigned
    final ExecutableFlow flow2 = createExecution(Status.PREPARING);

    final ExecutableFlow flow3 = createExecutionAndAssign(Status.RUNNING, executor);
    flow3.setStartTime(System.currentTimeMillis() + 1);
    this.executionFlowDao.updateExecutableFlow(flow3);

    final ExecutableFlow flow4 = createExecutionAndAssign(Status.SUCCEEDED, executor);
    flow4.setStartTime(System.currentTimeMillis() - 2);
    flow4.setEndTime(System.currentTimeMillis() - 1);
    this.executionFlowDao.updateExecutableFlow(flow4);

    final Executor executor2 = this.executorDao.addExecutor("test2", 2);
    // flow5 is assigned to an executor that is then removed
    final ExecutableFlow flow5 = createExecutionAndAssign(Status.RUNNING, executor2);
    flow5.setStartTime(System.currentTimeMillis() + 1);
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
    final ExecutableFlow flow = createExecution(status);
    this.assignExecutor.assignExecutor(executor.getId(), flow.getExecutionId());
    return flow;
  }

  private ExecutableFlow createExecution(final Status status)
      throws IOException, ExecutorManagerException {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    flow.setSubmitTime(System.currentTimeMillis());
    this.executionFlowDao.uploadExecutableFlow(flow);
    flow.setStatus(status);
    this.executionFlowDao.updateExecutableFlow(flow);
    return flow;
  }

  @Test
  public void testFetchActiveFlowsStatusChanged() throws Exception {
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
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

    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1");
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
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    flow.setStatus(Status.PREPARING);
    flow.setSubmitTime(System.currentTimeMillis());
    this.executionFlowDao.uploadExecutableFlow(flow);
    final Executor executor = this.executorDao.addExecutor("localhost", 12345);
    assertThat(this.executionFlowDao.selectAndUpdateExecution(executor.getId(), true))
        .isEqualTo(flow.getExecutionId());
    assertThat(this.executorDao.fetchExecutorByExecutionId(flow.getExecutionId())).isEqualTo
        (executor);
  }

  @Test
  public void testSelectAndUpdateExecutionWithPriority() throws Exception {
    // Selecting executions when DB is empty
    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected no execution selected")
        .isEqualTo(-1);

    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow lowPriorityFlow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY);

    final ExecutableFlow highPriorityFlow = submitNewFlow("exectest1", "exec1", currentTime + 5,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 5);

    final ExecutableFlow lowPriorityFlow2 = submitNewFlow("exectest1", "exec1", currentTime + 10,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 3);

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected flow with highest priority")
        .isEqualTo(highPriorityFlow.getExecutionId());

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected second flow with highest priority")
        .isEqualTo(lowPriorityFlow2.getExecutionId());

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected flow with lowest priority")
        .isEqualTo(lowPriorityFlow1.getExecutionId());

    // Selecting executions when there are no more submitted flows left
    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected no execution selected")
        .isEqualTo(-1);
  }

  @Test
  public void testSelectAndUpdateExecutionWithSamePriority() throws Exception {
    // Selecting executions when DB is empty
    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected no execution selected")
        .isEqualTo(-1);

    final long currentTime = System.currentTimeMillis();
    final ExecutableFlow submittedFlow1 = submitNewFlow("exectest1", "exec1", currentTime,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 3);

    final ExecutableFlow submittedFlow2 = submitNewFlow("exectest1", "exec1", currentTime + 5,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 3);

    final ExecutableFlow submittedFlow3 = submitNewFlow("exectest1", "exec1", currentTime + 10,
        ExecutionOptions.DEFAULT_FLOW_PRIORITY + 3);

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected first flow submitted")
        .isEqualTo(submittedFlow1.getExecutionId());

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected second flow submitted")
        .isEqualTo(submittedFlow2.getExecutionId());

    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected last flow submitted")
        .isEqualTo(submittedFlow3.getExecutionId());

    // Selecting executions when there are no more submitted flows left
    assertThat(this.executionFlowDao.selectAndUpdateExecution(-1, true))
        .as("Expected no execution selected")
        .isEqualTo(-1);
  }

  private ExecutableFlow submitNewFlow(final String projectName, final String flowName,
      final long submitTime, final int flowPriority) throws IOException, ExecutorManagerException {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow(projectName, flowName);
    flow.setStatus(Status.PREPARING);
    flow.setSubmitTime(submitTime);
    flow.getExecutionOptions().getFlowParameters().put(ExecutionOptions.FLOW_PRIORITY,
        String.valueOf(flowPriority));
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
   * restores the clock; see {@link #testFetchEmptyRecentlyFinishedFlows()}
   */
  @After
  public void clockReset() {
    DateTimeUtils.setCurrentMillisOffset(0);
  }

}
