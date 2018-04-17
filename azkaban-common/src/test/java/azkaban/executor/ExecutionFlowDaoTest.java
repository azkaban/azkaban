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
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import java.io.File;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ExecutionFlowDaoTest {

  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(1);
  private static final Duration FLOW_FINISHED_TIME = Duration.ofMinutes(2);

  private static DatabaseOperator dbOperator;
  private ExecutionFlowDao executionFlowDao;
  private ExecutorDao executorDao;
  private AssignExecutorDao assignExecutor;
  private FetchActiveFlowDao fetchActiveFlowDao;
  private ExecutionJobDao executionJobDao;

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
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("DELETE FROM execution_flows");
      dbOperator.update("DELETE FROM executors");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  private ExecutableFlow createTestFlow() throws Exception {
    return TestUtils.createTestExecutableFlow("exectest1", "exec1");
  }

  @Test
  public void testUploadAndFetchExecutionFlows() throws Exception {

    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDao.uploadExecutableFlow(flow);

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

    //Restore the clock
    DateTimeUtils.setCurrentMillisOffset(0);
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

    // Upload flow1, executor assigned
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    this.executionFlowDao.uploadExecutableFlow(flow1);
    final Executor executor = this.executorDao.addExecutor("test", 1);
    this.assignExecutor.assignExecutor(executor.getId(), flow1.getExecutionId());

    // Upload flow2, executor not assigned
    final ExecutableFlow flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.executionFlowDao.uploadExecutableFlow(flow2);

    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows1 =
        this.fetchActiveFlowDao.fetchActiveFlows();

    assertThat(activeFlows1.containsKey(flow1.getExecutionId())).isTrue();
    assertThat(activeFlows1.containsKey(flow2.getExecutionId())).isFalse();
    final ExecutableFlow flow1Result =
        activeFlows1.get(flow1.getExecutionId()).getSecond();
    assertTwoFlowSame(flow1Result, flow1);
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
  @Ignore
  // TODO jamiesjc: Active_execution_flow table is already deprecated. we should remove related
  // test methods as well.
  public void testFetchActiveFlowsReferenceChanged() throws Exception {
  }

  @Test
  @Ignore
  // TODO jamiesjc: Active_execution_flow table is already deprecated. we should remove related
  // test methods as well.
  public void testFetchActiveFlowByExecId() throws Exception {
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

  private void assertTwoFlowSame(final ExecutableFlow flow1, final ExecutableFlow flow2) {
    assertThat(flow1.getExecutionId()).isEqualTo(flow2.getExecutionId());
    assertThat(flow1.getStatus()).isEqualTo(flow2.getStatus());
    assertThat(flow1.getEndTime()).isEqualTo(flow2.getEndTime());
    assertThat(flow1.getStartTime()).isEqualTo(flow2.getStartTime());
    assertThat(flow1.getSubmitTime()).isEqualTo(flow2.getStartTime());
    assertThat(flow1.getFlowId()).isEqualTo(flow2.getFlowId());
    assertThat(flow1.getProjectId()).isEqualTo(flow2.getProjectId());
    assertThat(flow1.getVersion()).isEqualTo(flow2.getVersion());
    assertThat(flow1.getExecutionOptions().getFailureAction())
        .isEqualTo(flow2.getExecutionOptions().getFailureAction());
    assertThat(new HashSet<>(flow1.getEndNodes())).isEqualTo(new HashSet<>(flow2.getEndNodes()));
  }

}
