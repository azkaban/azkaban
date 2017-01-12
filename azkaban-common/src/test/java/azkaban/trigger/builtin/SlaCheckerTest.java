package azkaban.trigger.builtin;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.sla.SlaOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static azkaban.sla.SlaOption.TYPE_FLOW_FINISH;
import static azkaban.sla.SlaOption.TYPE_FLOW_SUCCEED;
import static azkaban.sla.SlaOption.TYPE_JOB_SUCCEED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SlaCheckerTest {
  private final int RUNNING_FLOW = 1;
  private final int SUCCEEDED_FLOW = 2;
  private final int KILLED_FLOW = 3;
  private final int FAILED_FLOW = 4;

  private ExecutorManagerAdapter executorManager;

  @Before
  public void setUp() throws Exception {
    executorManager = mock(ExecutorManagerAdapter.class);
    SlaChecker.setExecutorManager(executorManager);

    addMockFlow(RUNNING_FLOW, Status.RUNNING);
    addMockFlow(SUCCEEDED_FLOW, Status.SUCCEEDED);
    addMockFlow(KILLED_FLOW, Status.KILLED);
    addMockFlow(FAILED_FLOW, Status.FAILED);
  }

  private ExecutableFlow addMockFlow(int execId, Status status) throws ExecutorManagerException {
    ExecutableFlow flow = mock(ExecutableFlow.class);
    when(flow.getStartTime()).thenReturn(DateTime.now().getMillis());
    when(flow.getStatus()).thenReturn(status);

    when(executorManager.getExecutableFlow(execId)).thenReturn(flow);

    return flow;
  }

  private static SlaOption createSlaOption(String type, String duration) {
    Map<String, Object> info = new HashMap<>();
    info.put(SlaOption.INFO_DURATION, duration);
    return new SlaOption(type, Collections.emptyList(), info);
  }

  private static SlaChecker createSlaChecker(String type, String duration, int execId) {
    SlaOption slaOption = createSlaOption(type, duration);
    return new SlaChecker("MockFlowSlaChecker", slaOption, execId);
  }

  @Test
  public void testFlowRunningExpired() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_FINISH, "0s", RUNNING_FLOW);

    assertFalse((Boolean) slaChecker.isSlaPassed());
    assertTrue((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowRunningNotExpired() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_FINISH, "1d", RUNNING_FLOW);

    assertFalse((Boolean) slaChecker.isSlaPassed());
    assertFalse((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowRunning() throws Exception {
    final int execId = 100;
    ExecutableFlow flow = addMockFlow(execId, Status.RUNNING);
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_FINISH, "2s", execId);

    // Flow hasn't reached terminal state
    assertFalse((Boolean) slaChecker.isSlaPassed());
    // But SLA is still valid. returns false => trigger is not activated.
    assertFalse((Boolean) slaChecker.isSlaFailed());

    Thread.sleep(2000);

    // Flow still hasn't reached terminal state
    assertFalse((Boolean) slaChecker.isSlaPassed());
    // SLA has been missed
    assertTrue((Boolean) slaChecker.isSlaFailed());

    when(flow.getStatus()).thenReturn(Status.SUCCEEDED);

    // Terminal state reached
    assertTrue((Boolean) slaChecker.isSlaPassed());
    // SLA has not been violated
    assertFalse((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowSucceeded1() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_SUCCEED, "0s", SUCCEEDED_FLOW);

    assertTrue((Boolean) slaChecker.isSlaPassed());
    assertFalse((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowSucceeded2() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_SUCCEED, "1d", SUCCEEDED_FLOW);

    assertTrue((Boolean) slaChecker.isSlaPassed());
    assertFalse((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowKilledBeforeSla() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_SUCCEED, "1d", KILLED_FLOW);

    assertFalse((Boolean) slaChecker.isSlaPassed());
    assertTrue((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowFailedBeforeSla() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_SUCCEED, "1d", FAILED_FLOW);

    assertFalse((Boolean) slaChecker.isSlaPassed());
    assertTrue((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowFailedAfterSla() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_SUCCEED, "0s", FAILED_FLOW);

    assertFalse((Boolean) slaChecker.isSlaPassed());
    assertTrue((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowKilledAfterSla() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_SUCCEED, "0s", KILLED_FLOW);

    assertFalse((Boolean) slaChecker.isSlaPassed());
    assertTrue((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowKilledBeforeSlaFinish() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_FINISH, "1d", KILLED_FLOW);

    assertTrue((Boolean) slaChecker.isSlaPassed());
    assertFalse((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testFlowFailedAfterSlaFinish() throws Exception {
    SlaChecker slaChecker = createSlaChecker(TYPE_FLOW_FINISH, "0s", FAILED_FLOW);

    assertTrue((Boolean) slaChecker.isSlaPassed());
    assertFalse((Boolean) slaChecker.isSlaFailed());
  }

  @Test
  public void testJobSucceedSla() throws Exception {
    final int execId = 100;
    final String mockJobName = "mockJob";

    Map<String, Object> info = new HashMap<>();
    info.put(SlaOption.INFO_DURATION, "2s");
    info.put(SlaOption.INFO_JOB_NAME, mockJobName);
    SlaOption slaOption = new SlaOption(TYPE_JOB_SUCCEED, Collections.emptyList(), info);

    ExecutableNode mockJob = mock(ExecutableNode.class);
    when(mockJob.getStartTime()).thenReturn(DateTime.now().getMillis());
    when(mockJob.getStatus()).thenReturn(Status.RUNNING);

    ExecutableFlow flow = addMockFlow(execId, Status.RUNNING);
    when(flow.getExecutableNode(mockJobName)).thenReturn(mockJob);

    SlaChecker slaChecker = new SlaChecker("MockJobSlaChecker", slaOption, execId);

    // Flow hasn't reached terminal state
    assertFalse((Boolean) slaChecker.isSlaPassed());
    // But SLA is still valid. returns false => trigger is not activated.
    assertFalse((Boolean) slaChecker.isSlaFailed());

    Thread.sleep(2000);

    // Flow still hasn't reached terminal state
    assertFalse((Boolean) slaChecker.isSlaPassed());
    // SLA has been missed
    assertTrue((Boolean) slaChecker.isSlaFailed());

    when(mockJob.getStatus()).thenReturn(Status.SUCCEEDED);
    // Terminal state reached
    assertTrue((Boolean) slaChecker.isSlaPassed());
    // SLA has not been violated
    assertFalse((Boolean) slaChecker.isSlaFailed());

    when(mockJob.getStatus()).thenReturn(Status.KILLED);
    // Terminal state reached
    assertFalse((Boolean) slaChecker.isSlaPassed());
    // SLA has not been violated
    assertTrue((Boolean) slaChecker.isSlaFailed());
  }

}