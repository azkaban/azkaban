package azkaban.executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import azkaban.DispatchMethod;
import azkaban.alert.Alerter;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.Pair;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RunningExecutionsUpdaterTest {

  private static final int EXECUTION_ID_77 = 77;
  private static final ExecutorManagerException API_CALL_EXCEPTION =
      new ExecutorManagerException("Mocked API timeout");

  @Mock
  ExecutorManagerUpdaterStage updaterStage;
  @Mock
  AlerterHolder alerterHolder;
  @Mock
  CommonMetrics commonMetrics;
  @Mock
  ExecutorApiGateway apiGateway;
  @Mock
  ExecutionFinalizer executionFinalizer;
  @Mock
  private Alerter mailAlerter;
  @Mock
  private ExecutorLoader executorLoader;

  private ExecutableFlow execution;
  private RunningExecutions runningExecutions;
  private Executor activeExecutor;

  private RunningExecutionsUpdater updater;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    this.execution = new ExecutableFlow();
    this.execution.setExecutionId(EXECUTION_ID_77);
    this.activeExecutor = new Executor(1, "activeExecutor-1", 9999, true);
    this.runningExecutions = new RunningExecutions();
    this.runningExecutions.get().put(EXECUTION_ID_77, new Pair<>(
        new ExecutionReference(EXECUTION_ID_77, this.activeExecutor, DispatchMethod.PUSH), this.execution));
    this.updater = new RunningExecutionsUpdater(this.updaterStage, this.alerterHolder,
        this.commonMetrics, this.apiGateway, this.runningExecutions, this.executionFinalizer,
        this.executorLoader);
    when(this.alerterHolder.get("email")).thenReturn(this.mailAlerter);
  }

  @After
  public void tearDown() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void updateExecutionsStillRunning() throws Exception {
    mockFlowStillRunning();
    this.updater.updateExecutions();
    verifyCallUpdateApi();
    verifyZeroInteractions(this.executionFinalizer);
  }

  @Test
  public void updateExecutionsSucceeded() throws Exception {
    mockFlowSucceeded();
    this.updater.updateExecutions();
    verifyCallUpdateApi();
    verifyFinalizeFlow();
  }

  @Test
  public void updateExecutionsExecutorDoesNotExist() throws Exception {
    mockExecutorDoesNotExist();
    this.updater.updateExecutions();
    verifyFinalizeFlow();
  }

  @Test
  public void updateExecutionsFlowDoesNotExist() throws Exception {
    mockFlowDoesNotExist();
    this.updater.updateExecutions();
    verifyCallUpdateApi();
    verifyFinalizeFlow();
  }

  @Test
  public void updateExecutionsUpdateCallFails() throws Exception {
    mockUpdateCallFails();
    when(this.executorLoader.fetchExecutor(anyInt())).thenReturn(this.activeExecutor);
    DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
    for (int i = 0; i < this.updater.numErrorsBeforeUnresponsiveEmail; i++) {
      this.updater.updateExecutions();
      DateTimeUtils.setCurrentMillisFixed(
          DateTimeUtils.currentTimeMillis() + this.updater.errorThreshold + 1L);
    }
    verify(this.mailAlerter).alertOnFailedUpdate(
        this.activeExecutor, Collections.singletonList(this.execution), API_CALL_EXCEPTION);
    verifyZeroInteractions(this.executionFinalizer);
  }

  /**
   * Should finalize execution if executor doesn't exist in the DB.
   */
  @Test
  public void updateExecutionsUpdateCallFailsExecutorDoesntExist() throws Exception {
    mockUpdateCallFails();
    when(this.executorLoader.fetchExecutor(anyInt())).thenReturn(null);
    DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
    this.updater.updateExecutions();
    verify(this.executionFinalizer).finalizeFlow(
        this.execution, "Not running on the assigned executor (any more)", null);
  }

  /**
   * Shouldn't finalize executions if executor's existence can't be checked.
   */
  @Test
  public void updateExecutionsUpdateCallFailsExecutorCheckThrows() throws Exception {
    mockUpdateCallFails();
    when(this.executorLoader.fetchExecutor(anyInt()))
        .thenThrow(new ExecutorManagerException("Mocked fetchExecutor failure"));
    DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
    this.updater.updateExecutions();
    verifyZeroInteractions(this.executionFinalizer);
  }

  private void mockFlowStillRunning() throws Exception {
    mockUpdateResponse();
  }

  private void mockFlowSucceeded() throws Exception {
    final Map<String, Object> executionMap = mockUpdateResponse();
    executionMap.put(ExecutableNode.STATUS_PARAM, Status.SUCCEEDED.getNumVal());
  }

  private void mockExecutorDoesNotExist() {
    this.runningExecutions.get().put(EXECUTION_ID_77, new Pair<>(
        new ExecutionReference(EXECUTION_ID_77, null, DispatchMethod.PUSH), this.execution));
  }

  private void mockUpdateCallFails() throws ExecutorManagerException {
    doThrow(API_CALL_EXCEPTION).when(this.apiGateway).updateExecutions(any(), any());
  }

  private void verifyCallUpdateApi() throws ExecutorManagerException {
    verify(this.apiGateway).updateExecutions(
        this.activeExecutor, Collections.singletonList(this.execution));
  }

  private void mockFlowDoesNotExist() throws Exception {
    final Map<String, Object> executionMap = mockUpdateResponse();
    executionMap.put(ConnectorParams.RESPONSE_ERROR, "Flow does not exist");
  }

  private Map<String, Object> mockUpdateResponse() throws Exception {
    final Map<String, Object> executionMap = new HashMap<>(ImmutableMap.of(
        ConnectorParams.UPDATE_MAP_EXEC_ID, EXECUTION_ID_77));
    mockUpdateResponse(ImmutableMap.of(
        ConnectorParams.RESPONSE_UPDATED_FLOWS, Collections.singletonList(executionMap)));
    return executionMap;
  }

  // Suppress "unchecked generic array creation for varargs parameter".
  // No way to avoid this when mocking a method with generic varags.
  @SuppressWarnings("unchecked")
  private void mockUpdateResponse(
      final Map<String, List<Map<String, Object>>> map) throws Exception {
    doReturn(map).when(this.apiGateway).updateExecutions(any(), any());
  }

  private void verifyFinalizeFlow() {
    verify(this.executionFinalizer).finalizeFlow(this.execution,
        "Not running on the assigned executor (any more)", null);
  }

}
