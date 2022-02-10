/*
 * Copyright 2019 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.alert.Alerter;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for executor health checker.
 */
public class ExecutorHealthCheckerTest {

  private static final int EXECUTION_ID_11 = 11;
  private static final int EXECUTION_ID_12 = 12;
  private static final String AZ_ADMIN_ALERT_EMAIL = "az_admin1@foo.com,az_admin2@foo.com";
  private static final String FLOW_ADMIN_EMAIL = "flow_user1@foo.com,flow_user2@bar.com";
  private final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new HashMap<>();
  private ExecutorHealthChecker executorHealthChecker;
  private Props props;
  private ExecutorLoader loader;
  private ExecutorApiGateway apiGateway;
  private Alerter mailAlerter;
  private AlerterHolder alerterHolder;
  private ExecutableFlow flow1;
  private ExecutableFlow flow2;
  private Executor executor1;
  private Executor executor2;

  @Before
  public void setUp() throws Exception {
    this.props = new Props();
    this.props.put(ConfigurationKeys.AZKABAN_EXECUTOR_MAX_FAILURE_COUNT, 2);
    this.props.put(ConfigurationKeys.AZKABAN_ADMIN_ALERT_EMAIL, AZ_ADMIN_ALERT_EMAIL);
    this.loader = mock(ExecutorLoader.class);
    this.mailAlerter = mock(Alerter.class);
    this.alerterHolder = mock(AlerterHolder.class);
    this.apiGateway = mock(ExecutorApiGateway.class);
    this.executorHealthChecker = new ExecutorHealthChecker(this.props, this.loader, this
        .apiGateway, this.alerterHolder);
    this.flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.POLL);
    this.flow1.getExecutionOptions().setFailureEmails(Arrays.asList(FLOW_ADMIN_EMAIL.split(",")));
    this.flow1.setExecutionId(EXECUTION_ID_11);
    this.flow1.setStatus(Status.RUNNING);
    this.flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2", DispatchMethod.POLL);
    this.flow2.setExecutionId(EXECUTION_ID_12);
    this.flow2.setStatus(Status.RUNNING);

    this.executor1 = new Executor(1, "localhost", 12345, true);
    this.executor2 = new Executor(2, "localhost", 5678, true);
    when(this.loader.fetchActiveFlows(any())).thenReturn(this.activeFlows);
    when(this.alerterHolder.get("email")).thenReturn(this.mailAlerter);
  }

  /**
   * Test running flow is not finalized and alert email is not sent when executor is alive.
   */
  @Test
  public void checkExecutorHealthAlive() throws Exception {
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, this.executor1, DispatchMethod.POLL), this.flow1));
    when(this.apiGateway.callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null, null)).thenReturn(ImmutableMap.of(ConnectorParams
        .STATUS_PARAM, ConnectorParams.RESPONSE_ALIVE));
    this.executorHealthChecker.checkExecutorHealth();
    assertThat(this.flow1.getStatus()).isEqualTo(Status.RUNNING);
    verifyZeroInteractions(this.alerterHolder);
  }

  /**
   * Test running flow is finalized when its executor is removed from DB.
   */
  @Test
  public void checkExecutorHealthExecutorIdRemoved() throws Exception {
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, null, DispatchMethod.POLL), this.flow1));
    when(this.loader.fetchExecutableFlow(EXECUTION_ID_11)).thenReturn(this.flow1);
    this.executorHealthChecker.checkExecutorHealth();
    verify(this.loader).updateExecutableFlow(this.flow1);
    assertThat(this.flow1.getStatus()).isEqualTo(Status.FAILED);
  }

  /**
   * Test alert emails are sent when there are consecutive failures to contact the executor.
   */
  @Test
  public void checkExecutorHealthConsecutiveFailures() throws Exception {
    // By default mocked methods will return an empty collection.
    // Therefore underlying call to apiGateway.callWithExecutionId returns an empty Map for all
    // invocations of executorHealthChecker.checkExecutorHealth() in this test.
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, this.executor1, DispatchMethod.POLL), this.flow1));
    // Failed to ping executor. Failure count (=1) < MAX_FAILURE_COUNT (=2). Do not alert.
    this.executorHealthChecker.checkExecutorHealth();
    verify(this.apiGateway).callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null, null);
    verifyZeroInteractions(this.alerterHolder);

    // Pinged executor successfully. Failure count (=0) < MAX_FAILURE_COUNT (=2). Do not alert.
    when(this.apiGateway.callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null, null)).thenReturn(ImmutableMap.of(ConnectorParams
        .STATUS_PARAM, ConnectorParams.RESPONSE_ALIVE));
    this.executorHealthChecker.checkExecutorHealth();
    verifyZeroInteractions(this.alerterHolder);

    // Failed to ping executor. Failure count (=1) < MAX_FAILURE_COUNT (=2). Do not alert.
    when(this.apiGateway.callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null, null)).thenReturn(null);
    this.executorHealthChecker.checkExecutorHealth();
    verifyZeroInteractions(this.alerterHolder);

    // Failed to ping executor again. Failure count (=2) = MAX_FAILURE_COUNT (=2). Alert AZ admin.
    when(this.loader.fetchExecutableFlow(flow1.getExecutionId())).thenReturn(flow1);
    this.executorHealthChecker.checkExecutorHealth();
    verify((this.alerterHolder).get("email"))
        .alertOnFailedExecutorHealthCheck(eq(this.executor1), eq(Arrays.asList(this.flow1)),
            any(ExecutorManagerException.class),
            eq(Arrays.asList(AZ_ADMIN_ALERT_EMAIL.split(","))));

    // Verify remediation tasks are performed for unreachable executors.
    // Flow should be finalized with alerts sent over email.
    assertThat(this.flow1.getStatus()).isEqualTo(Status.FAILED);
    String expectedReason = "Executor was unreachable, executor-id: 1, executor-host: localhost, "
        + "executor-port: 12345";
    verify((this.alerterHolder).get("email")).alertOnError(eq(flow1), eq(expectedReason));
  }

  /**
   * Test that the wrapper routine swallows any exceptions reported by underlying health checker.
   */
  @Test
  public void testCheckExecutorHealthWrapperExceptionHandling() throws Exception {
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, this.executor1, DispatchMethod.POLL), this.flow1));
    when(this.apiGateway.callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null, null)).thenThrow(new RuntimeException("test exception"));

    // this will throw, causing the test to fail in case the error is not caught correctly
    this.executorHealthChecker.checkExecutorHealthQuietly();
    verifyZeroInteractions(this.alerterHolder);
  }

  /**
   * Test that runtime exceptions from the Ping API for one executor don't prevent healthchecks on
   * other executors.
   */
  @Test
  public void testFailureDuringExecutorPing() throws Exception {
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, this.executor1, DispatchMethod.POLL), this.flow1));
    this.activeFlows.put(EXECUTION_ID_12, new Pair<>(
        new ExecutionReference(EXECUTION_ID_12, this.executor2, DispatchMethod.POLL), this.flow2));

    // Throw a runtime exception for both executors.
    when(this.apiGateway.callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null, null)).thenThrow(new RuntimeException("test exception"));
    when(this.apiGateway.callWithExecutionId(this.executor2.getHost(), this.executor2.getPort(),
        ConnectorParams.PING_ACTION, null, null, null)).thenThrow(new RuntimeException("test exception"));
    this.executorHealthChecker.checkExecutorHealth();

    // Verify ping API is called for both executors. Implying that runtime exception for one of the
    // executors did not prevent the check on other executor.
    verify(this.apiGateway).callWithExecutionId(this.executor1.getHost(),
        this.executor1.getPort(), ConnectorParams.PING_ACTION, null, null, null);
    verify(this.apiGateway).callWithExecutionId(this.executor2.getHost(),
        this.executor2.getPort(), ConnectorParams.PING_ACTION, null, null, null);
    verifyZeroInteractions(this.alerterHolder);
  }

  /**
   * Test that any failures while sending alerts for unreachable executors don't prevent the
   * finalization(cleanup) of flows running on that executor.
   */
  @Test
  public void testFailureDuringAlerting() throws Exception {
    this.activeFlows.clear();
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, this.executor1, DispatchMethod.POLL), this.flow1));

    // Force a failure of the executor ping API
    ExecutorManagerException healthcheckException = new ExecutorManagerException("test exception");
    when(this.apiGateway.callWithExecutionId(
        this.executor1.getHost(),
        this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null, null))
        .thenThrow(healthcheckException);

    // Force an unchecked exception when sending alert emails for the healthcheck failure
    // Note that we can't use this.alerterHolder.get("email") in the when() as mockito
    // doesn't like nested mocks.
    doThrow(new RuntimeException("test runtime exception"))
        .when(this.mailAlerter)
        .alertOnFailedExecutorHealthCheck(
            this.executor1,
            Arrays.asList(this.flow1),
            healthcheckException,
            Arrays.asList(AZ_ADMIN_ALERT_EMAIL.split(",")));

    when(this.loader.fetchExecutableFlow(EXECUTION_ID_11)).thenReturn(this.flow1);
    for (int failureCount = 0;
        failureCount < props.getInt(ConfigurationKeys.AZKABAN_EXECUTOR_MAX_FAILURE_COUNT);
        failureCount++) {
      this.executorHealthChecker.checkExecutorHealth();
    }

    // Confirm that cleanup for the executor is attempted despite failure to send emails.
    // verify() can't be called on executorHealthCheck.cleanUpForMissingExecutor as it's not being
    // mocked. Directly checking the flow update through the mocked 'loader' is a suitable proxy
    // for this.
    verify(this.loader).updateExecutableFlow(this.flow1);
    assertThat(this.flow1.getStatus()).isEqualTo(Status.FAILED);
  }

  /**
   * Test that exceptions during flow finalization do not block finalization of subsequent flow
   * for an executor.
   */
  @Test
  public void testFailureDuringFinalization() throws Exception {
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, this.executor1, DispatchMethod.POLL), this.flow1));
    this.activeFlows.put(EXECUTION_ID_12, new Pair<>(
        new ExecutionReference(EXECUTION_ID_12, this.executor1, DispatchMethod.POLL), this.flow2));

    when(this.loader.fetchExecutableFlow(EXECUTION_ID_11)).thenThrow(new RuntimeException(
        "test runtime exception"));
    when(this.loader.fetchExecutableFlow(EXECUTION_ID_12)).thenThrow(new RuntimeException(
        "test runtime exception"));

    this.executorHealthChecker.finalizeFlows(ImmutableList.of(this.flow1, flow2),
        "test finalize reason");
    verify(this.loader).fetchExecutableFlow(flow1.getExecutionId());
    verify(this.loader).fetchExecutableFlow(flow2.getExecutionId());
  }
}
