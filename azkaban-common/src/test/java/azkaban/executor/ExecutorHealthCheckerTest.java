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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import azkaban.Constants.ConfigurationKeys;
import azkaban.alert.Alerter;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
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
  private static final String AZ_ADMIN_ALERT_EMAIL = "az_admin1@foo.com,az_admin2@foo.com";
  private final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new HashMap<>();
  private ExecutorHealthChecker executorHealthChecker;
  private Props props;
  private ExecutorLoader loader;
  private ExecutorApiGateway apiGateway;
  private Alerter mailAlerter;
  private AlerterHolder alerterHolder;
  private ExecutableFlow flow1;
  private Executor executor1;

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
    this.flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    this.flow1.setExecutionId(EXECUTION_ID_11);
    this.flow1.setStatus(Status.RUNNING);
    this.executor1 = new Executor(1, "localhost", 12345, true);
    when(this.loader.fetchActiveFlows()).thenReturn(this.activeFlows);
    when(this.alerterHolder.get("email")).thenReturn(this.mailAlerter);
  }

  /**
   * Test running flow is not finalized and alert email is not sent when executor is alive.
   */
  @Test
  public void checkExecutorHealthAlive() throws Exception {
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, this.executor1), this.flow1));
    when(this.apiGateway.callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null)).thenReturn(ImmutableMap.of(ConnectorParams
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
        new ExecutionReference(EXECUTION_ID_11, null), this.flow1));
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
    this.activeFlows.put(EXECUTION_ID_11, new Pair<>(
        new ExecutionReference(EXECUTION_ID_11, this.executor1), this.flow1));
    // Failed to ping executor. Failure count (=1) < MAX_FAILURE_COUNT (=2). Do not alert.
    this.executorHealthChecker.checkExecutorHealth();
    verify(this.apiGateway).callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null);
    verifyZeroInteractions(this.alerterHolder);

    // Pinged executor successfully. Failure count (=0) < MAX_FAILURE_COUNT (=2). Do not alert.
    when(this.apiGateway.callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null)).thenReturn(ImmutableMap.of(ConnectorParams
        .STATUS_PARAM, ConnectorParams.RESPONSE_ALIVE));
    this.executorHealthChecker.checkExecutorHealth();
    verifyZeroInteractions(this.alerterHolder);

    // Failed to ping executor. Failure count (=1) < MAX_FAILURE_COUNT (=2). Do not alert.
    when(this.apiGateway.callWithExecutionId(this.executor1.getHost(), this.executor1.getPort(),
        ConnectorParams.PING_ACTION, null, null)).thenReturn(null);
    this.executorHealthChecker.checkExecutorHealth();
    verifyZeroInteractions(this.alerterHolder);

    // Failed to ping executor again. Failure count (=2) = MAX_FAILURE_COUNT (=2). Alert AZ admin.
    this.executorHealthChecker.checkExecutorHealth();
    verify((this.alerterHolder).get("email"))
        .alertOnFailedUpdate(eq(this.executor1), eq(Arrays.asList(this.flow1)),
            any(ExecutorManagerException.class));
    assertThat(this.flow1.getExecutionOptions().getFailureEmails()).isEqualTo
        (Arrays.asList(AZ_ADMIN_ALERT_EMAIL.split(",")));
  }
}
