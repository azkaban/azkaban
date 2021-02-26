/*
 * Copyright 2020 LinkedIn Corp.
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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_HOSTNAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_PORT;
import static azkaban.executor.ExecutorApiClientTest.REVERSE_PROXY_HOST;
import static azkaban.executor.ExecutorApiClientTest.REVERSE_PROXY_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.DispatchMethod;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.executor.container.ContainerizedImplType;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.user.User;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ContainerizedDispatchManagerTest {

  private final CommonMetrics commonMetrics = new CommonMetrics(
      new MetricsManager(new MetricRegistry()));
  private Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new HashMap<>();
  private Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows = new
      HashMap<>();
  private List<Pair<ExecutionReference, ExecutableFlow>> queuedFlows = new
      ArrayList<>();
  private ExecutorLoader loader;
  private ExecutorApiGateway apiGateway;
  private ContainerizedImpl containerizedImpl;
  private ContainerizedDispatchManager containerizedDispatchManager;
  private Props props;
  private User user;
  private ExecutableFlow flow1;
  private ExecutableFlow flow2;
  private ExecutableFlow flow3;
  private ExecutableFlow flow4;
  private ExecutionReference ref1;
  private ExecutionReference ref2;
  private ExecutionReference ref3;

  @Before
  public void setup() throws Exception {
    this.props = new Props();
    this.user = TestUtils.getTestUser();
    this.loader = mock(ExecutorLoader.class);
    this.apiGateway = mock(ExecutorApiGateway.class);
    this.containerizedImpl = mock(ContainerizedImpl.class);
    this.props.put(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW, 1);
    this.props.put(ContainerizedDispatchManagerProperties.CONTAINERIZED_IMPL_TYPE,
        ContainerizedImplType.KUBERNETES.name());
    this.flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    this.flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow3 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow4 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    this.flow1.setExecutionId(1);
    this.flow2.setExecutionId(2);
    this.flow3.setExecutionId(3);
    this.flow4.setExecutionId(4);
    this.ref1 = new ExecutionReference(this.flow1.getExecutionId(), null);
    this.ref2 = new ExecutionReference(this.flow2.getExecutionId(), null);
    this.ref3 = new ExecutionReference(this.flow3.getExecutionId(), null);

    this.activeFlows = ImmutableMap
        .of(this.flow2.getExecutionId(), new Pair<>(this.ref2, this.flow2),
            this.flow3.getExecutionId(), new Pair<>(this.ref3, this.flow3));
    when(this.loader.fetchActiveFlows()).thenReturn(this.activeFlows);
    when(this.loader.fetchActiveFlowByExecId(flow1.getExecutionId())).thenReturn(
        new Pair<ExecutionReference, ExecutableFlow>(new ExecutionReference(flow1.getExecutionId()), flow1));
    this.queuedFlows = ImmutableList.of(new Pair<>(this.ref1, this.flow1));
    when(this.loader.fetchQueuedFlows(Status.READY)).thenReturn(this.queuedFlows);

    Pair<ExecutionReference, ExecutableFlow> executionReferencePair =
        new Pair<ExecutionReference, ExecutableFlow>(new ExecutionReference(
            flow1.getExecutionId(), new Executor(1, "host", 2021, true)),
            flow1);
    when(this.loader.fetchUnfinishedFlows()).thenReturn(ImmutableMap.of(flow1.getExecutionId(),
        executionReferencePair));
  }

  @Test
  public void testRampUpDispatchMethod() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.setRampUp(0);
    for (int i = 0; i < 100; i++) {
      DispatchMethod dispatchMethod = this.containerizedDispatchManager.getDispatchMethod();
      assertThat(dispatchMethod).isEqualTo(DispatchMethod.POLL);
    }
    this.containerizedDispatchManager.setRampUp(100);
    for (int i = 0; i < 100; i++) {
      DispatchMethod dispatchMethod = this.containerizedDispatchManager.getDispatchMethod();
      assertThat(dispatchMethod).isEqualTo(DispatchMethod.CONTAINERIZED);
    }
  }

  @Test
  public void testFetchAllActiveFlows() throws Exception {
    initializeContainerizedDispatchImpl();
    initializeUnfinishedFlows();
    final List<ExecutableFlow> flows = this.containerizedDispatchManager.getRunningFlows();
    this.unfinishedFlows.values()
        .forEach(pair -> assertThat(flows.contains(pair.getSecond())).isTrue());
  }

  @Test
  public void testFetchAllActiveFlowIds() throws Exception {
    initializeContainerizedDispatchImpl();
    initializeUnfinishedFlows();
    assertThat(this.containerizedDispatchManager.getRunningFlowIds())
        .isEqualTo(new ArrayList<>(this.unfinishedFlows.keySet()));
  }

  @Test
  public void testFetchAllQueuedFlowIds() throws Exception {
    initializeContainerizedDispatchImpl();
    assertThat(this.containerizedDispatchManager.getQueuedFlowIds())
        .isEqualTo(ImmutableList.of(this.flow1.getExecutionId()));
  }

  @Test
  public void testFetchQueuedFlowSize() throws Exception {
    initializeContainerizedDispatchImpl();
    assertThat(this.containerizedDispatchManager.getQueuedFlowSize())
        .isEqualTo(this.queuedFlows.size());
  }

  @Test
  public void testFetchActiveFlowByProject() throws Exception {
    initializeContainerizedDispatchImpl();
    initializeUnfinishedFlows();
    final List<Integer> executions = this.containerizedDispatchManager
        .getRunningFlows(this.flow2.getProjectId(), this.flow2.getFlowId());
    assertThat(executions.contains(this.flow2.getExecutionId())).isTrue();
    assertThat(executions.contains(this.flow3.getExecutionId())).isTrue();
    assertThat(this.containerizedDispatchManager
        .isFlowRunning(this.flow2.getProjectId(), this.flow2.getFlowId()))
        .isTrue();
    assertThat(this.containerizedDispatchManager
        .isFlowRunning(this.flow3.getProjectId(), this.flow3.getFlowId()))
        .isTrue();
  }

  @Test
  public void testSubmitFlows() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.submitExecutableFlow(this.flow1, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(this.flow1);
  }

  @Test
  public void testSubmitFlowsExceedingMaxConcurrentRuns() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.disableQueueProcessorThread();
    this.props.put(ConfigurationKeys.CONCURRENT_RUNS_ONEFLOW_WHITELIST, "exectest1,"
        + "exec2,3");
    submitFlow(this.flow2, this.ref2);
    submitFlow(this.flow3, this.ref3);
    assertThatThrownBy(() -> this.containerizedDispatchManager
        .submitExecutableFlow(this.flow4, this.user.getUserId
            ())).isInstanceOf(ExecutorManagerException.class).hasMessageContaining("Flow " + this
        .flow4.getId() + " has more than 1 concurrent runs. Skipping");
  }

  @Test
  public void testSubmitFlowsConcurrentWhitelist() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.disableQueueProcessorThread();
    this.props.put(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW, 1);
    submitFlow(this.flow2, this.ref2);
    submitFlow(this.flow3, this.ref3);
    assertThatThrownBy(() -> this.containerizedDispatchManager
        .submitExecutableFlow(this.flow4, this.user.getUserId
            ())).isInstanceOf(ExecutorManagerException.class).hasMessageContaining("Flow " + this
        .flow4.getId() + " has more than 1 concurrent runs. Skipping");
  }


  @Test
  public void testSubmitFlowsWithSkipOption() throws Exception {
    initializeContainerizedDispatchImpl();
    submitFlow(this.flow2, this.ref2);
    this.flow3.getExecutionOptions().setConcurrentOption(ExecutionOptions.CONCURRENT_OPTION_SKIP);
    assertThatThrownBy(
        () -> this.containerizedDispatchManager
            .submitExecutableFlow(this.flow3, this.user.getUserId()))
        .isInstanceOf(ExecutorManagerException.class).hasMessageContaining(
        "Flow " + this.flow3.getId() + " is already running. Skipping execution.");
  }


  @Test
  public void testSetFlowLock() throws Exception {
    initializeContainerizedDispatchImpl();
    // trying to execute a locked flow should raise an error
    this.flow1.setLocked(true);
    final String msg = this.containerizedDispatchManager
        .submitExecutableFlow(this.flow1, this.user.getUserId());
    assertThat(msg).isEqualTo("Flow derived-member-data for project flow is locked.");

    // should succeed after unlocking the flow
    this.flow1.setLocked(false);
    this.containerizedDispatchManager.submitExecutableFlow(this.flow1, this.user.getUserId());
    verify(this.loader).uploadExecutableFlow(this.flow1);
  }

  /* Test disabling queue process thread to pause dispatching */
  @Test
  public void testDisablingQueueProcessThread() throws Exception {
    initializeContainerizedDispatchImpl();
    Assert.assertEquals(this.containerizedDispatchManager.isQueueProcessorThreadActive(), true);
    this.containerizedDispatchManager.disableQueueProcessorThread();
    Assert.assertEquals(this.containerizedDispatchManager.isQueueProcessorThreadActive(), false);
    this.containerizedDispatchManager.enableQueueProcessorThread();
  }

  /* Test renabling queue process thread to pause restart dispatching */
  @Test
  public void testEnablingQueueProcessThread() throws Exception {
    initializeContainerizedDispatchImpl();
    this.containerizedDispatchManager.disableQueueProcessorThread();
    Assert.assertEquals(this.containerizedDispatchManager.isQueueProcessorThreadActive(), false);
    this.containerizedDispatchManager.enableQueueProcessorThread();
    Assert.assertEquals(this.containerizedDispatchManager.isQueueProcessorThreadActive(), true);
  }

  private void submitFlow(final ExecutableFlow flow, final ExecutionReference ref) throws
      Exception {
    when(this.loader.fetchUnfinishedFlows()).thenReturn(this.unfinishedFlows);
    when(this.loader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    this.containerizedDispatchManager.submitExecutableFlow(flow, this.user.getUserId());
    this.unfinishedFlows.put(flow.getExecutionId(), new Pair<>(ref, flow));
  }

  private void initializeUnfinishedFlows() throws Exception {
    this.unfinishedFlows = ImmutableMap
        .of(this.flow1.getExecutionId(), new Pair<>(this.ref1, this.flow1),
            this.flow2.getExecutionId(), new Pair<>(this.ref2, this.flow2),
            this.flow3.getExecutionId(), new Pair<>(this.ref3, this.flow3));
    when(this.loader.fetchUnfinishedFlows()).thenReturn(this.unfinishedFlows);
  }

  private void initializeContainerizedDispatchImpl() throws Exception{
    this.containerizedDispatchManager =
        new ContainerizedDispatchManager(this.props, this.loader,
        this.commonMetrics,
        this.apiGateway, this.containerizedImpl, null);
    this.containerizedDispatchManager.start();
  }

  @Test
  public void testGetFlowLogs() throws Exception {
    WrappedExecutorApiClient apiClient =
        new WrappedExecutorApiClient(createContainerDispatchEnabledProps(this.props));
    ContainerizedDispatchManager dispatchManager = createDefaultDispatchWithGateway(apiClient);
    apiClient.setNextHttpPostResponse(WrappedExecutorApiClient.DEFAULT_LOG_JSON);
    LogData logResponse = dispatchManager.getExecutableFlowLog(this.flow1, 0, 1024);
    Assert.assertEquals(WrappedExecutorApiClient.DEFAULT_LOG_TEXT, logResponse.getData());
    Assert.assertEquals(apiClient.getExpectedReverseProxyContainerizedURI(),
        apiClient.getLastBuildExecutorUriRespone());
  }

  @Test
  public void testGetJobLogs() throws Exception {
    WrappedExecutorApiClient apiClient =
        new WrappedExecutorApiClient(createContainerDispatchEnabledProps(this.props));
    ContainerizedDispatchManager dispatchManager = createDefaultDispatchWithGateway(apiClient);
    apiClient.setNextHttpPostResponse(WrappedExecutorApiClient.DEFAULT_LOG_JSON);
    LogData logResponse = dispatchManager.getExecutionJobLog(this.flow1, "job1",0, 1, 1024);
    Assert.assertEquals(WrappedExecutorApiClient.DEFAULT_LOG_TEXT, logResponse.getData());
    Assert.assertEquals(apiClient.getExpectedReverseProxyContainerizedURI(),
        apiClient.getLastBuildExecutorUriRespone());
  }

  @Test
  public void testCancelFlow() throws Exception {
    WrappedExecutorApiClient apiClient =
        new WrappedExecutorApiClient(createContainerDispatchEnabledProps(this.props));
    ContainerizedDispatchManager dispatchManager = createDefaultDispatchWithGateway(apiClient);
    apiClient.setNextHttpPostResponse(WrappedExecutorApiClient.STATUS_SUCCESS_JSON);
    dispatchManager.cancelFlow(flow1, this.user.getUserId());
    Assert.assertEquals(apiClient.getExpectedReverseProxyContainerizedURI(),
        apiClient.getLastBuildExecutorUriRespone());
    //Verify that httpPost was requested with the 'cancel' param.
    Pair cancelAction = new Pair<String, String> ("action", "cancel");
    Assert.assertTrue(apiClient.getLastHttpPostParams().stream().anyMatch(pair -> cancelAction.equals(pair)));
  }

  @Test
  public void testCancelFlowWithMissingExecutor() throws Exception {
    // Return a null executor for the unfinished execution
    Pair<ExecutionReference, ExecutableFlow> executionReferencePair =
        new Pair<ExecutionReference, ExecutableFlow>(new ExecutionReference(flow1.getExecutionId(), null), flow1);
    when(this.loader.fetchUnfinishedFlows()).thenReturn(ImmutableMap.of(flow1.getExecutionId(),
        executionReferencePair));

    WrappedExecutorApiClient apiClient =
        new WrappedExecutorApiClient(createContainerDispatchEnabledProps(this.props));
    ContainerizedDispatchManager dispatchManager = createDefaultDispatchWithGateway(apiClient);
    apiClient.setNextHttpPostResponse(WrappedExecutorApiClient.STATUS_SUCCESS_JSON);
    dispatchManager.cancelFlow(flow1, this.user.getUserId());
    Assert.assertEquals(apiClient.getExpectedReverseProxyContainerizedURI(),
        apiClient.getLastBuildExecutorUriRespone());
    //Verify that httpPost was requested with the 'cancel' param.
    Pair cancelAction = new Pair<String, String> ("action", "cancel");
    Assert.assertTrue(apiClient.getLastHttpPostParams().stream().anyMatch(pair -> cancelAction.equals(pair)));
  }

  private Props createContainerDispatchEnabledProps(Props parentProps) {
    Props containerProps = new Props(parentProps);
    containerProps.put(ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_ENABLED, "true");
    containerProps.put(AZKABAN_EXECUTOR_REVERSE_PROXY_HOSTNAME, REVERSE_PROXY_HOST);
    containerProps.put(AZKABAN_EXECUTOR_REVERSE_PROXY_PORT, REVERSE_PROXY_PORT);
    containerProps.put(ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
        DispatchMethod.CONTAINERIZED.name());
    return containerProps;
  }

  private ContainerizedDispatchManager createDefaultDispatchWithGateway(ExecutorApiClient apiClient) throws Exception {
    Props containerEnabledProps = createContainerDispatchEnabledProps(this.props);
    ExecutorApiGateway executorApiGateway = new ExecutorApiGateway(apiClient, containerEnabledProps);
    return createDispatchWithGateway(executorApiGateway, containerEnabledProps);
  }

  private ContainerizedDispatchManager createDispatchWithGateway(ExecutorApiGateway apiGateway,
      Props containerEnabledProps) throws Exception {
    ContainerizedDispatchManager dispatchManager =
        new ContainerizedDispatchManager(containerEnabledProps, this.loader,
            this.commonMetrics, apiGateway, this.containerizedImpl, null);
    dispatchManager.start();
    return dispatchManager;
  }

  @NotThreadSafe
  private static class WrappedExecutorApiClient extends ExecutorApiClient {
    private static String DEFAULT_LOG_TEXT = "line1";
    private static String DEFAULT_LOG_JSON =
        String.format("{\"length\":%d,\"offset\":0,\"data\":\"%s\"}",
            DEFAULT_LOG_TEXT.length(),
            DEFAULT_LOG_TEXT);
    private static String STATUS_SUCCESS_JSON = "{\"status\":\"success\"}";
    private URI lastBuildExecutorUriRespone = null;
    private URI lastHttpPostUri = null;
    private List<Pair<String, String>> lastHttpPostParams = null;
    private String nextHttpPostResponse = DEFAULT_LOG_JSON;

    public WrappedExecutorApiClient(Props azkProps) {
      super(azkProps);
    }

    public URI getExpectedReverseProxyContainerizedURI() throws IOException {
      return buildExecutorUri(null, 1, "container", false, (Pair<String, String>[]) null);
    }

    @Override
    public URI buildExecutorUri(String host, int port, String path,
        boolean isHttp, Pair<String, String>... params) throws IOException {
      this.lastBuildExecutorUriRespone = super.buildExecutorUri(host, port, path, isHttp, params);
      return lastBuildExecutorUriRespone;
    }

    @Override
    public String httpPost(URI uri, List<Pair<String, String>> params)
        throws IOException {
      this.lastHttpPostUri = uri;
      this.lastHttpPostParams = params;
      return nextHttpPostResponse;
    }

  public void setNextHttpPostResponse(String nextHttpPostResponse) {
    this.nextHttpPostResponse = nextHttpPostResponse;
  }

  public URI getLastBuildExecutorUriRespone() {
      return lastBuildExecutorUriRespone;
    }

    public URI getLastHttpPostUri() {
      return lastHttpPostUri;
    }

    public List<Pair<String, String>> getLastHttpPostParams() {
      return lastHttpPostParams;
    }
  }

  @After
  public void shutdown() {
    if(this.containerizedDispatchManager != null) {
      this.containerizedDispatchManager.shutdown();
    }
  }
}
