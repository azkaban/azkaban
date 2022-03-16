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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_HOSTNAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_PORT;
import static azkaban.executor.ExecutionControllerUtils.clusterQualifiedExecId;
import static azkaban.executor.ExecutorApiClientTest.REVERSE_PROXY_HOST;
import static azkaban.executor.ExecutorApiClientTest.REVERSE_PROXY_PORT;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ExecutorApiGatewayTest {

  private ExecutorApiGateway gateway;
  private ExecutorApiClient client;
  @Captor
  ArgumentCaptor<List<Pair<String, String>>> params;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    this.client = Mockito.mock(ExecutorApiClient.class);
    this.gateway = new ExecutorApiGateway(this.client, new Props());
  }

  @Test
  public void testExecutorInfoJsonParser() throws Exception {
    final ExecutorInfo exeInfo = new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 89,
        10);
    final String json = JSONUtils.toJSON(exeInfo);
    when(this.client.doPost(any(), any(), any(), any())).thenReturn(json);
    final ExecutorInfo exeInfo2 = this.gateway
        .callForJsonType("localhost", 1234, "executor", null, Optional.of(-1), null,
        ExecutorInfo.class);
    Assert.assertTrue(exeInfo.equals(exeInfo2));
  }

  @Test
  public void updateExecutions() throws Exception {
    final ImmutableMap<String, String> map = ImmutableMap.of("test", "response");
    when(this.client
        .doPost(any(), any(), any(), this.params.capture()))
        .thenReturn(JSONUtils.toJSON(map));
    final Map<String, Object> response = this.gateway
        .updateExecutions(new Executor(2, "executor-2", 1234, true),
            Collections.singletonList(new ExecutableFlow()));
    assertEquals(map, response);
    assertEquals(new Pair<>("executionId", "[-1]"), this.params.getValue().get(0));
    assertEquals(new Pair<>("updatetime", "[-1]"), this.params.getValue().get(1));
    assertEquals(new Pair<>("action", "update"), this.params.getValue().get(2));
    assertEquals(new Pair<>("execid", "null"), this.params.getValue().get(3));
    assertEquals(new Pair<>("user", null), this.params.getValue().get(4));
  }

  @Test
  public void testPathWithDefaultConfigs() throws Exception {
    final int executionId = 12345;
    final String path = gateway.createExecutionPath(Optional.of(executionId), null);
    Assert.assertEquals("/"+ExecutorApiGateway.DEFAULT_EXECUTION_RESOURCE, path);
  }

  @Test
  public void testPathWithContainerizedConfigs() throws Exception {
    final ExecutorApiGateway gateway = gatewayWithConfigs(this.client, containerizationEnabledProps());
    final int executionId = 12345;
    final String path = gateway.createExecutionPath(Optional.of(executionId), DispatchMethod.CONTAINERIZED);
    Assert.assertEquals("/" + clusterQualifiedExecId(gateway.getClusterName(), executionId) + "/" +
            ExecutorApiGateway.CONTAINERIZED_EXECUTION_RESOURCE,
        path);
  }

  private Props containerizationEnabledProps() {
    final Props containerizedProps = new Props();
    containerizedProps.put(ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_ENABLED, "true");
    containerizedProps.put(AZKABAN_EXECUTOR_REVERSE_PROXY_HOSTNAME, REVERSE_PROXY_HOST);
    containerizedProps.put(AZKABAN_EXECUTOR_REVERSE_PROXY_PORT, REVERSE_PROXY_PORT);
    containerizedProps.put(ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
        DispatchMethod.CONTAINERIZED.name());
    return containerizedProps;
  }

  private ExecutorApiGateway gatewayWithConfigs(final ExecutorApiClient client, final Props props) {
    return new ExecutorApiGateway(client, props);
  }

  @Test
  public void testPingWithDefaultConfigs() throws Exception {
    final Props props = new Props();
    final ExecutorApiClient clientSpy = Mockito.spy(new SendDisabledExecutorApiClient(props));

    final ExecutorApiGateway gateway = gatewayWithConfigs(clientSpy, props);
    final String apiAction = ConnectorParams.PING_ACTION;
    final String apiHost = "host1";
    final int apiPort = 1234;
    final Map<String, Object> response = gateway.callWithExecutionId(apiHost, apiPort, apiAction,
        null, null, null, Optional.of(5000));

    final URI expectedUri = new URI("http://host1:1234/executor");
    final List<Pair<String, String>> expectedParams = ImmutableList.of(
        new Pair<>("action", "ping"),
        new Pair<>("execid", "null"), // this is "null" (string) due to application of String.valueOf()
        new Pair<>("user", null));
    Mockito.verify(clientSpy).httpPost(eq(expectedUri), eq(Optional.of(5000)), eq(expectedParams));
  }

  @Test
  public void testPingWithContainerization() throws Exception {
    final Props props = new Props();
    props.put(ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD, DispatchMethod.CONTAINERIZED.name());
    final ExecutorApiClient clientSpy = Mockito.spy(new SendDisabledExecutorApiClient(props));
    final ExecutorApiGateway gateway = gatewayWithConfigs(clientSpy, props);
    final String apiAction = ConnectorParams.PING_ACTION;
    final String apiHost = "host1";
    final int apiPort = 1234;
    final Map<String, Object> response = gateway.callWithExecutionId(apiHost, apiPort, apiAction,
        1, "bond", DispatchMethod.CONTAINERIZED, Optional.of(5000));

    final URI expectedUri = new URI("http://host1:1234/azkaban-1/container");
    final List<Pair<String, String>> expectedParams = ImmutableList.of(
        new Pair<>("action", "ping"),
        new Pair<>("execid", "1"),
        new Pair<>("user", "bond"));
    Mockito.verify(clientSpy).httpPost(eq(expectedUri), eq(Optional.of(5000)), eq(expectedParams));
  }

  @Test(expected = ExecutorManagerException.class)
  public void testPingWithReverseProxy() throws Exception {
    final Props props = containerizationEnabledProps();
    final ExecutorApiClient client = new SendDisabledExecutorApiClient(props);
    final ExecutorApiGateway gateway = gatewayWithConfigs(client, props);
    final String apiAction = ConnectorParams.PING_ACTION;
    final String apiHost = "host1";
    final int apiPort = 1234;

    // this should throw an exception as the properties have reverse-proxy enabled and execution id is null
    final Map<String, Object> response = gateway.callWithExecutionId(apiHost, apiPort, apiAction,
        null, null, DispatchMethod.CONTAINERIZED, Optional.of(5000));
    Assert.fail();
  }

  private static class SendDisabledExecutorApiClient extends ExecutorApiClient {
    private static String SUCCESS_JSON = "{\"status\":\"success\"}";

    public SendDisabledExecutorApiClient(Props azkProps) {
      super(azkProps);
    }

    @Override
    protected String sendAndReturn(HttpUriRequest request, final Optional<Integer> httpTimeout) throws IOException {
      return SUCCESS_JSON;
    }
  }
}
