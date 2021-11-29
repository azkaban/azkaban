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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    when(this.client.httpPost(any(), any())).thenReturn(json);
    final ExecutorInfo exeInfo2 = this.gateway
        .callForJsonType("localhost", 1234, "executor", null, ExecutorInfo.class);
    Assert.assertTrue(exeInfo.equals(exeInfo2));
  }

  @Test
  public void updateExecutions() throws Exception {
    final ImmutableMap<String, String> map = ImmutableMap.of("test", "response");
    when(this.client
        .httpPost(any(), this.params.capture()))
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
  public void testPathWithDefaultConfigs() {
    int executionId = 12345;
    String path = gateway.createExecutionPath(12345);
    Assert.assertEquals("/"+ExecutorApiGateway.DEFAULT_EXECUTION_RESOURCE, path);
  }

  @Test
  public void testPathWithContainerizedConfigs() {
    Props containerizedConfigs = new Props();
    containerizedConfigs.put(ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_ENABLED, "true");
    containerizedConfigs.put(ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
        DispatchMethod.CONTAINERIZED.name());
    ExecutorApiGateway gateway = new ExecutorApiGateway(client, containerizedConfigs);
    int executionId = 12345;
    String path = gateway.createExecutionPath(12345);
    Assert.assertEquals("/" + executionId + "/" +
            ExecutorApiGateway.CONTAINERIZED_EXECUTION_RESOURCE,
        path);
  }
}
