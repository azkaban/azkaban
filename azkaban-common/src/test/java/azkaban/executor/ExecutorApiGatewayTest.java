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

import static org.mockito.Mockito.when;

import azkaban.utils.JSONUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ExecutorApiGatewayTest {

  private ExecutorApiGateway gateway;
  private ExecutorApiClient client;

  @Before
  public void setUp() throws Exception {
    this.client = Mockito.mock(ExecutorApiClient.class);
    this.gateway = new ExecutorApiGateway(this.client);
  }

  @Test
  public void testExecutorInfoJsonParser() throws Exception {
    final ExecutorInfo exeInfo = new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 89,
        10);
    final String json = JSONUtils.toJSON(exeInfo);
    when(this.client.httpPost(Mockito.any(), Mockito.any())).thenReturn(json);
    final ExecutorInfo exeInfo2 = this.gateway
        .callForJsonType("localhost", 1234, "executor", null, ExecutorInfo.class);
    Assert.assertTrue(exeInfo.equals(exeInfo2));
  }

}
