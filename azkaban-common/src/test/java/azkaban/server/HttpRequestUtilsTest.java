/*
 * Copyright 2015 LinkedIn Corp.
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
package azkaban.server;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManagerException;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.utils.TestUtils;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static java.nio.charset.StandardCharsets.*;


/**
 * Test class for HttpRequestUtils
 */
public final class HttpRequestUtilsTest {

  /* Helper method to get a test flow and add required properties */
  public static ExecutableFlow createExecutableFlow() throws IOException {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    flow.getExecutionOptions().getFlowParameters()
        .put(ExecutionOptions.FLOW_PRIORITY, "1");
    flow.getExecutionOptions().getFlowParameters()
        .put(ExecutionOptions.USE_EXECUTOR, "2");
    return flow;
  }

  /* Test that flow properties are removed for non-admin user */
  @Test
  public void TestFilterNonAdminOnlyFlowParams() throws IOException,
      ExecutorManagerException, UserManagerException {
    final ExecutableFlow flow = createExecutableFlow();
    final UserManager manager = TestUtils.createTestXmlUserManager();
    final User user = manager.getUser("testUser", "testUser");

    HttpRequestUtils.filterAdminOnlyFlowParams(manager,
        flow.getExecutionOptions(), user);

    Assert.assertFalse(flow.getExecutionOptions().getFlowParameters()
        .containsKey(ExecutionOptions.FLOW_PRIORITY));
    Assert.assertFalse(flow.getExecutionOptions().getFlowParameters()
        .containsKey(ExecutionOptions.USE_EXECUTOR));
  }

  /* Test that flow properties are retained for admin user */
  @Test
  public void TestFilterAdminOnlyFlowParams() throws IOException,
      ExecutorManagerException, UserManagerException {
    final ExecutableFlow flow = createExecutableFlow();
    final UserManager manager = TestUtils.createTestXmlUserManager();
    final User user = manager.getUser("testAdmin", "testAdmin");

    HttpRequestUtils.filterAdminOnlyFlowParams(manager,
        flow.getExecutionOptions(), user);

    Assert.assertTrue(flow.getExecutionOptions().getFlowParameters()
        .containsKey(ExecutionOptions.FLOW_PRIORITY));
    Assert.assertTrue(flow.getExecutionOptions().getFlowParameters()
        .containsKey(ExecutionOptions.USE_EXECUTOR));
  }

  /* Test exception, if param is a valid integer */
  @Test
  public void testvalidIntegerParam() throws ExecutorManagerException {
    final Map<String, String> params = new HashMap<>();
    params.put("param1", "123");
    HttpRequestUtils.validateIntegerParam(params, "param1");
  }

  /* Test exception, if param is not a valid integer */
  @Test(expected = ExecutorManagerException.class)
  public void testInvalidIntegerParam() throws ExecutorManagerException {
    final Map<String, String> params = new HashMap<>();
    params.put("param1", "1dff2");
    HttpRequestUtils.validateIntegerParam(params, "param1");
  }

  /* Verify permission for admin user */
  @Test
  public void testHasAdminPermission() throws UserManagerException {
    final UserManager manager = TestUtils.createTestXmlUserManager();
    final User adminUser = manager.getUser("testAdmin", "testAdmin");
    Assert.assertTrue(HttpRequestUtils.hasPermission(manager, adminUser,
        Type.ADMIN));
  }

  /* verify permission for non-admin user */
  @Test
  public void testHasOrdinaryPermission() throws UserManagerException {
    final UserManager manager = TestUtils.createTestXmlUserManager();
    final User testUser = manager.getUser("testUser", "testUser");
    Assert.assertFalse(HttpRequestUtils.hasPermission(manager, testUser,
        Type.ADMIN));
  }

  @Test
  public void testGetJsonBodyForListOfMapObject() throws IOException, ServletException {
    HttpServletRequest httpRequest = Mockito.mock(HttpServletRequest.class);
    String originalString = "[\n" + "  {\n" + "    \"action\": \"update\",\n" + "    \"table\": \"ramp\",\n"
        + "    \"conditions\" : {\n" + "      \"rampId\" : \"dali\"\n" + "    },\n" + "    \"values\": {\n"
        + "      \"rampStage\": 2,\n" + "      \"lastUpdatedTime\": 1566259437000\n" + "    }\n" + "  }\n" + "]";
    InputStream inputStream = new ByteArrayInputStream(originalString.getBytes(UTF_8));
    InputStreamReader inputStreamReader = new InputStreamReader(inputStream, UTF_8);
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    Mockito.when(httpRequest.getReader()).thenReturn(bufferedReader);
    Object object = HttpRequestUtils.getJsonBody(httpRequest);
    Assert.assertTrue(object instanceof List);
    List<Map<String, Object>> list = (List<Map<String, Object>>) object;
    Assert.assertEquals(list.size(), 1);
    Assert.assertEquals(list.get(0).get("action").toString(), "update");
    Assert.assertEquals(list.get(0).get("table").toString(), "ramp");
    Assert.assertEquals(((Map<String,Object>)list.get(0).get("conditions")).get("rampId").toString(), "dali");
    Assert.assertEquals(((Map<String,Object>)list.get(0).get("values")).get("rampStage"), 2);
    Assert.assertEquals(((Map<String,Object>)list.get(0).get("values")).get("lastUpdatedTime"), 1566259437000L);
  }

  @Test
  public void testGetJsonBodyForSingleMapObject() throws IOException, ServletException {
    HttpServletRequest httpRequest = Mockito.mock(HttpServletRequest.class);
    String originalString = "  {\n" + "    \"action\": \"update\",\n" + "    \"table\": \"ramp\",\n"
        + "    \"conditions\" : {\n" + "      \"rampId\" : \"dali\"\n" + "    },\n" + "    \"values\": {\n"
        + "      \"rampStage\": 2,\n" + "      \"lastUpdatedTime\": 1566259437000\n" + "    }\n" + "  }\n";
    InputStream inputStream = new ByteArrayInputStream(originalString.getBytes(UTF_8));
    InputStreamReader inputStreamReader = new InputStreamReader(inputStream, UTF_8);
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    Mockito.when(httpRequest.getReader()).thenReturn(bufferedReader);
    Object object = HttpRequestUtils.getJsonBody(httpRequest);
    Assert.assertTrue(object instanceof Map);
    Map<String, Object> singleObj = (Map<String, Object>) object;
    Assert.assertEquals(singleObj.get("action").toString(), "update");
    Assert.assertEquals(singleObj.get("table").toString(), "ramp");
    Assert.assertEquals(((Map<String,Object>)singleObj.get("conditions")).get("rampId").toString(), "dali");
    Assert.assertEquals(((Map<String,Object>)singleObj.get("values")).get("rampStage"), 2);
    Assert.assertEquals(((Map<String,Object>)singleObj.get("values")).get("lastUpdatedTime"), 1566259437000L);
  }
}
