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

import static java.nio.charset.StandardCharsets.UTF_8;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManagerException;
import azkaban.sla.SlaAction;
import azkaban.sla.SlaOption;
import azkaban.sla.SlaType;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.utils.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


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
    final HttpServletRequest httpRequest = Mockito.mock(HttpServletRequest.class);
    final String originalString = TestUtils.readResource("list_map_object.json", this);
    final InputStream inputStream = new ByteArrayInputStream(originalString.getBytes(UTF_8));
    final InputStreamReader inputStreamReader = new InputStreamReader(inputStream, UTF_8);
    final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    Mockito.when(httpRequest.getReader()).thenReturn(bufferedReader);
    final Object object = HttpRequestUtils.getJsonBody(httpRequest);
    Assert.assertTrue(object instanceof List);
    final List<Map<String, Object>> list = (List<Map<String, Object>>) object;
    Assert.assertEquals(list.size(), 1);
    Assert.assertEquals(list.get(0).get("action").toString(), "update");
    Assert.assertEquals(list.get(0).get("table").toString(), "ramp");
    Assert.assertEquals(
        ((Map<String, Object>) list.get(0).get("conditions")).get("rampId").toString(), "dali");
    Assert.assertEquals(((Map<String, Object>) list.get(0).get("values")).get("rampStage"), 2);
    Assert.assertEquals(((Map<String, Object>) list.get(0).get("values")).get("lastUpdatedTime"),
        1566259437000L);
  }

  @Test
  public void testGetJsonBodyForSingleMapObject() throws IOException, ServletException {
    final HttpServletRequest httpRequest = Mockito.mock(HttpServletRequest.class);
    final String originalString = TestUtils.readResource("single_map_object.json", this);
    final InputStream inputStream = new ByteArrayInputStream(originalString.getBytes(UTF_8));
    final InputStreamReader inputStreamReader = new InputStreamReader(inputStream, UTF_8);
    final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    Mockito.when(httpRequest.getReader()).thenReturn(bufferedReader);
    final Object object = HttpRequestUtils.getJsonBody(httpRequest);
    Assert.assertTrue(object instanceof Map);
    final Map<String, Object> singleObj = (Map<String, Object>) object;
    Assert.assertEquals(singleObj.get("action").toString(), "update");
    Assert.assertEquals(singleObj.get("table").toString(), "ramp");
    Assert
        .assertEquals(((Map<String, Object>) singleObj.get("conditions")).get("rampId").toString(),
            "dali");
    Assert.assertEquals(((Map<String, Object>) singleObj.get("values")).get("rampStage"), 2);
    Assert.assertEquals(((Map<String, Object>) singleObj.get("values")).get("lastUpdatedTime"),
        1566259437000L);
  }

  @Test
  public void testParseFlowOptionsSla() throws Exception {
    final HttpServletRequest req = mockRequestWithSla(ImmutableMap.of(
        // job_name, status, duration, is_email, is_kill
        "slaSettings[1]", ",FINISH,2:30,true,false",
        "slaSettings[2]", "test_job,SUCCESS,12:00,false,true",
        "slaSettings[3]", ",SUCCESS,12:00,true,true"));
    final ExecutionOptions options = HttpRequestUtils.parseFlowOptions(req, "test-flow");
    final List<SlaOption> slaOptions = options.getSlaOptions();
    final List<SlaOption> expected = Arrays.asList(
        new SlaOption(SlaType.FLOW_FINISH, "test-flow", "", Duration.ofMinutes(150),
            ImmutableSet.of(SlaAction.ALERT), ImmutableList.of()),
        new SlaOption(SlaType.JOB_SUCCEED, "test-flow", "test_job", Duration.ofMinutes(720),
            ImmutableSet.of(SlaAction.KILL), ImmutableList.of()),
        new SlaOption(SlaType.FLOW_SUCCEED, "test-flow", "", Duration.ofMinutes(720),
            ImmutableSet.of(SlaAction.ALERT, SlaAction.KILL), ImmutableList.of())
    );
    Assert.assertEquals(expected, slaOptions);
  }

  @Test
  public void testParseFlowOptionsSlaWithEmail() throws Exception {
    final HttpServletRequest req = mockRequestWithSla(ImmutableMap.of(
        "slaSettings[1]", ",FINISH,2:30,true,false",
        "slaEmails", "sla1@example.com,sla2@example.com"));
    final ExecutionOptions options = HttpRequestUtils.parseFlowOptions(req, "test-flow");
    final List<SlaOption> slaOptions = options.getSlaOptions();
    final List<SlaOption> expected = Arrays.asList(new SlaOption(SlaType.FLOW_FINISH, "test-flow",
        "", Duration.ofMinutes(150), ImmutableSet.of(SlaAction.ALERT),
        ImmutableList.of("sla1@example.com", "sla2@example.com")));
    Assert.assertEquals(expected, slaOptions);
  }

  private static HttpServletRequest mockRequestWithSla(final Map<String, String> params) {
    final HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getParameterNames()).thenReturn(Collections.enumeration(params.keySet()));
    Mockito.when(req.getParameter(Mockito.anyString()))
        .thenAnswer(i -> params.get(i.getArgument(0, String.class)));
    return req;
  }

}
