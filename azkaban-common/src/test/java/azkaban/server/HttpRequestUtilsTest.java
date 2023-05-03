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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTION_RESTART_LIMIT;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_MAX_RETRIES;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_RESTART_STRATEGY;
import static java.nio.charset.StandardCharsets.UTF_8;

import azkaban.Constants.FlowRetryStrategy;
import azkaban.DispatchMethod;
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
import azkaban.utils.Props;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test class for HttpRequestUtils
 */
public final class HttpRequestUtilsTest {

  public static Props testAzProps;

  @Before
  public void before() {
    testAzProps = new Props();
    testAzProps.put(AZKABAN_EXECUTION_RESTART_LIMIT, 2);
  }

  /* Helper method to get a test flow and add required properties */
  public static ExecutableFlow createExecutableFlow() throws IOException {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1",
        DispatchMethod.POLL);
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
            ImmutableSet.of(SlaAction.ALERT), ImmutableList.of(), ImmutableMap.of()),
        new SlaOption(SlaType.JOB_SUCCEED, "test-flow", "test_job", Duration.ofMinutes(720),
            ImmutableSet.of(SlaAction.KILL), ImmutableList.of(), ImmutableMap.of()),
        new SlaOption(SlaType.FLOW_SUCCEED, "test-flow", "", Duration.ofMinutes(720),
            ImmutableSet.of(SlaAction.ALERT, SlaAction.KILL), ImmutableList.of(), ImmutableMap.of())
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
        ImmutableList.of("sla1@example.com", "sla2@example.com"), ImmutableMap.of()));
    Assert.assertEquals(expected, slaOptions);
  }

  @Test
  public void testParseFlowOptionsSlaWithAlertersConfigs() throws Exception {
    final HttpServletRequest req = mockRequestWithSla(ImmutableMap.of(
        "slaSettings[1]", ",FINISH,2:30,true,false",
        "slaAlerters[email][recipients]", "sla1@example.com,sla2@example.com",
        "slaAlerters[myAlerter][prop1]", "value1"));
    final ExecutionOptions options = HttpRequestUtils.parseFlowOptions(req, "test-flow");
    final List<SlaOption> slaOptions = options.getSlaOptions();
    final List<SlaOption> expected = Arrays.asList(new SlaOption(SlaType.FLOW_FINISH, "test-flow",
        "", Duration.ofMinutes(150), ImmutableSet.of(SlaAction.ALERT),
        ImmutableList.of("sla1@example.com", "sla2@example.com"), ImmutableMap.of(
        "myAlerter", ImmutableMap.of("prop1", ImmutableList.of("value1"))
    )));
    Assert.assertEquals(expected, slaOptions);
  }

  @Test
  public void testParseFlowOptionsFlowOverride() throws Exception {
    final HttpServletRequest req = mockRequestWithSla(ImmutableMap.of(
        "flowOverride[key1]", "val1",
        "flowOverride[key.2]", "val.2"));
    final ExecutionOptions options = HttpRequestUtils.parseFlowOptions(req, "test-flow");
    final Map<String, String> expected = ImmutableMap.of(
        "key1", "val1",
        "key.2", "val.2");
    Assert.assertEquals(expected, options.getFlowParameters());
  }

  @Test
  public void testParseFlowOptionsRuntimeProperty() throws Exception {
    final HttpServletRequest req = mockRequestWithSla(ImmutableMap.of(
        "runtimeProperty[ROOT][key1]", "val1",
        "runtimeProperty[ROOT][key.2]", "val.2",
        "runtimeProperty[job-1][job.key]", "job-val",
        "runtimeProperty[job-1][job.key2]", "job-val2",
        "runtimeProperty[job-2][job.key]", "job-2-val"));
    final ExecutionOptions options = HttpRequestUtils.parseFlowOptions(req, "test-flow");
    Assert.assertEquals(ImmutableMap.of(
        "key1", "val1",
        "key.2", "val.2"
    ), options.getFlowParameters());
    Assert.assertEquals(ImmutableMap.of(
        "job-1", ImmutableMap.of(
            "job.key", "job-val",
            "job.key2", "job-val2"),
        "job-2", ImmutableMap.of(
            "job.key", "job-2-val")
    ), options.getRuntimeProperties());
  }

  private static HttpServletRequest mockRequestWithSla(final Map<String, String> params) {
    final HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getParameterNames()).thenAnswer(i ->
        // Enumeration is "consumed", so must create a new instance on every call
        Collections.enumeration(params.keySet()));
    Mockito.when(req.getParameter(Mockito.anyString()))
        .thenAnswer(i -> params.get(i.getArgument(0, String.class)));
    return req;
  }


  @Test
  public void testValidatePreprocessFlowParamWithoutAnyFlowParameter() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }

  @Test
  public void testValidatePreprocessFlowParamWithGood_ALLOW_RESTART_ON_STATUS()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }

  @Test
  public void testValidatePreprocessFlowParamWithDefaultAllowListAndGood_ALLOW_RESTART_ON_STATUS()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED"
    ));

    // if not defined, has default value to [EXECUTION_STOPPED, FAILED]
    HttpRequestUtils.validatePreprocessFlowParameters(options, new Props());
  }

  @Test(expected = ServletException.class)
  public void testValidatePreprocessFlowParamWithBadAzkabanStatus_ALLOW_RESTART_ON_STATUS()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    // KILLED is not defined
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "super-bad-status, not-an-azkaban-status"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }


  @Test(expected = ServletException.class)
  public void testValidatePreprocessFlowParamWithInvalid_ALLOW_RESTART_ON_STATUS()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    // KILLED is not defined
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "KILLED, EXECUTION_STOPPED"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }

  @Test
  public void testValidatePreprocessFlowParamWithBadFormat_ALLOW_RESTART_ON_STATUS()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    // KILLED is not defined
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, ", EXECUTION_STOPPED,, ,    "
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
    Assert.assertEquals(
        options.getFlowParameters().get(FLOW_PARAM_ALLOW_RESTART_ON_STATUS),
        "EXECUTION_STOPPED");
  }

  @Test
  public void testValidatePreprocessFlowParamWithGood_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_MAX_RETRIES, "1"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }

  @Test(expected = ServletException.class)
  public void testValidatePreprocessFlowParamWithNegative_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_MAX_RETRIES, "-11"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }

  @Test(expected = ServletException.class)
  public void testValidatePreprocessFlowParamWithExceed_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_MAX_RETRIES, "100000"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }

  @Test
  public void testValidatePreprocessFlowParamWithAllValidSettings() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED",
        FLOW_PARAM_MAX_RETRIES, "2"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }


  @Test
  public void testValidatePreprocessFlowParamWithAllowRestartExecutionStopped()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "FAILED",
        FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED, "true"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
    Map<String, String> result = options.getFlowParameters();
    Assert.assertTrue(
        result.get(FLOW_PARAM_ALLOW_RESTART_ON_STATUS).contains("EXECUTION_STOPPED"));
  }

  @Test
  public void testValidatePreprocessFlowParamWithNegativeAllowRestartExecutionStopped()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "FAILED",
        FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED, "false"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
    Map<String, String> result = options.getFlowParameters();
    Assert.assertFalse(
        result.get(FLOW_PARAM_ALLOW_RESTART_ON_STATUS).contains("EXECUTION_STOPPED"));
  }

  @Test
  public void testValidatePreprocessFlowParamWithEmptyMaxRetries()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "FAILED"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
    Map<String, String> result = options.getFlowParameters();
    Assert.assertNull(result.get(FLOW_PARAM_MAX_RETRIES));
  }

  @Test
  public void testValidatePreprocessFlowParamWithValidRetryStrategy()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "FAILED",
        FLOW_PARAM_RESTART_STRATEGY, FlowRetryStrategy.DISABLE_SUCCEEDED_NODES.name()
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }

  @Test(expected = ServletException.class)
  public void testValidatePreprocessFlowParamWithInvalidRetryStrategy()
      throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "FAILED",
        FLOW_PARAM_RESTART_STRATEGY, "some bad wrong text"
    ));

    HttpRequestUtils.validatePreprocessFlowParameters(options, testAzProps);
  }
}
