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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for HttpRequestUtils
 */
public final class HttpRequestUtilsTest {

  /* Helper method to get a test flow and add required properties */
  public static ExecutableFlow createExecutableFlow() throws IOException {
    return createExecutableFlow("2");
  }

  /* Helper method to get a test flow and add required properties */
  public static ExecutableFlow createExecutableFlow(String useExecutor) throws IOException {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    flow.getExecutionOptions().getFlowParameters()
        .put(ExecutionOptions.FLOW_PRIORITY, "1");
    flow.getExecutionOptions().getFlowParameters()
        .put(ExecutionOptions.USE_EXECUTOR, useExecutor);
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

  @Test
  public void TestForInValidUseExecutorFlowParameter() throws IOException,
          ExecutorManagerException, UserManagerException {
    final UserManager manager = TestUtils.createTestXmlUserManager();
    final User user = manager.getUser("testAdmin", "testAdmin");

    final ExecutableFlow hostOnlyFlow = createExecutableFlow("hostOnly:");
    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(manager,
              hostOnlyFlow.getExecutionOptions(), user);
    } catch (ExecutorManagerException expected) {
      if (!expected.getMessage().contains("should either be an integer or in format 'hostName:validPortNumber'")) {
        Assert.fail("we excepted validation msg to specify improper format was provided but got msg: " + expected.getMessage());
      }
    }

    final ExecutableFlow portOnlyFlow = createExecutableFlow(":9090");
    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(manager,
              portOnlyFlow.getExecutionOptions(), user);
    } catch (ExecutorManagerException expected) {
      if (!expected.getMessage().contains("should either be an integer or in format 'hostName:validPortNumber'")) {
        Assert.fail("we excepted validation msg to specify improper format was provided but got msg: " + expected.getMessage());
      }
    }

    final ExecutableFlow emptyFlow = createExecutableFlow(":");
    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(manager,
              emptyFlow.getExecutionOptions(), user);
    } catch (ExecutorManagerException expected) {
      if (!expected.getMessage().contains("should either be an integer or in format 'hostName:validPortNumber'")) {
        Assert.fail("we excepted validation msg to specify improper format was provided but got msg: " + expected.getMessage());
      }
    }

    final ExecutableFlow noSemiColonFlow = createExecutableFlow("");
    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(manager,
              noSemiColonFlow.getExecutionOptions(), user);
    } catch (ExecutorManagerException expected) {
      if (!expected.getMessage().contains("should either be an integer or in format 'hostName:validPortNumber'")) {
        Assert.fail("we excepted validation msg to specify improper format was provided but got msg: " + expected.getMessage());
      }
    }


    final ExecutableFlow badPortFlow = createExecutableFlow("host:badPort");
    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(manager,
              badPortFlow.getExecutionOptions(), user);
    } catch (ExecutorManagerException expected) {
      if (!expected.getMessage().contains("should either be an integer or in format 'hostName:validPortNumber'")) {
        Assert.fail("we excepted validation msg to specify improper format was provided but got msg: " + expected.getMessage());
      }
    }

    final ExecutableFlow badPort2Flow = createExecutableFlow("host:-2");
    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(manager,
              badPort2Flow.getExecutionOptions(), user);
    } catch (ExecutorManagerException expected) {
      if (!expected.getMessage().contains("should either be an integer or in format 'hostName:validPortNumber'")) {
        Assert.fail("we excepted validation msg to specify improper format was provided but got msg: " + expected.getMessage());
      }
    }
  }

  @Test
  public void TestForValidUseExecutorFlowParameter() throws IOException, UserManagerException, ExecutorManagerException {
    final UserManager manager = TestUtils.createTestXmlUserManager();
    final User user = manager.getUser("testAdmin", "testAdmin");
    final ExecutableFlow goodFlow = createExecutableFlow("localhost:9009");
    //no exception should be thrown
    HttpRequestUtils.filterAdminOnlyFlowParams(manager, goodFlow.getExecutionOptions(), user);
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
}
