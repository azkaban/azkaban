/*
 * Copyright 2023 LinkedIn, Inc
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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTION_RESTART_LIMIT;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_RESTART_COUNT;

import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.servlet.ServletException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExecutionOptionsTest {

  public static Props testAzProps;

  @Before
  public void before(){
    testAzProps = new Props();
    testAzProps.put(AZKABAN_EXECUTION_RESTART_LIMIT, 2);
  }

  @Test
  public void testValidatePreprocessFlowParamWithoutAnyFlowParameter() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.validatePreprocessFlowParameters(testAzProps);
  }

  @Test
  public void testValidatePreprocessFlowParamWithGood_ALLOW_RESTART_ON_STATUS() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED"
    ));

    options.validatePreprocessFlowParameters(testAzProps);
  }

  @Test
  public void testValidatePreprocessFlowParamWithDefaultAllowListAndGood_ALLOW_RESTART_ON_STATUS() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED"
    ));

    // if not defined, has default value to [EXECUTION_STOPPED, FAILED]
    options.validatePreprocessFlowParameters(new Props());
  }

  @Test(expected = ServletException.class)
  public void testValidatePreprocessFlowParamWithInvalid_ALLOW_RESTART_ON_STATUS() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    // KILLED is not defined
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "KILLED"
    ));

    options.validatePreprocessFlowParameters(testAzProps);
  }

  @Test
  public void testValidatePreprocessFlowParamWithGood_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_RESTART_COUNT, "1"
    ));

    options.validatePreprocessFlowParameters(testAzProps);
  }

  @Test(expected = ServletException.class)
  public void testValidatePreprocessFlowParamWithNegative_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_RESTART_COUNT, "-11"
    ));

    options.validatePreprocessFlowParameters(testAzProps);
  }

  @Test(expected = ServletException.class)
  public void testValidatePreprocessFlowParamWithExceed_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_RESTART_COUNT, "100000"
    ));

    options.validatePreprocessFlowParameters(testAzProps);
  }

  @Test
  public void testValidatePreprocessFlowParamWithAllValidSettings() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED",
        FLOW_PARAM_RESTART_COUNT, "2"
    ));

    options.validatePreprocessFlowParameters(testAzProps);
  }


  @Test
  public void testValidatePreprocessFlowParamWithAllowRestartExecutionStopped() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "FAILED",
        FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED, "true"
    ));

    options.validatePreprocessFlowParameters(testAzProps);
    Map<String, String> result = options.getFlowParameters();
    Assert.assertTrue(
        result.get(FLOW_PARAM_ALLOW_RESTART_ON_STATUS).contains("EXECUTION_STOPPED"));
  }

  @Test
  public void testValidatePreprocessFlowParamWithNegativeAllowRestartExecutionStopped() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "FAILED",
        FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED, "false"
    ));

    options.validatePreprocessFlowParameters(testAzProps);
    Map<String, String> result = options.getFlowParameters();
    Assert.assertFalse(
        result.get(FLOW_PARAM_ALLOW_RESTART_ON_STATUS).contains("EXECUTION_STOPPED"));
  }

}
