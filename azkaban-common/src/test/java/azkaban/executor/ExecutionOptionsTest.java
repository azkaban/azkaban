package azkaban.executor;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTION_RESTARTABLE_STATUS;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTION_RESTART_LIMIT;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS;
import static azkaban.Constants.FlowParameters.FLOW_PARAM_RESTART_COUNT;

import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import javax.servlet.ServletException;
import org.junit.Before;
import org.junit.Test;

public class ExecutionOptionsTest {

  public static Props testAzProps;

  @Before
  public void before(){
    testAzProps = new Props();
    testAzProps.put(AZKABAN_EXECUTION_RESTARTABLE_STATUS, "EXECUTION_STOPPED");
    testAzProps.put(AZKABAN_EXECUTION_RESTART_LIMIT, 2);
  }

  @Test
  public void testValidateFlowParamWithoutAnyFlowParameter() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.validate(testAzProps);
  }

  @Test
  public void testValidateFlowParamWithGood_ALLOW_RESTART_ON_STATUS() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED"
    ));

    options.validate(testAzProps);
  }

  @Test
  public void testValidateFlowParamWithDefaultAllowListAndGood_ALLOW_RESTART_ON_STATUS() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED"
    ));

    // if not defined, has default value to [EXECUTION_STOPPED, FAILED]
    options.validate(new Props());
  }

  @Test(expected = ServletException.class)
  public void testValidateFlowParamWithInvalid_ALLOW_RESTART_ON_STATUS() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    // KILLED is not defined
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "KILLED"
    ));

    options.validate(testAzProps);
  }

  @Test
  public void testValidateFlowParamWithGood_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_RESTART_COUNT, "1"
    ));

    options.validate(testAzProps);
  }

  @Test(expected = ServletException.class)
  public void testValidateFlowParamWithNegative_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_RESTART_COUNT, "-11"
    ));

    options.validate(testAzProps);
  }

  @Test(expected = ServletException.class)
  public void testValidateFlowParamWithExceed_RESTART_COUNT() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_RESTART_COUNT, "100000"
    ));

    options.validate(testAzProps);
  }

  @Test
  public void testValidateFlowParamWithAllValidSettings() throws ServletException {
    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED",
        FLOW_PARAM_RESTART_COUNT, "2"
    ));

    options.validate(testAzProps);
  }

}
