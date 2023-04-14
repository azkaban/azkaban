package azkaban.executor;

import static org.junit.Assert.assertEquals;

import azkaban.Constants.FlowParameters;
import azkaban.executor.ExecutionOptions.FailureAction;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.junit.Test;

public class ExecutionOptionsTest {

  @Test
  public void testMerge() {
    ExecutionOptions options1 = new ExecutionOptions();
    options1.addAllFlowParameters(
        ImmutableMap.of(
            FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "FAILED",
            FlowParameters.FLOW_PARAM_ENABLE_DEV_POD, "true")
    );
    options1.setFailureAction(FailureAction.FINISH_ALL_POSSIBLE);
    options1.setMailCreator("dummy");

    ExecutionOptions overwrite = new ExecutionOptions();
    overwrite.addAllFlowParameters(
        ImmutableMap.of(
            FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED",
            FlowParameters.FLOW_PARAM_MAX_RETRIES, "1")
    );
    overwrite.setFailureAction(FailureAction.CANCEL_ALL);
    overwrite.setMemoryCheck(true);
    overwrite.setSuccessEmails(ImmutableSet.of("abcd@linkedin.com"));
    overwrite.setPipelineExecutionId(123);
    overwrite.setMailCreator("  ");
    overwrite.addAllRuntimeProperties(
        ImmutableMap.of("property1", ImmutableMap.of("key", "value")));

    // do the merge
    options1.merge(overwrite);

    // check
    Map<String, String> expectFlowParams = ImmutableMap.of(
        FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED",
        FlowParameters.FLOW_PARAM_MAX_RETRIES, "1",
        FlowParameters.FLOW_PARAM_ENABLE_DEV_POD, "true");
    Map<String, String> flowParams = options1.getFlowParameters();
    assertEquals(expectFlowParams, flowParams);

    ExecutionOptions expected = ExecutionOptions.createFromObject(overwrite.toObject());
    expected.addAllFlowParameters(expectFlowParams);
    expected.setMailCreator("dummy");
    assertEquals(expected.toJSON(), options1.toJSON());
  }
}
