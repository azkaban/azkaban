package azkaban.test.trigger;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import azkaban.executor.ExecutionOptions;
import azkaban.trigger.ActionTypeLoader;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Props;


public class ExecuteFlowActionTest {
	
	@Test
	public void jsonConversionTest() throws Exception {
		ActionTypeLoader loader = new ActionTypeLoader();
		loader.init(new Props());
		
		ExecutionOptions options = new ExecutionOptions();
		List<Object> disabledJobs = new ArrayList<Object>();
		options.setDisabledJobs(disabledJobs);
		
		ExecuteFlowAction executeFlowAction = new ExecuteFlowAction("ExecuteFlowAction", 1, "testproject", "testflow", "azkaban", options, null);
		
		Object obj = executeFlowAction.toJson();
		
		ExecuteFlowAction action = (ExecuteFlowAction) loader.createActionFromJson(ExecuteFlowAction.type, obj);
		assertTrue(executeFlowAction.getProjectId() == action.getProjectId());
		assertTrue(executeFlowAction.getFlowName().equals(action.getFlowName()));
		assertTrue(executeFlowAction.getSubmitUser().equals(action.getSubmitUser()));
	}

	
	
}
