package azkaban.test.trigger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import azkaban.executor.ExecutionOptions;
import azkaban.trigger.ActionTypeLoader;
import azkaban.trigger.CheckerTypeLoader;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerException;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class TriggerTest {
	
	private CheckerTypeLoader checkerLoader;
	private ActionTypeLoader actionLoader;
	
	@Before
	public void setup() throws TriggerException {
		checkerLoader = new CheckerTypeLoader();
		checkerLoader.init(new Props());
		Condition.setCheckerLoader(checkerLoader);
		actionLoader = new ActionTypeLoader();
		actionLoader.init(new Props());
		Trigger.setActionTypeLoader(actionLoader);
	}
	
	@Test
	public void jsonConversionTest() throws Exception {
		DateTime now = DateTime.now();
		ConditionChecker checker1 = new BasicTimeChecker("timeChecker1", now.getMillis(), now.getZone(), true, true, Utils.parsePeriodString("1h"));
		Map<String, ConditionChecker> checkers1 = new HashMap<String, ConditionChecker>();
		checkers1.put(checker1.getId(), checker1);
		String expr1 = checker1.getId() + ".eval()";
		Condition triggerCond = new Condition(checkers1, expr1);
		Condition expireCond = new Condition(checkers1, expr1);
		List<TriggerAction> actions = new ArrayList<TriggerAction>();
		TriggerAction action = new ExecuteFlowAction("executeAction", 1, "testProj", "testFlow", "azkaban", new ExecutionOptions(), null);
		actions.add(action);
		Trigger t = new Trigger(now.getMillis(), now.getMillis(), "azkaban", "test", triggerCond, expireCond, actions);
		
		File temp = File.createTempFile("temptest", "temptest");
		temp.deleteOnExit();
		Object obj = t.toJson();
		JSONUtils.toJSON(obj, temp);
		
		Trigger t2 = Trigger.fromJson(JSONUtils.parseJSONFromFile(temp));
		
		assertTrue(t.getSource().equals(t2.getSource()));
		assertTrue(t.getTriggerId() == t2.getTriggerId());
		
	}

}
