package azkaban.test.trigger;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.CheckerTypeLoader;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.ActionTypeLoader;
import azkaban.trigger.TriggerLoader;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerException;
import azkaban.utils.Props;

public class TriggerManagerTest {
	
	@Before
	public void setup() {

	}
	
	@After
	public void tearDown() {
		
	}
	
	@Test
	public void TriggerManagerSimpleTest() {
		Props props = new Props();
		TriggerManager triggerManager = new TriggerManager(props, new MockTriggerLoader(), new MockCheckerLoader(), new MockActionLoader());
		List<Trigger> triggers = triggerManager.getTriggers();
		assertTrue(triggers.size() == 1);
		
		Trigger t2 = createFakeTrigger("addnewtriggger");
		triggerManager.insertTrigger(t2);
		
		triggers = triggerManager.getTriggers();
		assertTrue(triggers.size() == 2);
		
		triggerManager.removeTrigger(t2);
		triggers = triggerManager.getTriggers();
		assertTrue(triggers.size() == 1);
	}
	
	public class MockTriggerLoader implements TriggerLoader {

		private List<Trigger> triggers;
		
		@Override
		public void addTrigger(Trigger t) throws TriggerManagerException {
			triggers.add(t);			
		}

		@Override
		public void removeTrigger(Trigger s) throws TriggerManagerException {
			triggers.remove(s);
			
		}

		@Override
		public void updateTrigger(Trigger t) throws TriggerManagerException {

		}

		@Override
		public List<Trigger> loadTriggers()
				throws TriggerManagerException {
			Trigger t = createFakeTrigger("test");
			triggers = new ArrayList<Trigger>();
			triggers.add(t);
			return triggers;
		}
		
	}
	
	private Trigger createFakeTrigger(String message) {
		
		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		
		List<TriggerAction> actions = new ArrayList<TriggerAction>();
		TriggerAction act  = new DummyTriggerAction(message);
		actions.add(act);
		
		String expr = "true";
		
		Condition triggerCond = new Condition(checkers, expr);
		Condition expireCond = new Condition(checkers, expr);
		
		Trigger fakeTrigger = new Trigger(DateTime.now().getMillis(), DateTime.now().getMillis(), "azkaban", "tester", triggerCond, expireCond, actions);
		
		return fakeTrigger;
	}

	public class MockCheckerLoader extends CheckerTypeLoader{
		
		@Override
		public void init(Props props) {
			checkerToClass.put(ThresholdChecker.type, ThresholdChecker.class);
		}
	}
	
	public class MockActionLoader extends ActionTypeLoader {
		@Override
		public void init(Props props) {
			actionToClass.put(DummyTriggerAction.type, DummyTriggerAction.class);
		}
	}

}
