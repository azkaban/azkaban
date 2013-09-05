package azkaban.test.trigger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;
import org.junit.Test;

import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.utils.Utils;

public class BasicTimeCheckerTest {

	@Test
	public void basicTimerTest(){
		
		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		
		// get a new timechecker, start from now, repeat every minute. should evaluate to false now, and true a minute later.
		DateTime now = DateTime.now();
		ReadablePeriod period = Utils.parsePeriodString("10s");
		
		BasicTimeChecker timeChecker = new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(), now.getZone(), true, true, period);
		checkers.put(timeChecker.getId(), timeChecker);
		String expr = timeChecker.getId() + ".eval()";
		
		Condition cond = new Condition(checkers, expr);
		System.out.println(expr);
		
		assertFalse(cond.isMet());
		
		//sleep for 1 min
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertTrue(cond.isMet());
		
		cond.resetCheckers();
		
		assertFalse(cond.isMet());
		
		//sleep for 1 min
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertTrue(cond.isMet());
		
	}
}
