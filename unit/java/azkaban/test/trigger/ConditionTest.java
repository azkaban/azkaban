package azkaban.test.trigger;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.commons.jexl2.JexlEngine;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;
import org.junit.Test;

import azkaban.trigger.CheckerTypeLoader;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.BasicTimeChecker;
import azkaban.utils.Props;

public class ConditionTest {

	private JexlEngine jexl = new JexlEngine();
	
	@Test
	public void conditionTest(){
		
		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		DateTime now = DateTime.now();
		FakeTimeChecker fake1 = new FakeTimeChecker(now);
		FakeTimeChecker fake2 = new FakeTimeChecker(now.plusMinutes(1));
		checkers.put(fake1.getId(), fake1);
		checkers.put(fake2.getId(), fake2);

		String expr1 = "( " + fake1.getId()+ ".eval()" + " && " + fake2.getId()+ ".eval()" + " )" + " || " + "( " + "!" + fake1.getId()+".eval()" + " && " + fake2.getId()+".eval()" + " )";
		String expr2 = "( " + fake1.getId()+ ".eval()" + " && " + fake2.getId()+ ".eval()" + " )" + " || " + "( " + fake1.getId()+".eval()" + " && " + fake2.getId()+".eval()" + " )";

		Condition cond = new Condition(checkers, expr1);

		System.out.println("Setting expression " + expr1);
		assertTrue(cond.isMet());
		cond.setExpression(expr2);
		System.out.println("Setting expression " + expr2);
		assertFalse(cond.isMet());
		
	}
	
	@Test
	public void timeCheckerTest(){
		
		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		
		// get a new timechecker, start from now, repeat every minute. should evaluate to false now, and true a minute later.
		DateTime now = DateTime.now();
		ReadablePeriod period = BasicTimeChecker.parsePeriodString("10s");
		
		BasicTimeChecker timeChecker = new BasicTimeChecker(now, true, true, period);
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
	
	@Test
	public void BasicTimeCheckerTest() {
		
		CheckerTypeLoader checkerTypeLoader = new CheckerTypeLoader();
		checkerTypeLoader.init(new Props());
		Condition.setCheckerLoader(checkerTypeLoader);

		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		
		// get a new timechecker, start from now, repeat every minute. should evaluate to false now, and true a minute later.
		DateTime now = DateTime.now();
		String period = "6s";
		
		//BasicTimeChecker timeChecker = new BasicTimeChecker(now, true, true, period);
		ConditionChecker timeChecker = checkerTypeLoader.createChecker(BasicTimeChecker.type, now, true, true, period);
		System.out.println("checker id is " + timeChecker.getId());
		
		checkers.put(timeChecker.getId(), timeChecker);
		String expr = timeChecker.getId() + ".eval()";
		
		Condition cond = new Condition(checkers, expr);
		
		Object json = cond.toJson();
		Condition cond2 = Condition.fromJson(json);
		
		Map<String, ConditionChecker> checkers1 = cond.getCheckers();
		Map<String, ConditionChecker> checkers2 = cond2.getCheckers();
		
		assertTrue(cond.getExpression().equals(cond2.getExpression()));
		System.out.println("cond1: " + cond.getExpression());
		System.out.println("cond2: " + cond2.getExpression());
		assertTrue(checkers2.size() == 1);
		ConditionChecker checker2 = checkers2.get(timeChecker.getId());
		//assertTrue(checker2.getId().equals(timeChecker.getId()));
		System.out.println("checker1: " + timeChecker.getId());
		System.out.println("checker2: " + checker2.getId());
		assertTrue(timeChecker.getId().equals(checker2.getId()));
	}
	
}
