package azkaban.test.trigger;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import azkaban.trigger.ConditionChecker;


public class ThresholdChecker implements ConditionChecker{
	
	private int threshold = -1; 
	
	private static int curVal = -1;
	
	public static final String type = "ThresholdChecker";
	
	private String id;
	
	public ThresholdChecker(String id, int threshold){
		this.id = id;
		this.threshold = threshold;
	}
	
	public synchronized static void setVal(int val) {
		curVal = val;
	}
	
	@Override
	public Boolean eval() {
		return curVal > threshold;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}
	
	/*
	 * TimeChecker format:
	 * type_first-time-in-millis_next-time-in-millis_timezone_is-recurring_skip-past-checks_period
	 */
	@Override
	public String getId() {
		return id;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public ConditionChecker fromJson(Object obj) {
		return null;
	}

	@Override
	public Object getNum() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object toJson() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setId(String id) {
		this.id = id;
		
	}

}
