package azkaban.test.trigger;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import azkaban.trigger.ConditionChecker;


public class FakeTimeChecker implements ConditionChecker{
	
	private DateTime timeToCheck;
	private String message;
	
	public static final String type = "FakeTimeChecker";
	
	public FakeTimeChecker(DateTime timeToCheck){
		this.timeToCheck = timeToCheck;
	}
	
	@Override
	public Boolean eval() {
		return timeToCheck.isAfterNow();
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
		return getType() + "_" +
				timeToCheck.getMillis() + "_" +
				timeToCheck.getZone().getShortName(timeToCheck.getMillis() );
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public ConditionChecker fromJson(Object obj) {
		String str = (String) obj;
		String[] parts = str.split("_");
		
		if(!parts[0].equals(getType())) {
			throw new RuntimeException("Cannot create checker of " + getType() + " from " + parts[0]);
		}
		
		long timeToCheckMillis = Long.parseLong(parts[1]);
		DateTimeZone timezone = DateTimeZone.forID(parts[2]);
		
		return new FakeTimeChecker(new DateTime(timeToCheckMillis, timezone));
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

}
