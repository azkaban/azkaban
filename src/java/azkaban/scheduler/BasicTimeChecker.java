package azkaban.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;

import azkaban.trigger.ConditionChecker;
import azkaban.utils.Utils;

public class BasicTimeChecker implements ConditionChecker {

	public static final String type = "BasicTimeChecker";
	
	private DateTime firstCheckTime;
	private DateTime nextCheckTime;
	private DateTimeZone timezone;
	private boolean isRecurring = true;
	private boolean skipPastChecks = true;
	private ReadablePeriod period;
	
	private final String id; 
	
	public BasicTimeChecker(
			String id,
			DateTime firstCheckTime,
			DateTimeZone timezone,
			boolean isRecurring, 
			boolean skipPastChecks,
			ReadablePeriod period) {
		this.id = id;
		this.firstCheckTime = firstCheckTime;
		this.timezone = timezone;
		this.isRecurring = isRecurring;
		this.skipPastChecks = skipPastChecks;
		this.period = period;
		this.nextCheckTime = new DateTime(firstCheckTime);
		this.nextCheckTime = calculateNextCheckTime();
	}
	
	public DateTime getFirstCheckTime() {
		return firstCheckTime;
	}

	public boolean isRecurring() {
		return isRecurring;
	}

	public boolean isSkipPastChecks() {
		return skipPastChecks;
	}

	public ReadablePeriod getPeriod() {
		return period;
	}

	public DateTime getNextCheckTime() {
		return nextCheckTime;
	}
	
//	public BasicTimeChecker(
//			DateTime firstCheckTime,
//			Boolean isRecurring, 
//			Boolean skipPastChecks,
//			String period) {
//		this.firstCheckTime = firstCheckTime;
//		this.isRecurring = isRecurring;
//		this.skipPastChecks = skipPastChecks;
//		this.period = parsePeriodString(period);
//		this.nextCheckTime = new DateTime(firstCheckTime);
//		this.nextCheckTime = calculateNextCheckTime();
//	}
	
	public BasicTimeChecker(
			String id,
			DateTime firstCheckTime,
			DateTimeZone timezone,
			DateTime nextCheckTime,
			boolean isRecurring, 
			boolean skipPastChecks,
			ReadablePeriod period) {
		this.id = id;
		this.firstCheckTime = firstCheckTime;
		this.timezone = timezone;
		this.nextCheckTime = nextCheckTime;
		this.isRecurring = isRecurring;
		this.skipPastChecks = skipPastChecks;
		this.period = period;
	}
	
	@Override
	public Boolean eval() {
		return nextCheckTime.isBeforeNow();
	}

	@Override
	public void reset() {
		this.nextCheckTime = calculateNextCheckTime();
		
	}
	
	/*
	 * TimeChecker format:
	 * type_first-time-in-millis_next-time-in-millis_timezone_is-recurring_skip-past-checks_period
	 */
	@Override
	public String getId() {
//		return getType() + "$" +
//				firstCheckTime.getMillis() + "$" +
//				nextCheckTime.getMillis() + "$" +
//				firstCheckTime.getZone().getID().replace('/', '0') + "$" +
//				//"offset"+firstCheckTime.getZone().getOffset(firstCheckTime.getMillis()) + "_" +
//				(isRecurring == true ? "1" : "0") + "$" +
//				(skipPastChecks == true ? "1" : "0") + "$" +
//				createPeriodString(period);
		return id;
	}

	@Override
	public String getType() {
		return type;
	}

	@SuppressWarnings("unchecked")
	public static ConditionChecker createFromJson(Object obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new Exception("Cannot create checker of " + type + " from " + jsonObj.get("type"));
		}
		long firstTimeMillis = Long.valueOf((String)jsonObj.get("firstCheckTime"));
		String timezoneId = (String) jsonObj.get("timezone");
		long nextTimeMillis = Long.valueOf((String)jsonObj.get("nextCheckTime"));
		DateTimeZone timezone = DateTimeZone.forID(timezoneId);
		DateTime firstCheckTime = new DateTime(firstTimeMillis).withZone(timezone);
		DateTime nextCheckTime = new DateTime(nextTimeMillis).withZone(timezone);
		boolean isRecurring = Boolean.valueOf((String)jsonObj.get("isRecurring"));
		boolean skipPastChecks = Boolean.valueOf((String)jsonObj.get("skipPastChecks"));
		ReadablePeriod period = Utils.parsePeriodString((String)jsonObj.get("period"));
		String id = (String) jsonObj.get("id");
		return new BasicTimeChecker(id, firstCheckTime, timezone, nextCheckTime, isRecurring, skipPastChecks, period);
	}
	
	public static ConditionChecker createFromJson(HashMap<String, Object> obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new Exception("Cannot create checker of " + type + " from " + jsonObj.get("type"));
		}
		Long firstTimeMillis = Long.valueOf((String) jsonObj.get("firstCheckTime"));
		String timezoneId = (String) jsonObj.get("timezone");
		long nextTimeMillis = Long.valueOf((String) jsonObj.get("nextCheckTime"));
		DateTimeZone timezone = DateTimeZone.forID(timezoneId);
		DateTime firstCheckTime = new DateTime(firstTimeMillis).withZone(timezone);
		DateTime nextCheckTime = new DateTime(nextTimeMillis).withZone(timezone);
		boolean isRecurring = Boolean.valueOf((String)jsonObj.get("isRecurring"));
		boolean skipPastChecks = Boolean.valueOf((String)jsonObj.get("skipPastChecks"));
		ReadablePeriod period = Utils.parsePeriodString((String)jsonObj.get("period"));
		String id = (String) jsonObj.get("id");
		return new BasicTimeChecker(id, firstCheckTime, timezone, nextCheckTime, isRecurring, skipPastChecks, period);
	}
	
	@Override
	public ConditionChecker fromJson(Object obj) throws Exception{
		return createFromJson(obj);
	}
	
//	public static ConditionChecker createFromJson(String obj) {
//		String str = (String) obj;
//		String[] parts = str.split("\\$");
//		
//		if(!parts[0].equals(type)) {
//			throw new RuntimeException("Cannot create checker of " + type + " from " + parts[0]);
//		}
//		
//		long firstMillis = Long.parseLong(parts[1]);
//		long nextMillis = Long.parseLong(parts[2]);
//		//DateTimeZone timezone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(parts[3]));
//		DateTimeZone timezone = DateTimeZone.forID(parts[3].replace('0', '/'));
//		boolean isRecurring = parts[4].equals("1") ? true : false;
//		boolean skipPastChecks = parts[5].equals("1") ? true : false;
//		ReadablePeriod period = parsePeriodString(parts[6]);
//		
//		return new BasicTimeChecker(new DateTime(firstMillis, timezone), new DateTime(nextMillis, timezone), isRecurring, skipPastChecks, period);
//	}
//	
//	@Override
//	public ConditionChecker fromJson(Object obj) {
//		String str = (String) obj;
//		String[] parts = str.split("_");
//		
//		if(!parts[0].equals(getType())) {
//			throw new RuntimeException("Cannot create checker of " + getType() + " from " + parts[0]);
//		}
//		
//		long firstMillis = Long.parseLong(parts[1]);
//		long nextMillis = Long.parseLong(parts[2]);
//		DateTimeZone timezone = DateTimeZone.forID(parts[3]);
//		boolean isRecurring = Boolean.valueOf(parts[4]);
//		boolean skipPastChecks = Boolean.valueOf(parts[5]);
//		ReadablePeriod period = parsePeriodString(parts[6]);
//		
//		return new BasicTimeChecker(new DateTime(firstMillis, timezone), new DateTime(nextMillis, timezone), isRecurring, skipPastChecks, period);
//	}
	
	private DateTime calculateNextCheckTime(){
		DateTime date = new DateTime(nextCheckTime);
		int count = 0;
		while(!DateTime.now().isBefore(date) && skipPastChecks) {
			if(count > 100000) {
				throw new IllegalStateException("100000 increments of period did not get to present time.");
			}
			
			if(period == null) {
				break;
			}else {
				date = date.plus(period);
			}
			count += 1;
		}
		return date;
	}
	
	@Override
	public Object getNum() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("type", type);
		jsonObj.put("firstCheckTime", String.valueOf(firstCheckTime.getMillis()));
		jsonObj.put("timezone", timezone.getID());
		jsonObj.put("nextCheckTime", String.valueOf(nextCheckTime.getMillis()));
		jsonObj.put("isRecurrint", String.valueOf(isRecurring));
		jsonObj.put("skipPastChecks", String.valueOf(skipPastChecks));
		jsonObj.put("period", Utils.createPeriodString(period));
		jsonObj.put("id", id);
		
		return jsonObj;
	}


}
