package azkaban.trigger;

import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.tz.DateTimeZoneBuilder;

public class BasicTimeChecker implements ConditionChecker {

	private DateTime firstCheckTime;
	private DateTime nextCheckTime;
	private boolean isRecurring = true;
	private boolean skipPastChecks = true;
	private ReadablePeriod period;
	private String message;
	
	public static final String type = "BasicTimeChecker";
	
	public BasicTimeChecker(
			DateTime firstCheckTime,
			boolean isRecurring, 
			boolean skipPastChecks,
			ReadablePeriod period) {
		this.firstCheckTime = firstCheckTime;
		this.isRecurring = isRecurring;
		this.skipPastChecks = skipPastChecks;
		this.period = period;
		this.nextCheckTime = new DateTime(firstCheckTime);
		this.nextCheckTime = getNextCheckTime();
	}
	
	public BasicTimeChecker(
			DateTime firstCheckTime,
			Boolean isRecurring, 
			Boolean skipPastChecks,
			String period) {
		this.firstCheckTime = firstCheckTime;
		this.isRecurring = isRecurring;
		this.skipPastChecks = skipPastChecks;
		this.period = parsePeriodString(period);
		this.nextCheckTime = new DateTime(firstCheckTime);
		this.nextCheckTime = getNextCheckTime();
	}
	
	public BasicTimeChecker(
			DateTime firstCheckTime,
			DateTime nextCheckTime,
			boolean isRecurring, 
			boolean skipPastChecks,
			ReadablePeriod period) {
		this.firstCheckTime = firstCheckTime;
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
		this.nextCheckTime = getNextCheckTime();
		
	}
	
	/*
	 * TimeChecker format:
	 * type_first-time-in-millis_next-time-in-millis_timezone_is-recurring_skip-past-checks_period
	 */
	@Override
	public String getId() {
		return getType() + "$" +
				firstCheckTime.getMillis() + "$" +
				nextCheckTime.getMillis() + "$" +
				firstCheckTime.getZone().getID().replace('/', '0') + "$" +
				//"offset"+firstCheckTime.getZone().getOffset(firstCheckTime.getMillis()) + "_" +
				(isRecurring == true ? "1" : "0") + "$" +
				(skipPastChecks == true ? "1" : "0") + "$" +
				createPeriodString(period);
	}

	@Override
	public String getType() {
		return type;
	}

	public static ConditionChecker createFromJson(String obj) {
		String str = (String) obj;
		String[] parts = str.split("\\$");
		
		if(!parts[0].equals(type)) {
			throw new RuntimeException("Cannot create checker of " + type + " from " + parts[0]);
		}
		
		long firstMillis = Long.parseLong(parts[1]);
		long nextMillis = Long.parseLong(parts[2]);
		//DateTimeZone timezone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(parts[3]));
		DateTimeZone timezone = DateTimeZone.forID(parts[3].replace('0', '/'));
		boolean isRecurring = parts[4].equals("1") ? true : false;
		boolean skipPastChecks = parts[5].equals("1") ? true : false;
		ReadablePeriod period = parsePeriodString(parts[6]);
		
		return new BasicTimeChecker(new DateTime(firstMillis, timezone), new DateTime(nextMillis, timezone), isRecurring, skipPastChecks, period);
	}
	
	@Override
	public ConditionChecker fromJson(Object obj) {
		String str = (String) obj;
		String[] parts = str.split("_");
		
		if(!parts[0].equals(getType())) {
			throw new RuntimeException("Cannot create checker of " + getType() + " from " + parts[0]);
		}
		
		long firstMillis = Long.parseLong(parts[1]);
		long nextMillis = Long.parseLong(parts[2]);
		DateTimeZone timezone = DateTimeZone.forID(parts[3]);
		boolean isRecurring = Boolean.valueOf(parts[4]);
		boolean skipPastChecks = Boolean.valueOf(parts[5]);
		ReadablePeriod period = parsePeriodString(parts[6]);
		
		return new BasicTimeChecker(new DateTime(firstMillis, timezone), new DateTime(nextMillis, timezone), isRecurring, skipPastChecks, period);
	}
	
	private DateTime getNextCheckTime(){
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
	
	public static ReadablePeriod parsePeriodString(String periodStr) {
		ReadablePeriod period;
		char periodUnit = periodStr.charAt(periodStr.length() - 1);
		if (periodUnit == 'n') {
			return null;
		}

		int periodInt = Integer.parseInt(periodStr.substring(0,
				periodStr.length() - 1));
		switch (periodUnit) {
		case 'M':
			period = Months.months(periodInt);
			break;
		case 'w':
			period = Weeks.weeks(periodInt);
			break;
		case 'd':
			period = Days.days(periodInt);
			break;
		case 'h':
			period = Hours.hours(periodInt);
			break;
		case 'm':
			period = Minutes.minutes(periodInt);
			break;
		case 's':
			period = Seconds.seconds(periodInt);
			break;
		default:
			throw new IllegalArgumentException("Invalid schedule period unit '"
					+ periodUnit);
		}

		return period;
	}

	public static String createPeriodString(ReadablePeriod period) {
		String periodStr = "n";

		if (period == null) {
			return "n";
		}

		if (period.get(DurationFieldType.months()) > 0) {
			int months = period.get(DurationFieldType.months());
			periodStr = months + "M";
		} else if (period.get(DurationFieldType.weeks()) > 0) {
			int weeks = period.get(DurationFieldType.weeks());
			periodStr = weeks + "w";
		} else if (period.get(DurationFieldType.days()) > 0) {
			int days = period.get(DurationFieldType.days());
			periodStr = days + "d";
		} else if (period.get(DurationFieldType.hours()) > 0) {
			int hours = period.get(DurationFieldType.hours());
			periodStr = hours + "h";
		} else if (period.get(DurationFieldType.minutes()) > 0) {
			int minutes = period.get(DurationFieldType.minutes());
			periodStr = minutes + "m";
		} else if (period.get(DurationFieldType.seconds()) > 0) {
			int seconds = period.get(DurationFieldType.seconds());
			periodStr = seconds + "s";
		}

		return periodStr;
	}

	@Override
	public Object getNum() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object toJson() {
		return getId();
	}

}
