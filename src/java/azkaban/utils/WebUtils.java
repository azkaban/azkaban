package azkaban.utils;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.format.DateTimeFormat;

import azkaban.executor.ExecutableFlow.Status;

public class WebUtils {
	public static final String DATE_TIME_STRING = "YYYY-MM-dd HH:mm:ss";
	
	public String formatDate(long timeMS) {
		if (timeMS == -1) {
			return "-";
		}
		
		return DateTimeFormat.forPattern(DATE_TIME_STRING).print(timeMS);
	}
	
	public String formatDuration(long startTime, long endTime) {
		if (startTime == -1) {
			return "-";
		}
		
		long durationMS;
		if (endTime == -1) {
			durationMS = System.currentTimeMillis() - startTime;
		}
		else {
			durationMS = endTime - startTime;
		}
		
		long seconds = durationMS/1000;
		if (seconds < 60) {
			return seconds + " sec";
		}
		
		long minutes = seconds / 60;
		seconds %= 60;
		if (minutes < 60) {
			return minutes + "m " + seconds + "s";
		}
		
		long hours = minutes / 60;
		minutes %= 60;
		if (hours < 24) {
			return hours + "h " + minutes + "m " + seconds + "s";
		}
		
		long days = hours / 24;
		hours %= 24;
		return days + "d " + hours + "h " + minutes + "m";
	}
	
	public String formatStatus(Status status) {
		switch(status) {
		case SUCCEEDED:
			return "Success";
		case FAILED:
			return "Failed";
		case RUNNING:
			return "Running";
		case DISABLED:
			return "Disabled";
		case KILLED:
			return "Killed";
		case FAILED_FINISHING:
			return "Running w/Failure";
		case WAITING:
			return "Waiting";
		case READY:
			return "Ready";
		default:
		}
		
		return "Unknown";
	}
	
	public String formatDateTime(DateTime dt)
	{
		return DateTimeFormat.forPattern(DATE_TIME_STRING).print(dt);
	}
	
	public String formatPeriod(ReadablePeriod period)
	{
        String periodStr = "n";

        if (period == null) {
            return periodStr;
        }

        if (period.get(DurationFieldType.months()) > 0) {
            int months = period.get(DurationFieldType.months());
            periodStr = months + " month(s)";
        }
        else if (period.get(DurationFieldType.weeks()) > 0) {
            int weeks = period.get(DurationFieldType.weeks());
            periodStr = weeks + " week(s)";
        }
        else if (period.get(DurationFieldType.days()) > 0) {
            int days = period.get(DurationFieldType.days());
            periodStr = days + " day(s)";
        }
        else if (period.get(DurationFieldType.hours()) > 0) {
            int hours = period.get(DurationFieldType.hours());
            periodStr = hours + " hour(s)";
        }
        else if (period.get(DurationFieldType.minutes()) > 0) {
            int minutes = period.get(DurationFieldType.minutes());
            periodStr = minutes + " minute(s)";
        }
        else if (period.get(DurationFieldType.seconds()) > 0) {
            int seconds = period.get(DurationFieldType.seconds());
            periodStr = seconds + " second(s)";
        }
        
        return periodStr;
	}
	
	public String extractNumericalId(String execId) {
		int index = execId.indexOf('.');
		int index2 = execId.indexOf('.', index+1);
		
		return execId.substring(0, index2);
	}
}
