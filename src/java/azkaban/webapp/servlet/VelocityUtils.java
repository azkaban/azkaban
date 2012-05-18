package azkaban.webapp.servlet;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class VelocityUtils {
    public String formatDate(long timestamp) {
        return formatDate(timestamp, "yyyy-MM-dd HH:mm:ss");
    }
	
    public String formatDate(DateTime date) {
        return formatDate(date, "yyyy-MM-dd HH:mm:ss");
    }
    
    public String formatDate(long timestamp, String format) {
        DateTimeFormatter f = DateTimeFormat.forPattern(format);
        return f.print(timestamp);
    }
    
    public String formatDate(DateTime date, String format) {
        DateTimeFormatter f = DateTimeFormat.forPattern(format);
        return f.print(date);
    }
}