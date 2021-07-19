package azkaban.scheduler;

import azkaban.Constants;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Utility functions to work with cron expressions.
 * Might be removable once org.quartz.CronExpression::getFinalFireTime is implemented.
 *
 * @see org.quartz.CronExpression
 */
public class CronCalculator {

  public static final int MAX_YEAR = getMaxYear();

  private static final Pattern numberPattern = Pattern.compile("\\d+");
  private static final Predicate<String> isNumber = token ->
      numberPattern.matcher(token).matches();
  private static final Predicate<String> isNonStatic = token -> !isNumber.test(token);

  private final String[] tokens;
  private final int secondIndex;
  private final int minuteIndex;
  private final int hourIndex;
  private final int dayOfMonthIndex;
  private final int monthIndex;
  private final int dayOfWeekIndex;
  private final int yearIndex;

  public CronCalculator(String cronExpression) {
    tokens = cronExpression.split(" ");
    if(tokens.length == 5) {
      secondIndex = -1;
      minuteIndex = 0;
      hourIndex = 1;
      dayOfMonthIndex = 2;
      monthIndex = 3;
      dayOfWeekIndex = 4;
      yearIndex = -1;
    } else if(tokens.length == 6) {
      secondIndex = 0;
      minuteIndex = 1;
      hourIndex = 2;
      dayOfMonthIndex = 3;
      monthIndex = 4;
      dayOfWeekIndex = 5;
      yearIndex = -1;
    } else if(tokens.length == 7) {
      secondIndex = 0;
      minuteIndex = 1;
      hourIndex = 2;
      dayOfMonthIndex = 3;
      monthIndex = 4;
      dayOfWeekIndex = 5;
      yearIndex = 6;
    } else {
      throw new IllegalArgumentException("Invalid Cron Expression: " + cronExpression);
    }
  }

  public boolean isStatic() {
    if(IntStream.of(secondIndex, minuteIndex, hourIndex, monthIndex, yearIndex)
        .filter(i->i>=0).mapToObj(i->tokens[i]).anyMatch(isNonStatic)) return false;
    return !IntStream.of(dayOfMonthIndex, dayOfWeekIndex)
        .mapToObj(i->tokens[i]).allMatch(isNonStatic);
  }

  /**
   * Calculate the end time of the cron expression
   * @return last date matching the cron expression
   */
  public Date getUpperBound() {
    if(isUnbounded()) return new Date(Constants.DEFAULT_SCHEDULE_END_EPOCH_TIME);
    int lastYear = getLast(tokens[yearIndex], MAX_YEAR);
    int lastMonth = getLast(tokens[monthIndex], 12);
    // Note: This implementation ignores Day of Week
    int lastDay = getLast(tokens[dayOfMonthIndex], getLastDayOfMonth(lastMonth, lastYear));
    int lastHour = getLast(tokens[hourIndex], 23);
    int lastMinute = getLast(tokens[minuteIndex], 59);
    int lastSecond = secondIndex < 0 ? 59 : getLast(tokens[secondIndex], 59);
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(0);
    calendar.set(lastYear, lastMonth, lastDay, lastHour, lastMinute, lastSecond);
    return calendar.getTime();
  }

  private static int getMaxYear() {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date(Constants.DEFAULT_SCHEDULE_END_EPOCH_TIME));
    return calendar.get(Calendar.YEAR);
  }

  /**
   * (Copied from Quartz)
   * @see org.quartz.CronExpression
   * @param year
   * @return true if leap year
   */
  private boolean isLeapYear(int year) {
    return ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0));
  }

  /**
   * (Copied from Quartz)
   * @see org.quartz.CronExpression
   * @param monthNum
   * @param year
   * @return last day of month
   */
  private int getLastDayOfMonth(int monthNum, int year) {
    switch (monthNum) {
      case 1:
        return 31;
      case 2:
        return (isLeapYear(year)) ? 29 : 28;
      case 3:
        return 31;
      case 4:
        return 30;
      case 5:
        return 31;
      case 6:
        return 30;
      case 7:
        return 31;
      case 8:
        return 31;
      case 9:
        return 30;
      case 10:
        return 31;
      case 11:
        return 30;
      case 12:
        return 31;
      default:
        throw new IllegalArgumentException("Illegal month number: "
            + monthNum);
    }
  }

  private int getLast(String token, int maxValue) {
    if(token.equals("*")) return maxValue;
    if(isNumber.test(token)) return Integer.parseInt(token);
    if(token.contains("-")) {
      String[] split = token.split("\\-");
      if(split.length != 2) throw new IllegalStateException("Invalid range expression: " + token);
      return Integer.parseInt(split[1]);
    }
    if(token.contains(",")) {
      return Arrays.stream(token.split(",")).mapToInt(Integer::parseInt).max().getAsInt();
    }
    throw new IllegalStateException("Cannot compute max value: " + token);
  }

  /**
   * Does cron have a last execution time?
   * @return true if there is no end
   */
  public boolean isUnbounded() {
    if (yearIndex >= 0) {
      if (tokens[yearIndex].equals("*")) {
        return true;
      }
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return Arrays.stream(tokens).reduce((a,b)->a+" "+b).get();
  }
}
