/*
 * Copyright 2019 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;


/**
 * Utilities for Time Operations
 */
public class TimeUtils {

  private static final String DATE_TIME_ZONE_PATTERN = "yyyy/MM/dd HH:mm:ss z";
  private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

  /**
   * Formats the given millisecond instant into a string using the pattern "yyyy/MM/dd HH:mm:ss z"
   */
  public static String formatDateTimeZone(final long timestampMs) {
    return format(timestampMs, DATE_TIME_ZONE_PATTERN);
  }

  /**
   * Formats the given millisecond instant into a string using the pattern "yyyy-MM-dd HH:mm:ss"
   */
  public static String formatDateTime(final long timestampMs) {
    return format(timestampMs, DATE_TIME_PATTERN);
  }

  private static String format(final long timestampMs, final String pattern) {
    if (timestampMs < 0) {
      return "-";
    }
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
    final ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampMs),
        ZoneId.systemDefault());
    return formatter.format(zonedDateTime);
  }

  /**
   * Format time period pair to Duration String
   *
   * @param startTime start time
   * @param endTime end time
   * @return Duration String
   */
  public static String formatDuration(final long startTime, final long endTime) {
    if (startTime == -1) {
      return "-";
    }

    final long durationMS;
    if (endTime == -1) {
      durationMS = System.currentTimeMillis() - startTime;
    } else {
      durationMS = endTime - startTime;
    }

    long seconds = durationMS / 1000;
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

    final long days = hours / 24;
    hours %= 24;
    return days + "d " + hours + "h " + minutes + "m";
  }

  /**
   * Format ReadablePeriod object to string
   *
   * @param period readable period object
   * @return String presentation of ReadablePeriod Object
   */
  public static String formatPeriod(final ReadablePeriod period) {
    String periodStr = "null";

    if (period == null) {
      return periodStr;
    }

    if (period.get(DurationFieldType.years()) > 0) {
      final int years = period.get(DurationFieldType.years());
      periodStr = years + " year(s)";
    } else if (period.get(DurationFieldType.months()) > 0) {
      final int months = period.get(DurationFieldType.months());
      periodStr = months + " month(s)";
    } else if (period.get(DurationFieldType.weeks()) > 0) {
      final int weeks = period.get(DurationFieldType.weeks());
      periodStr = weeks + " week(s)";
    } else if (period.get(DurationFieldType.days()) > 0) {
      final int days = period.get(DurationFieldType.days());
      periodStr = days + " day(s)";
    } else if (period.get(DurationFieldType.hours()) > 0) {
      final int hours = period.get(DurationFieldType.hours());
      periodStr = hours + " hour(s)";
    } else if (period.get(DurationFieldType.minutes()) > 0) {
      final int minutes = period.get(DurationFieldType.minutes());
      periodStr = minutes + " minute(s)";
    } else if (period.get(DurationFieldType.seconds()) > 0) {
      final int seconds = period.get(DurationFieldType.seconds());
      periodStr = seconds + " second(s)";
    }

    return periodStr;
  }

  /**
   * Parse Period String to a ReadablePeriod Object
   *
   * @param periodStr string formatted period
   * @return ReadablePeriod Object
   */
  public static ReadablePeriod parsePeriodString(final String periodStr) {
    final ReadablePeriod period;
    final char periodUnit = periodStr.charAt(periodStr.length() - 1);
    if (periodStr.equals("null") || periodUnit == 'n') {
      return null;
    }

    final int periodInt =
        Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
    switch (periodUnit) {
      case 'y':
        period = Years.years(periodInt);
        break;
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

  /**
   * Convert ReadablePeriod Object to string
   *
   * @param period ReadablePeriod Object
   * @return string formatted ReadablePeriod Object
   */
  public static String createPeriodString(final ReadablePeriod period) {
    String periodStr = "null";

    if (period == null) {
      return periodStr;
    }

    if (period.get(DurationFieldType.years()) > 0) {
      final int years = period.get(DurationFieldType.years());
      periodStr = years + "y";
    } else if (period.get(DurationFieldType.months()) > 0) {
      final int months = period.get(DurationFieldType.months());
      periodStr = months + "M";
    } else if (period.get(DurationFieldType.weeks()) > 0) {
      final int weeks = period.get(DurationFieldType.weeks());
      periodStr = weeks + "w";
    } else if (period.get(DurationFieldType.days()) > 0) {
      final int days = period.get(DurationFieldType.days());
      periodStr = days + "d";
    } else if (period.get(DurationFieldType.hours()) > 0) {
      final int hours = period.get(DurationFieldType.hours());
      periodStr = hours + "h";
    } else if (period.get(DurationFieldType.minutes()) > 0) {
      final int minutes = period.get(DurationFieldType.minutes());
      periodStr = minutes + "m";
    } else if (period.get(DurationFieldType.seconds()) > 0) {
      final int seconds = period.get(DurationFieldType.seconds());
      periodStr = seconds + "s";
    }

    return periodStr;
  }
}
