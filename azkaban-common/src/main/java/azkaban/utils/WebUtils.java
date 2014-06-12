/*
 * Copyright 2012 LinkedIn Corp.
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

import java.text.NumberFormat;

import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;

import azkaban.executor.Status;

public class WebUtils {
  public static final String DATE_TIME_STRING = "YYYY-MM-dd HH:mm:ss";

  private static final long ONE_KB = 1024;
  private static final long ONE_MB = 1024 * ONE_KB;
  private static final long ONE_GB = 1024 * ONE_MB;
  private static final long ONE_TB = 1024 * ONE_GB;

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

    long days = hours / 24;
    hours %= 24;
    return days + "d " + hours + "h " + minutes + "m";
  }

  public String formatStatus(Status status) {
    switch (status) {
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
    case PREPARING:
      return "Preparing";
    case READY:
      return "Ready";
    case PAUSED:
      return "Paused";
    case SKIPPED:
      return "Skipped";
    default:
    }
    return "Unknown";
  }

  public String formatDateTime(DateTime dt) {
    return DateTimeFormat.forPattern(DATE_TIME_STRING).print(dt);
  }

  public String formatDateTime(long timestamp) {
    return formatDateTime(new DateTime(timestamp));
  }

  public String formatPeriod(ReadablePeriod period) {
    String periodStr = "null";

    if (period == null) {
      return periodStr;
    }

    if (period.get(DurationFieldType.years()) > 0) {
      int years = period.get(DurationFieldType.years());
      periodStr = years + " year(s)";
    } else if (period.get(DurationFieldType.months()) > 0) {
      int months = period.get(DurationFieldType.months());
      periodStr = months + " month(s)";
    } else if (period.get(DurationFieldType.weeks()) > 0) {
      int weeks = period.get(DurationFieldType.weeks());
      periodStr = weeks + " week(s)";
    } else if (period.get(DurationFieldType.days()) > 0) {
      int days = period.get(DurationFieldType.days());
      periodStr = days + " day(s)";
    } else if (period.get(DurationFieldType.hours()) > 0) {
      int hours = period.get(DurationFieldType.hours());
      periodStr = hours + " hour(s)";
    } else if (period.get(DurationFieldType.minutes()) > 0) {
      int minutes = period.get(DurationFieldType.minutes());
      periodStr = minutes + " minute(s)";
    } else if (period.get(DurationFieldType.seconds()) > 0) {
      int seconds = period.get(DurationFieldType.seconds());
      periodStr = seconds + " second(s)";
    }

    return periodStr;
  }

  public String extractNumericalId(String execId) {
    int index = execId.indexOf('.');
    int index2 = execId.indexOf('.', index + 1);

    return execId.substring(0, index2);
  }

  public String displayBytes(long sizeBytes) {
    NumberFormat nf = NumberFormat.getInstance();
    nf.setMaximumFractionDigits(2);
    if (sizeBytes >= ONE_TB)
      return nf.format(sizeBytes / (double) ONE_TB) + " tb";
    else if (sizeBytes >= ONE_GB)
      return nf.format(sizeBytes / (double) ONE_GB) + " gb";
    else if (sizeBytes >= ONE_MB)
      return nf.format(sizeBytes / (double) ONE_MB) + " mb";
    else if (sizeBytes >= ONE_KB)
      return nf.format(sizeBytes / (double) ONE_KB) + " kb";
    else
      return sizeBytes + " B";
  }
}
