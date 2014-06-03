/*
 * Copyright 2012 LinkedIn, Inc
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

package azkaban.migration.scheduler;

import java.util.HashMap;
import java.util.Map;

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

import azkaban.executor.ExecutionOptions;
import azkaban.migration.sla.SlaOptions;
import azkaban.utils.Pair;

@Deprecated
public class Schedule {

  // private long projectGuid;
  // private long flowGuid;

  // private String scheduleId;

  private int projectId;
  private String projectName;
  private String flowName;
  private long firstSchedTime;
  private DateTimeZone timezone;
  private long lastModifyTime;
  private ReadablePeriod period;
  private long nextExecTime;
  private String submitUser;
  private String status;
  private long submitTime;

  private ExecutionOptions executionOptions;
  private SlaOptions slaOptions;

  public Schedule(int projectId, String projectName, String flowName,
      String status, long firstSchedTime, DateTimeZone timezone,
      ReadablePeriod period, long lastModifyTime, long nextExecTime,
      long submitTime, String submitUser) {
    this.projectId = projectId;
    this.projectName = projectName;
    this.flowName = flowName;
    this.firstSchedTime = firstSchedTime;
    this.timezone = timezone;
    this.lastModifyTime = lastModifyTime;
    this.period = period;
    this.nextExecTime = nextExecTime;
    this.submitUser = submitUser;
    this.status = status;
    this.submitTime = submitTime;
    this.executionOptions = null;
    this.slaOptions = null;
  }

  public Schedule(int projectId, String projectName, String flowName,
      String status, long firstSchedTime, String timezoneId, String period,
      long lastModifyTime, long nextExecTime, long submitTime,
      String submitUser, ExecutionOptions executionOptions,
      SlaOptions slaOptions) {
    this.projectId = projectId;
    this.projectName = projectName;
    this.flowName = flowName;
    this.firstSchedTime = firstSchedTime;
    this.timezone = DateTimeZone.forID(timezoneId);
    this.lastModifyTime = lastModifyTime;
    this.period = parsePeriodString(period);
    this.nextExecTime = nextExecTime;
    this.submitUser = submitUser;
    this.status = status;
    this.submitTime = submitTime;
    this.executionOptions = executionOptions;
    this.slaOptions = slaOptions;
  }

  public Schedule(int projectId, String projectName, String flowName,
      String status, long firstSchedTime, DateTimeZone timezone,
      ReadablePeriod period, long lastModifyTime, long nextExecTime,
      long submitTime, String submitUser, ExecutionOptions executionOptions,
      SlaOptions slaOptions) {
    this.projectId = projectId;
    this.projectName = projectName;
    this.flowName = flowName;
    this.firstSchedTime = firstSchedTime;
    this.timezone = timezone;
    this.lastModifyTime = lastModifyTime;
    this.period = period;
    this.nextExecTime = nextExecTime;
    this.submitUser = submitUser;
    this.status = status;
    this.submitTime = submitTime;
    this.executionOptions = executionOptions;
    this.slaOptions = slaOptions;
  }

  public ExecutionOptions getExecutionOptions() {
    return executionOptions;
  }

  public void setFlowOptions(ExecutionOptions executionOptions) {
    this.executionOptions = executionOptions;
  }

  public SlaOptions getSlaOptions() {
    return slaOptions;
  }

  public void setSlaOptions(SlaOptions slaOptions) {
    this.slaOptions = slaOptions;
  }

  public String getScheduleName() {
    return projectName + "." + flowName + " (" + projectId + ")";
  }

  public String toString() {
    return projectName + "." + flowName + " (" + projectId + ")"
        + " to be run at (starting) "
        + new DateTime(firstSchedTime).toDateTimeISO()
        + " with recurring period of "
        + (period == null ? "non-recurring" : createPeriodString(period));
  }

  public Pair<Integer, String> getScheduleId() {
    return new Pair<Integer, String>(getProjectId(), getFlowName());
  }

  public int getProjectId() {
    return projectId;
  }

  public String getProjectName() {
    return projectName;
  }

  public String getFlowName() {
    return flowName;
  }

  public long getFirstSchedTime() {
    return firstSchedTime;
  }

  public DateTimeZone getTimezone() {
    return timezone;
  }

  public long getLastModifyTime() {
    return lastModifyTime;
  }

  public ReadablePeriod getPeriod() {
    return period;
  }

  public long getNextExecTime() {
    return nextExecTime;
  }

  public String getSubmitUser() {
    return submitUser;
  }

  public String getStatus() {
    return status;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public boolean updateTime() {
    if (new DateTime(nextExecTime).isAfterNow()) {
      return true;
    }

    if (period != null) {
      DateTime nextTime = getNextRuntime(nextExecTime, timezone, period);

      this.nextExecTime = nextTime.getMillis();
      return true;
    }

    return false;
  }

  private DateTime getNextRuntime(long scheduleTime, DateTimeZone timezone,
      ReadablePeriod period) {
    DateTime now = new DateTime();
    DateTime date = new DateTime(scheduleTime).withZone(timezone);
    int count = 0;
    while (!now.isBefore(date)) {
      if (count > 100000) {
        throw new IllegalStateException(
            "100000 increments of period did not get to present time.");
      }

      if (period == null) {
        break;
      } else {
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

    int periodInt =
        Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
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

  public Map<String, Object> optionsToObject() {
    if (executionOptions != null || slaOptions != null) {
      HashMap<String, Object> schedObj = new HashMap<String, Object>();

      if (executionOptions != null) {
        schedObj.put("executionOptions", executionOptions.toObject());
      }
      if (slaOptions != null) {
        schedObj.put("slaOptions", slaOptions.toObject());
      }

      return schedObj;
    }
    return null;
  }

  public void createAndSetScheduleOptions(Object obj) {
    @SuppressWarnings("unchecked")
    HashMap<String, Object> schedObj = (HashMap<String, Object>) obj;
    if (schedObj.containsKey("executionOptions")) {
      ExecutionOptions execOptions =
          ExecutionOptions.createFromObject(schedObj.get("executionOptions"));
      this.executionOptions = execOptions;
    } else if (schedObj.containsKey("flowOptions")) {
      ExecutionOptions execOptions =
          ExecutionOptions.createFromObject(schedObj.get("flowOptions"));
      this.executionOptions = execOptions;
      execOptions.setConcurrentOption(ExecutionOptions.CONCURRENT_OPTION_SKIP);
    } else {
      this.executionOptions = new ExecutionOptions();
      this.executionOptions
          .setConcurrentOption(ExecutionOptions.CONCURRENT_OPTION_SKIP);
    }

    if (schedObj.containsKey("slaOptions")) {
      SlaOptions slaOptions = SlaOptions.fromObject(schedObj.get("slaOptions"));
      this.slaOptions = slaOptions;
    }
  }
}
