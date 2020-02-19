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
package azkaban.scheduler;

import azkaban.executor.ExecutionOptions;
import azkaban.utils.Pair;
import azkaban.utils.TimeUtils;
import azkaban.utils.Utils;
import java.util.Date;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.quartz.CronExpression;


public class Schedule {

  private final int projectId;
  private final String projectName;
  private final String flowName;
  private final long firstSchedTime;
  private final long endSchedTime;
  private final DateTimeZone timezone;
  private final long lastModifyTime;
  private final ReadablePeriod period;
  private final String submitUser;
  private final String status;
  private final long submitTime;
  private final String cronExpression;
  private final boolean skipPastOccurrences = true;
  private int scheduleId;
  private long nextExecTime;
  private ExecutionOptions executionOptions;

  public Schedule(final int scheduleId,
      final int projectId,
      final String projectName,
      final String flowName,
      final String status,
      final long firstSchedTime,
      final long endSchedTime,
      final DateTimeZone timezone,
      final ReadablePeriod period,
      final long lastModifyTime,
      final long nextExecTime,
      final long submitTime,
      final String submitUser,
      final ExecutionOptions executionOptions,
      final String cronExpression) {
    this.scheduleId = scheduleId;
    this.projectId = projectId;
    this.projectName = projectName;
    this.flowName = flowName;
    this.firstSchedTime = firstSchedTime;
    this.endSchedTime = endSchedTime;
    this.timezone = timezone;
    this.lastModifyTime = lastModifyTime;
    this.period = period;
    this.nextExecTime = nextExecTime;
    this.submitUser = submitUser;
    this.status = status;
    this.submitTime = submitTime;
    this.executionOptions = executionOptions;
    this.cronExpression = cronExpression;
  }

  public ExecutionOptions getExecutionOptions() {
    return this.executionOptions;
  }

  public void setFlowOptions(final ExecutionOptions executionOptions) {
    this.executionOptions = executionOptions;
  }

  public String getScheduleName() {
    return this.projectName + "." + this.flowName + " (" + this.projectId + ")";
  }

  @Override
  public String toString() {

    final String underlying =
        this.projectName + "." + this.flowName + " (" + this.projectId + ")"
            + " to be run at (starting) " + new DateTime(
            this.firstSchedTime).toDateTimeISO();
    if (this.period == null && this.cronExpression == null) {
      return underlying + " non-recurring";
    } else if (this.cronExpression != null) {
      return underlying + " with CronExpression {" + this.cronExpression + "} and timezone "
          + timezone.getID();
    } else {
      return underlying + " with precurring period of " + TimeUtils.createPeriodString(this.period);
    }
  }

  public Pair<Integer, String> getScheduleIdentityPair() {
    return new Pair<>(getProjectId(), getFlowName());
  }

  public int getScheduleId() {
    return this.scheduleId;
  }

  public void setScheduleId(final int scheduleId) {
    this.scheduleId = scheduleId;
  }

  public int getProjectId() {
    return this.projectId;
  }

  public String getProjectName() {
    return this.projectName;
  }

  public String getFlowName() {
    return this.flowName;
  }

  public long getFirstSchedTime() {
    return this.firstSchedTime;
  }

  public DateTimeZone getTimezone() {
    return this.timezone;
  }

  public long getLastModifyTime() {
    return this.lastModifyTime;
  }

  public ReadablePeriod getPeriod() {
    return this.period;
  }

  public long getNextExecTime() {
    return this.nextExecTime;
  }

  public void setNextExecTime(final long nextExecTime) {
    this.nextExecTime = nextExecTime;
  }

  public String getSubmitUser() {
    return this.submitUser;
  }

  public String getStatus() {
    return this.status;
  }

  public long getSubmitTime() {
    return this.submitTime;
  }

  public String getCronExpression() {
    return this.cronExpression;
  }

  public boolean updateTime() {
    if (new DateTime(this.nextExecTime).isAfterNow()) {
      return true;
    }

    if (this.cronExpression != null) {
      final DateTime nextTime = getNextCronRuntime(
          this.nextExecTime, this.timezone, Utils.parseCronExpression(this.cronExpression,
              this.timezone));
      this.nextExecTime = nextTime.getMillis();
      return true;
    }

    if (this.period != null) {
      final DateTime nextTime = getNextRuntime(this.nextExecTime, this.timezone, this.period);

      this.nextExecTime = nextTime.getMillis();
      return true;
    }

    return false;
  }

  private DateTime getNextRuntime(final long scheduleTime, final DateTimeZone timezone,
      final ReadablePeriod period) {
    final DateTime now = new DateTime();
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

  /**
   * @param scheduleTime represents the time when Schedule Servlet receives the Cron Schedule API
   * call.
   * @param timezone is always UTC (after 3.1.0)
   * @return the First Scheduled DateTime to run this flow.
   */
  private DateTime getNextCronRuntime(final long scheduleTime, final DateTimeZone timezone,
      final CronExpression ce) {

    Date date = new DateTime(scheduleTime).withZone(timezone).toDate();
    if (ce != null) {
      date = ce.getNextValidTimeAfter(date);
    }
    return new DateTime(date);
  }

  public boolean isRecurring() {
    if (this.period != null) {
      return true;
    }
    if (this.cronExpression != null) {
      return !new CronCalculator(this.cronExpression).isStatic();
    }
    return false;
  }

  public boolean skipPastOccurrences() {
    return this.skipPastOccurrences;
  }

  public long getEndSchedTime() {
    return this.endSchedTime;
  }
}
