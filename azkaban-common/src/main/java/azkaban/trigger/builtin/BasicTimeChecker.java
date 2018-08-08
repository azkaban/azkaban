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

package azkaban.trigger.builtin;

import azkaban.trigger.ConditionChecker;
import azkaban.utils.Utils;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.quartz.CronExpression;

public class BasicTimeChecker implements ConditionChecker {

  public static final String type = "BasicTimeChecker";
  private final String id;
  private final long firstCheckTime;
  private final DateTimeZone timezone;
  private final ReadablePeriod period;
  private final String cronExpression;
  private final CronExpression cronExecutionTime;
  private long nextCheckTime;
  private boolean isRecurring = true;
  private boolean skipPastChecks = true;

  public BasicTimeChecker(final String id, final long firstCheckTime,
      final DateTimeZone timezone, final boolean isRecurring, final boolean skipPastChecks,
      final ReadablePeriod period, final String cronExpression) {
    this.id = id;
    this.firstCheckTime = firstCheckTime;
    this.timezone = timezone;
    this.isRecurring = isRecurring;
    this.skipPastChecks = skipPastChecks;
    this.period = period;
    this.nextCheckTime = firstCheckTime;
    this.cronExpression = cronExpression;
    this.cronExecutionTime = Utils.parseCronExpression(cronExpression, timezone);
    this.nextCheckTime = calculateNextCheckTime();
  }

  public BasicTimeChecker(final String id, final long firstCheckTime,
      final DateTimeZone timezone, final long nextCheckTime, final boolean isRecurring,
      final boolean skipPastChecks, final ReadablePeriod period, final String cronExpression) {
    this.id = id;
    this.firstCheckTime = firstCheckTime;
    this.timezone = timezone;
    this.nextCheckTime = nextCheckTime;
    this.isRecurring = isRecurring;
    this.skipPastChecks = skipPastChecks;
    this.period = period;
    this.cronExpression = cronExpression;
    this.cronExecutionTime = Utils.parseCronExpression(cronExpression, timezone);
  }

  public static BasicTimeChecker createFromJson(final Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  public static BasicTimeChecker createFromJson(final HashMap<String, Object> obj)
      throws Exception {
    final Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from "
          + jsonObj.get("type"));
    }
    final Long firstCheckTime = Long.valueOf((String) jsonObj.get("firstCheckTime"));
    final String timezoneId = (String) jsonObj.get("timezone");
    final long nextCheckTime = Long.valueOf((String) jsonObj.get("nextCheckTime"));
    final DateTimeZone timezone = DateTimeZone.forID(timezoneId);
    final boolean isRecurring = Boolean.valueOf((String) jsonObj.get("isRecurring"));
    final boolean skipPastChecks =
        Boolean.valueOf((String) jsonObj.get("skipPastChecks"));
    final ReadablePeriod period =
        Utils.parsePeriodString((String) jsonObj.get("period"));
    final String id = (String) jsonObj.get("id");
    final String cronExpression = (String) jsonObj.get("cronExpression");

    final BasicTimeChecker checker =
        new BasicTimeChecker(id, firstCheckTime, timezone, nextCheckTime,
            isRecurring, skipPastChecks, period, cronExpression);
    if (skipPastChecks) {
      checker.updateNextCheckTime();
    }
    return checker;
  }

  public long getFirstCheckTime() {
    return this.firstCheckTime;
  }

  public DateTimeZone getTimeZone() {
    return this.timezone;
  }

  public boolean isRecurring() {
    return this.isRecurring;
  }

  public boolean isSkipPastChecks() {
    return this.skipPastChecks;
  }

  public ReadablePeriod getPeriod() {
    return this.period;
  }

  @Override
  public long getNextCheckTime() {
    return this.nextCheckTime;
  }

  public String getCronExpression() {
    return this.cronExpression;
  }

  @Override
  public Boolean eval() {
    return this.nextCheckTime < DateTimeUtils.currentTimeMillis();
  }

  @Override
  public void reset() {
    this.nextCheckTime = calculateNextCheckTime();
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public BasicTimeChecker fromJson(final Object obj) throws Exception {
    return createFromJson(obj);
  }

  private void updateNextCheckTime() {
    this.nextCheckTime = calculateNextCheckTime();
  }

  private long calculateNextCheckTime() {
    DateTime date = new DateTime(this.nextCheckTime).withZone(this.timezone);
    int count = 0;
    while (!date.isAfterNow()) {
      if (count > 100000) {
        throw new IllegalStateException(
            "100000 increments of period did not get to present time.");
      }
      if (this.period == null && this.cronExpression == null) {
        break;
      } else if (this.cronExecutionTime != null) {
        final Date nextDate = this.cronExecutionTime.getNextValidTimeAfter(date.toDate());
        // Some Cron Expressions possibly do not have follow-up occurrences
        if (nextDate != null) {
          date = new DateTime(nextDate);
        } else {
          break;
        }
      } else {
        date = date.plus(this.period);
      }
      count += 1;
    }
    return date.getMillis();
  }

  @Override
  public Object getNum() {
    return null;
  }

  @Override
  public Object toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("type", type);
    jsonObj.put("firstCheckTime", String.valueOf(this.firstCheckTime));
    jsonObj.put("timezone", this.timezone.getID());
    jsonObj.put("nextCheckTime", String.valueOf(this.nextCheckTime));
    jsonObj.put("isRecurring", String.valueOf(this.isRecurring));
    jsonObj.put("skipPastChecks", String.valueOf(this.skipPastChecks));
    jsonObj.put("period", Utils.createPeriodString(this.period));
    jsonObj.put("id", this.id);
    jsonObj.put("cronExpression", this.cronExpression);

    return jsonObj;
  }

  @Override
  public void stopChecker() {
    return;
  }

  @Override
  public void setContext(final Map<String, Object> context) {
  }

}
