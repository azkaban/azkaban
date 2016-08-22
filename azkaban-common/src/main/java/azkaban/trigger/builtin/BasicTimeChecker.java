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

import java.util.HashMap;
import java.util.Map;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.apache.log4j.Logger;

import org.quartz.CronExpression;

import azkaban.trigger.ConditionChecker;
import azkaban.utils.Utils;

public class BasicTimeChecker implements ConditionChecker {

  private static Logger logger = Logger.getLogger(BasicTimeChecker.class);

  public static final String type = "BasicTimeChecker";

  private long firstCheckTime;
  private long nextCheckTime;
  private DateTimeZone timezone;
  private boolean isRecurring = true;
  private boolean skipPastChecks = true;
  private ReadablePeriod period;

  private String cronExpression;
  private CronExpression cronExecutionTime;
  private final String id;

  public BasicTimeChecker(String id, long firstCheckTime,
      DateTimeZone timezone, boolean isRecurring, boolean skipPastChecks,
      ReadablePeriod period, String cronExpression) {
    this.id = id;
    this.firstCheckTime = firstCheckTime;
    this.timezone = timezone;
    this.isRecurring = isRecurring;
    this.skipPastChecks = skipPastChecks;
    this.period = period;
    this.nextCheckTime = firstCheckTime;
    this.cronExpression = cronExpression;
    cronExecutionTime = Utils.parseCronExpression(cronExpression, timezone);
    this.nextCheckTime = calculateNextCheckTime();
  }

  public long getFirstCheckTime() {
    return firstCheckTime;
  }

  public DateTimeZone getTimeZone() {
    return timezone;
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

  public long getNextCheckTime() {
    return nextCheckTime;
  }

  public String getCronExpression() {
    return cronExpression;
  }

  public BasicTimeChecker(String id, long firstCheckTime,
      DateTimeZone timezone, long nextCheckTime, boolean isRecurring,
      boolean skipPastChecks, ReadablePeriod period, String cronExpression) {
    this.id = id;
    this.firstCheckTime = firstCheckTime;
    this.timezone = timezone;
    this.nextCheckTime = nextCheckTime;
    this.isRecurring = isRecurring;
    this.skipPastChecks = skipPastChecks;
    this.period = period;
    this.cronExpression = cronExpression;
    cronExecutionTime = Utils.parseCronExpression(cronExpression, timezone);
  }

  @Override
  public Boolean eval() {
    return nextCheckTime < System.currentTimeMillis();
  }

  @Override
  public void reset() {
    this.nextCheckTime = calculateNextCheckTime();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getType() {
    return type;
  }

  @SuppressWarnings("unchecked")
  public static BasicTimeChecker createFromJson(Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  public static BasicTimeChecker createFromJson(HashMap<String, Object> obj)
      throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from "
          + jsonObj.get("type"));
    }
    Long firstCheckTime = Long.valueOf((String) jsonObj.get("firstCheckTime"));
    String timezoneId = (String) jsonObj.get("timezone");
    long nextCheckTime = Long.valueOf((String) jsonObj.get("nextCheckTime"));
    DateTimeZone timezone = DateTimeZone.forID(timezoneId);
    boolean isRecurring = Boolean.valueOf((String) jsonObj.get("isRecurring"));
    boolean skipPastChecks =
        Boolean.valueOf((String) jsonObj.get("skipPastChecks"));
    ReadablePeriod period =
        Utils.parsePeriodString((String) jsonObj.get("period"));
    String id = (String) jsonObj.get("id");
    String cronExpression = (String) jsonObj.get("cronExpression");

    BasicTimeChecker checker =
        new BasicTimeChecker(id, firstCheckTime, timezone, nextCheckTime,
            isRecurring, skipPastChecks, period, cronExpression);
    if (skipPastChecks) {
      checker.updateNextCheckTime();
    }
    return checker;
  }

  @Override
  public BasicTimeChecker fromJson(Object obj) throws Exception {
    return createFromJson(obj);
  }

  private void updateNextCheckTime() {
    nextCheckTime = calculateNextCheckTime();
  }

  private long calculateNextCheckTime() {
    DateTime date = new DateTime(nextCheckTime).withZone(timezone);
    int count = 0;
    while (!date.isAfterNow()) {
      if (count > 100000) {
        throw new IllegalStateException(
            "100000 increments of period did not get to present time.");
      }
      if (period == null && cronExpression == null) {
        break;
      } else if (cronExecutionTime != null) {
        Date nextDate = cronExecutionTime.getNextValidTimeAfter(date.toDate());
        date = new DateTime(nextDate);
      } else {
        date = date.plus(period);
      }
      count += 1;
      if (!skipPastChecks) {
        continue;
      }
    }
    return date.getMillis();
  }

  @Override
  public Object getNum() {
    return null;
  }

  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("type", type);
    jsonObj.put("firstCheckTime", String.valueOf(firstCheckTime));
    jsonObj.put("timezone", timezone.getID());
    jsonObj.put("nextCheckTime", String.valueOf(nextCheckTime));
    jsonObj.put("isRecurring", String.valueOf(isRecurring));
    jsonObj.put("skipPastChecks", String.valueOf(skipPastChecks));
    jsonObj.put("period", Utils.createPeriodString(period));
    jsonObj.put("id", id);
    jsonObj.put("cronExpression", cronExpression);

    return jsonObj;
  }

  @Override
  public void stopChecker() {
    return;
  }

  @Override
  public void setContext(Map<String, Object> context) {
  }

}
