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
import azkaban.trigger.TriggerAgent;
import azkaban.trigger.TriggerStatus;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * The ScheduleManager stores and executes the schedule. It uses a single thread instead and waits
 * until correct loading time for the flow. It will not remove the flow from the schedule when it is
 * run, which can potentially allow the flow to and overlap each other.
 *
 * TODO kunkun-tang: When new AZ quartz Scheduler comes, we will remove this class.
 */
public class ScheduleManager implements TriggerAgent {

  public static final String SIMPLE_TIME_TRIGGER = "SimpleTimeTrigger";
  private static final Logger logger = Logger.getLogger(ScheduleManager.class);
  private final DateTimeFormatter _dateFormat = DateTimeFormat
      .forPattern("MM-dd-yyyy HH:mm:ss:SSS");
  private final ScheduleLoader loader;

  private final Map<Integer, Schedule> scheduleIDMap =
      new LinkedHashMap<>();
  private final Map<Pair<Integer, String>, Schedule> scheduleIdentityPairMap =
      new LinkedHashMap<>();

  /**
   * Give the schedule manager a loader class that will properly load the schedule.
   */
  @Inject
  public ScheduleManager(final ScheduleLoader loader) {
    this.loader = loader;
  }

  // Since ScheduleManager was already replaced by TriggerManager, many methods like start are
  // never used.
  @Deprecated
  @Override
  public void start() throws ScheduleManagerException {
  }

  // only do this when using external runner
  private synchronized void updateLocal() throws ScheduleManagerException {
    final List<Schedule> updates = this.loader.loadUpdatedSchedules();
    for (final Schedule s : updates) {
      if (s.getStatus().equals(TriggerStatus.EXPIRED.toString())) {
        onScheduleExpire(s);
      } else {
        internalSchedule(s);
      }
    }
  }

  private void onScheduleExpire(final Schedule s) {
    removeSchedule(s);
  }

  /**
   * Shutdowns the scheduler thread. After shutdown, it may not be safe to use it again.
   */
  @Override
  public void shutdown() {

  }

  /**
   * Retrieves a copy of the list of schedules.
   */
  public synchronized List<Schedule> getSchedules()
      throws ScheduleManagerException {

    updateLocal();
    return new ArrayList<>(this.scheduleIDMap.values());
  }

  /**
   * Returns the scheduled flow for the flow name
   */
  public Schedule getSchedule(final int projectId, final String flowId)
      throws ScheduleManagerException {
    updateLocal();
    return this.scheduleIdentityPairMap.get(new Pair<>(projectId,
        flowId));
  }

  /**
   * Returns the scheduled flow for the scheduleId
   *
   * @param scheduleId Schedule ID
   */
  public Schedule getSchedule(final int scheduleId) throws ScheduleManagerException {
    updateLocal();
    return this.scheduleIDMap.get(scheduleId);
  }


  /**
   * Removes the flow from the schedule if it exists.
   */
  public synchronized void removeSchedule(final Schedule sched) {
    final Pair<Integer, String> identityPairMap = sched.getScheduleIdentityPair();

    final Schedule schedule = this.scheduleIdentityPairMap.get(identityPairMap);
    if (schedule != null) {
      this.scheduleIdentityPairMap.remove(identityPairMap);
    }

    this.scheduleIDMap.remove(sched.getScheduleId());

    try {
      this.loader.removeSchedule(sched);
    } catch (final ScheduleManagerException e) {
      logger.error(e);
    }
  }

  public Schedule scheduleFlow(final int scheduleId,
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
      final ExecutionOptions execOptions) {
    final Schedule sched = new Schedule(scheduleId, projectId, projectName, flowName, status,
        firstSchedTime, endSchedTime, timezone, period, lastModifyTime, nextExecTime,
        submitTime, submitUser, execOptions, null);
    logger
        .info("Scheduling flow '" + sched.getScheduleName() + "' for "
            + this._dateFormat.print(firstSchedTime) + " with a period of " + (period == null
            ? "(non-recurring)"
            : period));

    insertSchedule(sched);
    return sched;
  }

  public Schedule cronScheduleFlow(final int scheduleId,
      final int projectId,
      final String projectName,
      final String flowName,
      final String status,
      final long firstSchedTime,
      final long endSchedTime,
      final DateTimeZone timezone,
      final long lastModifyTime,
      final long nextExecTime,
      final long submitTime,
      final String submitUser,
      final ExecutionOptions execOptions,
      final String cronExpression) {
    final Schedule sched =
        new Schedule(scheduleId, projectId, projectName, flowName, status,
            firstSchedTime, endSchedTime, timezone, null, lastModifyTime, nextExecTime,
            submitTime, submitUser, execOptions, cronExpression);
    logger
        .info("Scheduling flow '" + sched.getScheduleName() + "' for "
            + this._dateFormat.print(firstSchedTime) + " cron Expression = " + cronExpression);

    insertSchedule(sched);
    return sched;
  }

  /**
   * Schedules the flow, but doesn't save the schedule afterwards.
   */
  private synchronized void internalSchedule(final Schedule s) {
    this.scheduleIDMap.put(s.getScheduleId(), s);
    this.scheduleIdentityPairMap.put(s.getScheduleIdentityPair(), s);
  }

  /**
   * Adds a flow to the schedule.
   */
  public synchronized void insertSchedule(final Schedule s) {
    final Schedule exist = this.scheduleIdentityPairMap.get(s.getScheduleIdentityPair());
    if (s.updateTime()) {
      try {
        if (exist == null) {
          this.loader.insertSchedule(s);
          internalSchedule(s);
        } else {
          s.setScheduleId(exist.getScheduleId());
          this.loader.updateSchedule(s);
          internalSchedule(s);
        }
      } catch (final ScheduleManagerException e) {
        logger.error(e);
      }
    } else {
      logger
          .error("The provided schedule is non-recurring and the scheduled time already passed. "
              + s.getScheduleName());
    }
  }

  @Override
  public void loadTriggerFromProps(final Props props) throws ScheduleManagerException {
    throw new ScheduleManagerException("create " + getTriggerSource()
        + " from json not supported yet");
  }

  @Override
  public String getTriggerSource() {
    return SIMPLE_TIME_TRIGGER;
  }
}
