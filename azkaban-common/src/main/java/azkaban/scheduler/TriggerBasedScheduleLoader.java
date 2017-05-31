/*
 * Copyright 2014 LinkedIn Corp.
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

import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerAdapter;
import azkaban.trigger.TriggerManagerException;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class TriggerBasedScheduleLoader implements ScheduleLoader {

  private static final Logger logger = Logger
      .getLogger(TriggerBasedScheduleLoader.class);

  private final TriggerManagerAdapter triggerManager;

  private final String triggerSource;

  private long lastUpdateTime = -1;

  public TriggerBasedScheduleLoader(final TriggerManager triggerManager,
      final String triggerSource) {
    this.triggerManager = triggerManager;
    this.triggerSource = triggerSource;
  }

  private Trigger scheduleToTrigger(final Schedule s) {
    final Condition triggerCondition = createTriggerCondition(s);
    final Condition expireCondition = createExpireCondition(s);
    final List<TriggerAction> actions = createActions(s);

    final Trigger t = new Trigger.TriggerBuilder(s.getSubmitUser(),
        this.triggerSource,
        triggerCondition,
        expireCondition,
        actions)
        .setSubmitTime(s.getSubmitTime())
        .setLastModifyTime(s.getLastModifyTime())
        .setId(s.getScheduleId())
        .build();

    if (s.isRecurring()) {
      t.setResetOnTrigger(true);
    } else {
      t.setResetOnTrigger(false);
    }
    return t;
  }

  private List<TriggerAction> createActions(final Schedule s) {
    final List<TriggerAction> actions = new ArrayList<>();
    final ExecuteFlowAction executeAct =
        new ExecuteFlowAction("executeFlowAction", s.getProjectId(),
            s.getProjectName(), s.getFlowName(), s.getSubmitUser(),
            s.getExecutionOptions(), s.getSlaOptions());
    actions.add(executeAct);

    return actions;
  }

  private Condition createTriggerCondition(final Schedule s) {
    final Map<String, ConditionChecker> checkers =
        new HashMap<>();
    final ConditionChecker checker =
        new BasicTimeChecker("BasicTimeChecker_1", s.getFirstSchedTime(),
            s.getTimezone(), s.isRecurring(), s.skipPastOccurrences(),
            s.getPeriod(), s.getCronExpression());
    checkers.put(checker.getId(), checker);
    final String expr = checker.getId() + ".eval()";
    final Condition cond = new Condition(checkers, expr);
    return cond;
  }

  // if failed to trigger, auto expire?
  private Condition createExpireCondition(final Schedule s) {
    final Map<String, ConditionChecker> checkers =
        new HashMap<>();
    final ConditionChecker checker =
        new BasicTimeChecker("BasicTimeChecker_2", s.getFirstSchedTime(),
            s.getTimezone(), s.isRecurring(), s.skipPastOccurrences(),
            s.getPeriod(), s.getCronExpression());
    checkers.put(checker.getId(), checker);
    final String expr = checker.getId() + ".eval()";
    final Condition cond = new Condition(checkers, expr);
    return cond;
  }

  @Override
  public void insertSchedule(final Schedule s) throws ScheduleManagerException {
    final Trigger t = scheduleToTrigger(s);
    try {
      this.triggerManager.insertTrigger(t, t.getSubmitUser());
      s.setScheduleId(t.getTriggerId());
    } catch (final TriggerManagerException e) {
      throw new ScheduleManagerException("Failed to insert new schedule!", e);
    }
  }

  @Override
  public void updateSchedule(final Schedule s) throws ScheduleManagerException {
    final Trigger t = scheduleToTrigger(s);
    try {
      this.triggerManager.updateTrigger(t, t.getSubmitUser());
    } catch (final TriggerManagerException e) {
      throw new ScheduleManagerException("Failed to update schedule!", e);
    }
  }

  // TODO may need to add logic to filter out skip runs
  @Override
  public synchronized List<Schedule> loadSchedules()
      throws ScheduleManagerException {
    final List<Trigger> triggers = this.triggerManager.getTriggers(this.triggerSource);
    final List<Schedule> schedules = new ArrayList<>();
    for (final Trigger t : triggers) {
      this.lastUpdateTime = Math.max(this.lastUpdateTime, t.getLastModifyTime());
      final Schedule s = triggerToSchedule(t);
      schedules.add(s);
      System.out.println("loaded schedule for "
          + s.getProjectName() + " (project_ID: " + s.getProjectId() + ")");
    }
    return schedules;

  }

  private Schedule triggerToSchedule(final Trigger t) throws ScheduleManagerException {
    final Condition triggerCond = t.getTriggerCondition();
    final Map<String, ConditionChecker> checkers = triggerCond.getCheckers();
    BasicTimeChecker ck = null;
    for (final ConditionChecker checker : checkers.values()) {
      if (checker.getType().equals(BasicTimeChecker.type)) {
        ck = (BasicTimeChecker) checker;
        break;
      }
    }
    final List<TriggerAction> actions = t.getActions();
    ExecuteFlowAction act = null;
    for (final TriggerAction action : actions) {
      if (action.getType().equals(ExecuteFlowAction.type)) {
        act = (ExecuteFlowAction) action;
        break;
      }
    }
    if (ck != null && act != null) {
      final Schedule s =
          new Schedule(t.getTriggerId(), act.getProjectId(),
              act.getProjectName(), act.getFlowName(),
              t.getStatus().toString(), ck.getFirstCheckTime(),
              ck.getTimeZone(), ck.getPeriod(), t.getLastModifyTime(),
              ck.getNextCheckTime(), t.getSubmitTime(), t.getSubmitUser(),
              act.getExecutionOptions(), act.getSlaOptions(), ck.getCronExpression());
      return s;
    } else {
      logger.error("Failed to parse schedule from trigger!");
      throw new ScheduleManagerException(
          "Failed to parse schedule from trigger!");
    }
  }

  @Override
  public void removeSchedule(final Schedule s) throws ScheduleManagerException {
    try {
      this.triggerManager.removeTrigger(s.getScheduleId(), s.getSubmitUser());
    } catch (final TriggerManagerException e) {
      throw new ScheduleManagerException(e.getMessage());
    }

  }

  @Override
  public void updateNextExecTime(final Schedule s) throws ScheduleManagerException {

  }

  @Override
  public synchronized List<Schedule> loadUpdatedSchedules()
      throws ScheduleManagerException {
    final List<Trigger> triggers;
    try {
      triggers =
          this.triggerManager.getTriggerUpdates(this.triggerSource, this.lastUpdateTime);
    } catch (final TriggerManagerException e) {
      e.printStackTrace();
      throw new ScheduleManagerException(e);
    }
    final List<Schedule> schedules = new ArrayList<>();
    for (final Trigger t : triggers) {
      this.lastUpdateTime = Math.max(this.lastUpdateTime, t.getLastModifyTime());
      final Schedule s = triggerToSchedule(t);
      schedules.add(s);
      System.out.println("loaded schedule for "
          + s.getProjectName() + " (project_ID: " + s.getProjectId() + ")");
    }
    return schedules;
  }

}
