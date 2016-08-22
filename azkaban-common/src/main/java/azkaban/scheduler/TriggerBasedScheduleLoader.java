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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerAdapter;
import azkaban.trigger.TriggerManagerException;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;

public class TriggerBasedScheduleLoader implements ScheduleLoader {

  private static Logger logger = Logger
      .getLogger(TriggerBasedScheduleLoader.class);

  private TriggerManagerAdapter triggerManager;

  private String triggerSource;

  private long lastUpdateTime = -1;

  public TriggerBasedScheduleLoader(TriggerManager triggerManager,
      String triggerSource) {
    this.triggerManager = triggerManager;
    this.triggerSource = triggerSource;
  }

  private Trigger scheduleToTrigger(Schedule s) {
    Condition triggerCondition = createTriggerCondition(s);
    Condition expireCondition = createExpireCondition(s);
    List<TriggerAction> actions = createActions(s);
    Trigger t =
        new Trigger(s.getScheduleId(), s.getLastModifyTime(),
            s.getSubmitTime(), s.getSubmitUser(), triggerSource,
            triggerCondition, expireCondition, actions);
    if (s.isRecurring()) {
      t.setResetOnTrigger(true);
    } else {
      t.setResetOnTrigger(false);
    }
    return t;
  }

  private List<TriggerAction> createActions(Schedule s) {
    List<TriggerAction> actions = new ArrayList<TriggerAction>();
    ExecuteFlowAction executeAct =
        new ExecuteFlowAction("executeFlowAction", s.getProjectId(),
            s.getProjectName(), s.getFlowName(), s.getSubmitUser(),
            s.getExecutionOptions(), s.getSlaOptions());
    actions.add(executeAct);

    return actions;
  }

  private Condition createTriggerCondition(Schedule s) {
    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();
    ConditionChecker checker =
        new BasicTimeChecker("BasicTimeChecker_1", s.getFirstSchedTime(),
            s.getTimezone(), s.isRecurring(), s.skipPastOccurrences(),
            s.getPeriod(), s.getCronExpression());
    checkers.put(checker.getId(), checker);
    String expr = checker.getId() + ".eval()";
    Condition cond = new Condition(checkers, expr);
    return cond;
  }

  // if failed to trigger, auto expire?
  private Condition createExpireCondition(Schedule s) {
    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();
    ConditionChecker checker =
        new BasicTimeChecker("BasicTimeChecker_2", s.getFirstSchedTime(),
            s.getTimezone(), s.isRecurring(), s.skipPastOccurrences(),
            s.getPeriod(), s.getCronExpression());
    checkers.put(checker.getId(), checker);
    String expr = checker.getId() + ".eval()";
    Condition cond = new Condition(checkers, expr);
    return cond;
  }

  @Override
  public void insertSchedule(Schedule s) throws ScheduleManagerException {
    Trigger t = scheduleToTrigger(s);
    try {
      triggerManager.insertTrigger(t, t.getSubmitUser());
      s.setScheduleId(t.getTriggerId());
    } catch (TriggerManagerException e) {
      throw new ScheduleManagerException("Failed to insert new schedule!", e);
    }
  }

  @Override
  public void updateSchedule(Schedule s) throws ScheduleManagerException {
    Trigger t = scheduleToTrigger(s);
    try {
      triggerManager.updateTrigger(t, t.getSubmitUser());
    } catch (TriggerManagerException e) {
      throw new ScheduleManagerException("Failed to update schedule!", e);
    }
  }

  // TODO may need to add logic to filter out skip runs
  @Override
  public synchronized List<Schedule> loadSchedules()
      throws ScheduleManagerException {
    List<Trigger> triggers = triggerManager.getTriggers(triggerSource);
    List<Schedule> schedules = new ArrayList<Schedule>();
    for (Trigger t : triggers) {
      lastUpdateTime = Math.max(lastUpdateTime, t.getLastModifyTime());
      Schedule s = triggerToSchedule(t);
      schedules.add(s);
      System.out.println("loaded schedule for "
          + s.getProjectName() + " (project_ID: " + s.getProjectId() + ")");
    }
    return schedules;

  }

  private Schedule triggerToSchedule(Trigger t) throws ScheduleManagerException {
    Condition triggerCond = t.getTriggerCondition();
    Map<String, ConditionChecker> checkers = triggerCond.getCheckers();
    BasicTimeChecker ck = null;
    for (ConditionChecker checker : checkers.values()) {
      if (checker.getType().equals(BasicTimeChecker.type)) {
        ck = (BasicTimeChecker) checker;
        break;
      }
    }
    List<TriggerAction> actions = t.getActions();
    ExecuteFlowAction act = null;
    for (TriggerAction action : actions) {
      if (action.getType().equals(ExecuteFlowAction.type)) {
        act = (ExecuteFlowAction) action;
        break;
      }
    }
    if (ck != null && act != null) {
      Schedule s =
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
  public void removeSchedule(Schedule s) throws ScheduleManagerException {
    try {
      triggerManager.removeTrigger(s.getScheduleId(), s.getSubmitUser());
    } catch (TriggerManagerException e) {
      throw new ScheduleManagerException(e.getMessage());
    }

  }

  @Override
  public void updateNextExecTime(Schedule s) throws ScheduleManagerException {

  }

  @Override
  public synchronized List<Schedule> loadUpdatedSchedules()
      throws ScheduleManagerException {
    List<Trigger> triggers;
    try {
      triggers =
          triggerManager.getTriggerUpdates(triggerSource, lastUpdateTime);
    } catch (TriggerManagerException e) {
      e.printStackTrace();
      throw new ScheduleManagerException(e);
    }
    List<Schedule> schedules = new ArrayList<Schedule>();
    for (Trigger t : triggers) {
      lastUpdateTime = Math.max(lastUpdateTime, t.getLastModifyTime());
      Schedule s = triggerToSchedule(t);
      schedules.add(s);
      System.out.println("loaded schedule for "
          + s.getProjectName() + " (project_ID: " + s.getProjectId() + ")");
    }
    return schedules;
  }

}
