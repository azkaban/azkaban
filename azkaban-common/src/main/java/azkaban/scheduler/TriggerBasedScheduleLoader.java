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
import azkaban.trigger.builtin.EndTimeChecker;
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

    Trigger t = new Trigger.TriggerBuilder(s.getSubmitUser(),
        triggerSource,
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

  private Condition createExpireCondition(Schedule s) {
    Map<String, ConditionChecker> checkers = new HashMap<>();
    ConditionChecker checker = new BasicTimeChecker("EndTimeCheck_1", s.getFirstSchedTime(),
        s.getTimezone(), s.getEndSchedTime(),false, false,
        null, null);
    checkers.put(checker.getId(), checker);
    String expr = checker.getId() + ".eval()";
    return new Condition(checkers, expr);
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

  // Todo kunkun-tang: Never used method, should be completely removed later.
  @Override
  public synchronized List<Schedule> loadSchedules()
      throws ScheduleManagerException {
    return null;
  }

  private Schedule triggerToSchedule(Trigger t) throws ScheduleManagerException {
    Condition triggerCond = t.getTriggerCondition();
    Map<String, ConditionChecker> checkers = triggerCond.getCheckers();
    BasicTimeChecker triggerTimeChecker = null;
    BasicTimeChecker endTimeChecker = null;

    for (ConditionChecker checker : checkers.values()) {
      if (checker.getType().equals(BasicTimeChecker.type) && checker.getId().contains("BasicTimeCheck")) {
        triggerTimeChecker = (BasicTimeChecker) checker;
      }
      if (checker.getType().equals(BasicTimeChecker.type) && checker.getId().contains("EndTimeCheck")) {
        endTimeChecker = (BasicTimeChecker) checker;
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
    if (triggerTimeChecker != null && act != null) {
      return new Schedule(t.getTriggerId(),
          act.getProjectId(),
          act.getProjectName(),
          act.getFlowName(),
          t.getStatus().toString(),
          triggerTimeChecker.getFirstCheckTime(),
          // getNextCheckTime
          endTimeChecker == null? 2536871155000L: endTimeChecker.getNextCheckTime(),
          triggerTimeChecker.getTimeZone(),
          triggerTimeChecker.getPeriod(),
          t.getLastModifyTime(),
          triggerTimeChecker.getNextCheckTime(),
          t.getSubmitTime(),
          t.getSubmitUser(),
          act.getExecutionOptions(),
          act.getSlaOptions(),
          triggerTimeChecker.getCronExpression());
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
      logger.info("loaded schedule for "
          + s.getProjectName() + " (project_ID: " + s.getProjectId() + ")");
    }
    return schedules;
  }

}
