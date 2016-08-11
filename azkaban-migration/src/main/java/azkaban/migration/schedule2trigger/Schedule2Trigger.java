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

package azkaban.migration.schedule2trigger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;

import azkaban.executor.ExecutionOptions;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.JdbcTriggerLoader;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerLoader;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.Utils;

import static azkaban.migration.schedule2trigger.CommonParams.*;

@SuppressWarnings("deprecation")
public class Schedule2Trigger {

  private static final Logger logger = Logger.getLogger(Schedule2Trigger.class);
  private static Props props;
  private static File outputDir;

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
    }

    File confFile = new File(args[0]);
    try {
      logger.info("Trying to load config from " + confFile.getAbsolutePath());
      props = loadAzkabanConfig(confFile);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e);
      return;
    }

    try {
      outputDir = File.createTempFile("schedules", null);
      logger.info("Creating temp dir for dumping existing schedules.");
      outputDir.delete();
      outputDir.mkdir();
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e);
      return;
    }

    try {
      schedule2File();
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e);
      return;
    }

    try {
      file2ScheduleTrigger();
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e);
      return;
    }

    logger.info("Uploaded all schedules. Removing temp dir.");
    FileUtils.deleteDirectory(outputDir);
    System.exit(0);
  }

  private static Props loadAzkabanConfig(File confFile) throws IOException {
    return new Props(null, confFile);
  }

  private static void printUsage() {
    System.out.println("Usage: schedule2Trigger PATH_TO_CONFIG_FILE");
  }

  private static void schedule2File() throws Exception {
    azkaban.migration.scheduler.ScheduleLoader scheduleLoader =
        new azkaban.migration.scheduler.JdbcScheduleLoader(props);
    logger.info("Loading old schedule info from DB.");
    List<azkaban.migration.scheduler.Schedule> schedules =
        scheduleLoader.loadSchedules();
    for (azkaban.migration.scheduler.Schedule sched : schedules) {
      writeScheduleFile(sched, outputDir);
    }
  }

  private static void writeScheduleFile(
      azkaban.migration.scheduler.Schedule sched, File outputDir)
      throws IOException {
    String scheduleFileName =
        sched.getProjectName() + "-" + sched.getFlowName();
    File outputFile = new File(outputDir, scheduleFileName);
    outputFile.createNewFile();
    Props props = new Props();
    props.put("flowName", sched.getFlowName());
    props.put("projectName", sched.getProjectName());
    props.put("projectId", String.valueOf(sched.getProjectId()));
    props.put("period", azkaban.migration.scheduler.Schedule
        .createPeriodString(sched.getPeriod()));
    props.put("firstScheduleTimeLong", sched.getFirstSchedTime());
    props.put("timezone", sched.getTimezone().getID());
    props.put("submitUser", sched.getSubmitUser());
    props.put("submitTimeLong", sched.getSubmitTime());
    props.put("nextExecTimeLong", sched.getNextExecTime());

    ExecutionOptions executionOptions = sched.getExecutionOptions();
    if (executionOptions != null) {
      props.put("executionOptionsObj",
          JSONUtils.toJSON(executionOptions.toObject()));
    }

    azkaban.migration.sla.SlaOptions slaOptions = sched.getSlaOptions();
    if (slaOptions != null) {

      List<Map<String, Object>> settingsObj =
          new ArrayList<Map<String, Object>>();
      List<azkaban.migration.sla.SLA.SlaSetting> settings =
          slaOptions.getSettings();
      for (azkaban.migration.sla.SLA.SlaSetting set : settings) {
        Map<String, Object> setObj = new HashMap<String, Object>();
        String setId = set.getId();
        azkaban.migration.sla.SLA.SlaRule rule = set.getRule();
        Map<String, Object> info = new HashMap<String, Object>();
        info.put(INFO_DURATION, azkaban.migration.scheduler.Schedule
            .createPeriodString(set.getDuration()));
        info.put(INFO_EMAIL_LIST, slaOptions.getSlaEmails());
        List<String> actionsList = new ArrayList<String>();
        for (azkaban.migration.sla.SLA.SlaAction act : set.getActions()) {
          if (act.equals(azkaban.migration.sla.SLA.SlaAction.EMAIL)) {
            actionsList.add(ACTION_ALERT);
            info.put(ALERT_TYPE, "email");
          } else if (act.equals(azkaban.migration.sla.SLA.SlaAction.KILL)) {
            actionsList.add(ACTION_CANCEL_FLOW);
          }
        }
        setObj.put("actions", actionsList);
        if (setId.equals("")) {
          info.put(INFO_FLOW_NAME, sched.getFlowName());
          if (rule.equals(azkaban.migration.sla.SLA.SlaRule.FINISH)) {
            setObj.put("type", TYPE_FLOW_FINISH);
          } else if (rule.equals(azkaban.migration.sla.SLA.SlaRule.SUCCESS)) {
            setObj.put("type", TYPE_FLOW_SUCCEED);
          }
        } else {
          info.put(INFO_JOB_NAME, setId);
          if (rule.equals(azkaban.migration.sla.SLA.SlaRule.FINISH)) {
            setObj.put("type", TYPE_JOB_FINISH);
          } else if (rule.equals(azkaban.migration.sla.SLA.SlaRule.SUCCESS)) {
            setObj.put("type", TYPE_JOB_SUCCEED);
          }
        }
        setObj.put("info", info);
        settingsObj.add(setObj);
      }

      props.put("slaOptionsObj", JSONUtils.toJSON(settingsObj));
    }
    props.storeLocal(outputFile);
  }

  @SuppressWarnings("unchecked")
  private static void file2ScheduleTrigger() throws Exception {

    TriggerLoader triggerLoader = new JdbcTriggerLoader(props);
    for (File scheduleFile : outputDir.listFiles()) {
      logger.info("Trying to load schedule from "
          + scheduleFile.getAbsolutePath());
      if (scheduleFile.isFile()) {
        Props schedProps = new Props(null, scheduleFile);
        String flowName = schedProps.getString("flowName");
        String projectName = schedProps.getString("projectName");
        int projectId = schedProps.getInt("projectId");
        long firstSchedTimeLong = schedProps.getLong("firstScheduleTimeLong");
        // DateTime firstSchedTime = new DateTime(firstSchedTimeLong);
        String timezoneId = schedProps.getString("timezone");
        DateTimeZone timezone = DateTimeZone.forID(timezoneId);
        ReadablePeriod period =
            Utils.parsePeriodString(schedProps.getString("period"));
        String cronExpression = schedProps.getString("cronExpression");
        // DateTime lastModifyTime = DateTime.now();
        long nextExecTimeLong = schedProps.getLong("nextExecTimeLong");
        // DateTime nextExecTime = new DateTime(nextExecTimeLong);
        long submitTimeLong = schedProps.getLong("submitTimeLong");
        // DateTime submitTime = new DateTime(submitTimeLong);
        String submitUser = schedProps.getString("submitUser");
        ExecutionOptions executionOptions = null;
        if (schedProps.containsKey("executionOptionsObj")) {
          String executionOptionsObj =
              schedProps.getString("executionOptionsObj");
          executionOptions =
              ExecutionOptions.createFromObject(JSONUtils
                  .parseJSONFromString(executionOptionsObj));
        } else {
          executionOptions = new ExecutionOptions();
        }
        List<azkaban.sla.SlaOption> slaOptions = null;
        if (schedProps.containsKey("slaOptionsObj")) {
          slaOptions = new ArrayList<azkaban.sla.SlaOption>();
          List<Map<String, Object>> settingsObj =
              (List<Map<String, Object>>) JSONUtils
                  .parseJSONFromString(schedProps.getString("slaOptionsObj"));
          for (Map<String, Object> sla : settingsObj) {
            String type = (String) sla.get("type");
            Map<String, Object> info = (Map<String, Object>) sla.get("info");
            List<String> actions = (List<String>) sla.get("actions");
            azkaban.sla.SlaOption slaOption =
                new azkaban.sla.SlaOption(type, actions, info);
            slaOptions.add(slaOption);
          }
        }

        azkaban.scheduler.Schedule schedule =
            new azkaban.scheduler.Schedule(-1, projectId, projectName,
                flowName, "ready", firstSchedTimeLong, timezone, period,
                DateTime.now().getMillis(), nextExecTimeLong, submitTimeLong,
                submitUser, executionOptions, slaOptions, cronExpression);
        Trigger t = scheduleToTrigger(schedule);
        logger.info("Ready to insert trigger " + t.getDescription());
        triggerLoader.addTrigger(t);

      }

    }
  }

  private static Trigger scheduleToTrigger(azkaban.scheduler.Schedule s) {

    Condition triggerCondition = createTimeTriggerCondition(s);
    Condition expireCondition = createTimeExpireCondition(s);
    List<TriggerAction> actions = createActions(s);
    Trigger t =
        new Trigger(s.getScheduleId(), s.getLastModifyTime(),
            s.getSubmitTime(), s.getSubmitUser(),
            azkaban.scheduler.ScheduleManager.triggerSource, triggerCondition,
            expireCondition, actions);
    if (s.isRecurring()) {
      t.setResetOnTrigger(true);
    }
    return t;
  }

  private static List<TriggerAction> createActions(azkaban.scheduler.Schedule s) {
    List<TriggerAction> actions = new ArrayList<TriggerAction>();
    ExecuteFlowAction executeAct =
        new ExecuteFlowAction("executeFlowAction", s.getProjectId(),
            s.getProjectName(), s.getFlowName(), s.getSubmitUser(),
            s.getExecutionOptions(), s.getSlaOptions());
    actions.add(executeAct);

    return actions;
  }

  private static Condition createTimeTriggerCondition(
      azkaban.scheduler.Schedule s) {
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
  private static Condition createTimeExpireCondition(
      azkaban.scheduler.Schedule s) {
    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();
    ConditionChecker checker =
        new BasicTimeChecker("BasicTimeChecker_2", s.getFirstSchedTime(),
            s.getTimezone(), s.isRecurring(), s.skipPastOccurrences(),
            s.getPeriod(),s.getCronExpression());
    checkers.put(checker.getId(), checker);
    String expr = checker.getId() + ".eval()";
    Condition cond = new Condition(checkers, expr);
    return cond;
  }

}
