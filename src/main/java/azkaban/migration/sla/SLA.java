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

package azkaban.migration.sla;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.migration.scheduler.Schedule;

@Deprecated
public class SLA {

  public static enum SlaRule {
    SUCCESS(1), FINISH(2), WAITANDCHECKJOB(3);

    private int numVal;

    SlaRule(int numVal) {
      this.numVal = numVal;
    }

    public int getNumVal() {
      return numVal;
    }

    public static SlaRule fromInteger(int x) {
      switch (x) {
      case 1:
        return SUCCESS;
      case 2:
        return FINISH;
      case 3:
        return WAITANDCHECKJOB;
      default:
        return SUCCESS;
      }
    }
  }

  public static enum SlaAction {
    EMAIL(1), KILL(2);

    private int numVal;

    SlaAction(int numVal) {
      this.numVal = numVal;
    }

    public int getNumVal() {
      return numVal;
    }

    public static SlaAction fromInteger(int x) {
      switch (x) {
      case 1:
        return EMAIL;
      case 2:
        return KILL;
      default:
        return EMAIL;
      }
    }
  }

  public static class SlaSetting {
    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public ReadablePeriod getDuration() {
      return duration;
    }

    public void setDuration(ReadablePeriod duration) {
      this.duration = duration;
    }

    public SlaRule getRule() {
      return rule;
    }

    public void setRule(SlaRule rule) {
      this.rule = rule;
    }

    public List<SlaAction> getActions() {
      return actions;
    }

    public void setActions(List<SlaAction> actions) {
      this.actions = actions;
    }

    public Object toObject() {
      Map<String, Object> obj = new HashMap<String, Object>();
      obj.put("id", id);
      obj.put("duration", Schedule.createPeriodString(duration));
      // List<String> rulesObj = new ArrayList<String>();
      // for(SlaRule rule : rules) {
      // rulesObj.add(rule.toString());
      // }
      // obj.put("rules", rulesObj);
      obj.put("rule", rule.toString());
      List<String> actionsObj = new ArrayList<String>();
      for (SlaAction act : actions) {
        actionsObj.add(act.toString());
      }
      obj.put("actions", actionsObj);
      return obj;
    }

    @SuppressWarnings("unchecked")
    public static SlaSetting fromObject(Object obj) {
      Map<String, Object> slaObj = (HashMap<String, Object>) obj;
      String subId = (String) slaObj.get("id");
      ReadablePeriod dur =
          Schedule.parsePeriodString((String) slaObj.get("duration"));
      // List<String> rulesObj = (ArrayList<String>) slaObj.get("rules");
      // List<SlaRule> slaRules = new ArrayList<SLA.SlaRule>();
      // for(String rule : rulesObj) {
      // slaRules.add(SlaRule.valueOf(rule));
      // }
      SlaRule slaRule = SlaRule.valueOf((String) slaObj.get("rule"));
      List<String> actsObj = (ArrayList<String>) slaObj.get("actions");
      List<SlaAction> slaActs = new ArrayList<SlaAction>();
      for (String act : actsObj) {
        slaActs.add(SlaAction.valueOf(act));
      }

      SlaSetting ret = new SlaSetting();
      ret.setId(subId);
      ret.setDuration(dur);
      ret.setRule(slaRule);
      ret.setActions(slaActs);
      return ret;
    }

    private String id;
    private ReadablePeriod duration;
    private SlaRule rule = SlaRule.SUCCESS;
    private List<SlaAction> actions;
  }

  private int execId;
  private String jobName;
  private DateTime checkTime;
  private List<String> emails;
  private List<SlaAction> actions;
  private List<SlaSetting> jobSettings;
  private SlaRule rule;

  public SLA(int execId, String jobName, DateTime checkTime,
      List<String> emails, List<SlaAction> slaActions,
      List<SlaSetting> jobSettings, SlaRule slaRule) {
    this.execId = execId;
    this.jobName = jobName;
    this.checkTime = checkTime;
    this.emails = emails;
    this.actions = slaActions;
    this.jobSettings = jobSettings;
    this.rule = slaRule;
  }

  public int getExecId() {
    return execId;
  }

  public String getJobName() {
    return jobName;
  }

  public DateTime getCheckTime() {
    return checkTime;
  }

  public List<String> getEmails() {
    return emails;
  }

  public List<SlaAction> getActions() {
    return actions;
  }

  public List<SlaSetting> getJobSettings() {
    return jobSettings;
  }

  public SlaRule getRule() {
    return rule;
  }

  public String toString() {
    return execId + " " + jobName + " to be checked at "
        + checkTime.toDateTimeISO();
  }

  public Map<String, Object> optionToObject() {
    HashMap<String, Object> slaObj = new HashMap<String, Object>();

    slaObj.put("emails", emails);
    // slaObj.put("rule", rule.toString());

    List<String> actionsObj = new ArrayList<String>();
    for (SlaAction act : actions) {
      actionsObj.add(act.toString());
    }
    slaObj.put("actions", actionsObj);

    if (jobSettings != null && jobSettings.size() > 0) {
      List<Object> settingsObj = new ArrayList<Object>();
      for (SlaSetting set : jobSettings) {
        settingsObj.add(set.toObject());
      }
      slaObj.put("jobSettings", settingsObj);
    }

    return slaObj;
  }

  @SuppressWarnings("unchecked")
  public static SLA createSlaFromObject(int execId, String jobName,
      DateTime checkTime, SlaRule rule, Object obj) {

    HashMap<String, Object> slaObj = (HashMap<String, Object>) obj;

    List<String> emails = (List<String>) slaObj.get("emails");
    // SlaRule rule = SlaRule.valueOf((String)slaObj.get("rule"));
    List<String> actsObj = (ArrayList<String>) slaObj.get("actions");
    List<SlaAction> slaActs = new ArrayList<SlaAction>();
    for (String act : actsObj) {
      slaActs.add(SlaAction.valueOf(act));
    }
    List<SlaSetting> jobSets = null;
    if (slaObj.containsKey("jobSettings") && slaObj.get("jobSettings") != null) {
      jobSets = new ArrayList<SLA.SlaSetting>();
      for (Object set : (List<Object>) slaObj.get("jobSettings")) {
        SlaSetting jobSet = SlaSetting.fromObject(set);
        jobSets.add(jobSet);
      }
    }

    return new SLA(execId, jobName, checkTime, emails, slaActs, jobSets, rule);
  }

  public void setCheckTime(DateTime time) {
    this.checkTime = time;
  }

}
