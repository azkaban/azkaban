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

package azkaban.sla;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.executor.ExecutableFlow;

public class SlaOption {

  public static final String TYPE_FLOW_FINISH = "FlowFinish";
  public static final String TYPE_FLOW_SUCCEED = "FlowSucceed";
  public static final String TYPE_FLOW_PROGRESS = "FlowProgress";

  public static final String TYPE_JOB_FINISH = "JobFinish";
  public static final String TYPE_JOB_SUCCEED = "JobSucceed";
  public static final String TYPE_JOB_PROGRESS = "JobProgress";

  public static final String INFO_DURATION = "Duration";
  public static final String INFO_FLOW_NAME = "FlowName";
  public static final String INFO_JOB_NAME = "JobName";
  public static final String INFO_PROGRESS_PERCENT = "ProgressPercent";
  public static final String INFO_EMAIL_LIST = "EmailList";

  // always alert
  public static final String ALERT_TYPE = "SlaAlertType";
  public static final String ACTION_CANCEL_FLOW = "SlaCancelFlow";
  public static final String ACTION_ALERT = "SlaAlert";

  private String type;
  private Map<String, Object> info;
  private List<String> actions;

  private static DateTimeFormatter fmt = DateTimeFormat
      .forPattern("MM/dd, YYYY HH:mm");

  public SlaOption(String type, List<String> actions, Map<String, Object> info) {
    this.type = type;
    this.info = info;
    this.actions = actions;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Map<String, Object> getInfo() {
    return info;
  }

  public void setInfo(Map<String, Object> info) {
    this.info = info;
  }

  public List<String> getActions() {
    return actions;
  }

  public void setActions(List<String> actions) {
    this.actions = actions;
  }

  public Map<String, Object> toObject() {
    HashMap<String, Object> slaObj = new HashMap<String, Object>();

    slaObj.put("type", type);
    slaObj.put("info", info);
    slaObj.put("actions", actions);

    return slaObj;
  }

  @SuppressWarnings("unchecked")
  public static SlaOption fromObject(Object object) {

    HashMap<String, Object> slaObj = (HashMap<String, Object>) object;

    String type = (String) slaObj.get("type");
    List<String> actions = (List<String>) slaObj.get("actions");
    Map<String, Object> info = (Map<String, Object>) slaObj.get("info");

    return new SlaOption(type, actions, info);
  }

  public Object toWebObject() {
    HashMap<String, Object> slaObj = new HashMap<String, Object>();

    if (type.equals(TYPE_FLOW_FINISH) || type.equals(TYPE_FLOW_SUCCEED)) {
      slaObj.put("id", "");
    } else {
      slaObj.put("id", info.get(INFO_JOB_NAME));
    }
    slaObj.put("duration", info.get(INFO_DURATION));
    if (type.equals(TYPE_FLOW_FINISH) || type.equals(TYPE_JOB_FINISH)) {
      slaObj.put("rule", "FINISH");
    } else {
      slaObj.put("rule", "SUCCESS");
    }
    List<String> actionsObj = new ArrayList<String>();
    for (String act : actions) {
      if (act.equals(ACTION_ALERT)) {
        actionsObj.add("EMAIL");
      } else {
        actionsObj.add("KILL");
      }
    }
    slaObj.put("actions", actionsObj);

    return slaObj;
  }

  @Override
  public String toString() {
    return "Sla of " + getType() + getInfo() + getActions();
  }

  public static String createSlaMessage(SlaOption slaOption, ExecutableFlow flow) {
    String type = slaOption.getType();
    int execId = flow.getExecutionId();
    if (type.equals(SlaOption.TYPE_FLOW_FINISH)) {
      String flowName =
          (String) slaOption.getInfo().get(SlaOption.INFO_FLOW_NAME);
      String duration =
          (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
      String basicinfo =
          "SLA Alert: Your flow " + flowName + " failed to FINISH within "
              + duration + "</br>";
      String expected =
          "Here is details : </br>" + "Flow " + flowName + " in execution "
              + execId + " is expected to FINISH within " + duration + " from "
              + fmt.print(new DateTime(flow.getStartTime())) + "</br>";
      String actual = "Actual flow status is " + flow.getStatus();
      return basicinfo + expected + actual;
    } else if (type.equals(SlaOption.TYPE_FLOW_SUCCEED)) {
      String flowName =
          (String) slaOption.getInfo().get(SlaOption.INFO_FLOW_NAME);
      String duration =
          (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
      String basicinfo =
          "SLA Alert: Your flow " + flowName + " failed to SUCCEED within "
              + duration + "</br>";
      String expected =
          "Here is details : </br>" + "Flow " + flowName + " in execution "
              + execId + " expected to FINISH within " + duration + " from "
              + fmt.print(new DateTime(flow.getStartTime())) + "</br>";
      String actual = "Actual flow status is " + flow.getStatus();
      return basicinfo + expected + actual;
    } else if (type.equals(SlaOption.TYPE_JOB_FINISH)) {
      String jobName =
          (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      String duration =
          (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
      return "SLA Alert: Your job " + jobName + " failed to FINISH within "
          + duration + " in execution " + execId;
    } else if (type.equals(SlaOption.TYPE_JOB_SUCCEED)) {
      String jobName =
          (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      String duration =
          (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
      return "SLA Alert: Your job " + jobName + " failed to SUCCEED within "
          + duration + " in execution " + execId;
    } else {
      return "Unrecognized SLA type " + type;
    }
  }

}
