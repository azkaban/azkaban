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

import azkaban.executor.ExecutableFlow;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SlaOption {

  public static final String TYPE_FLOW_FINISH = "FlowFinish";
  public static final String TYPE_FLOW_SUCCEED = "FlowSucceed";

  public static final String TYPE_JOB_FINISH = "JobFinish";
  public static final String TYPE_JOB_SUCCEED = "JobSucceed";

  public static final String INFO_DURATION = "Duration";
  public static final String INFO_FLOW_NAME = "FlowName";
  public static final String INFO_JOB_NAME = "JobName";
  public static final String INFO_EMAIL_LIST = "EmailList";

  // always alert
  public static final String ALERT_TYPE = "SlaAlertType";
  public static final String ACTION_CANCEL_FLOW = "SlaCancelFlow";
  public static final String ACTION_ALERT = "SlaAlert";
  public static final String ACTION_KILL_JOB = "SlaKillJob";
  private static final DateTimeFormatter fmt = DateTimeFormat
      .forPattern("MM/dd, YYYY HH:mm");

  private String type;
  private Map<String, Object> info;
  private List<String> actions;

  public SlaOption(final String type, final List<String> actions, final Map<String, Object> info) {
    this.type = type;
    this.info = info;
    this.actions = actions;
  }

  public static List<SlaOption> getJobLevelSLAOptions(final ExecutableFlow flow) {
    final Set<String> jobLevelSLAs = new HashSet<>(
        Arrays.asList(SlaOption.TYPE_JOB_FINISH, SlaOption.TYPE_JOB_SUCCEED));
    return flow.getSlaOptions().stream()
        .filter(slaOption -> jobLevelSLAs.contains(slaOption.getType()))
        .collect(Collectors.toList());
  }

  public static List<SlaOption> getFlowLevelSLAOptions(final ExecutableFlow flow) {
    final Set<String> flowLevelSLAs = new HashSet<>(
        Arrays.asList(SlaOption.TYPE_FLOW_FINISH, SlaOption.TYPE_FLOW_SUCCEED));
    return flow.getSlaOptions().stream()
        .filter(slaOption -> flowLevelSLAs.contains(slaOption.getType()))
        .collect(Collectors.toList());
  }

  public static SlaOption fromObject(final Object object) {

    final HashMap<String, Object> slaObj = (HashMap<String, Object>) object;

    final String type = (String) slaObj.get("type");
    final List<String> actions = (List<String>) slaObj.get("actions");
    final Map<String, Object> info = (Map<String, Object>) slaObj.get("info");

    return new SlaOption(type, actions, info);
  }

  public static String createSlaMessage(final SlaOption slaOption, final ExecutableFlow flow) {
    final String type = slaOption.getType();
    final int execId = flow.getExecutionId();
    if (type.equals(SlaOption.TYPE_FLOW_FINISH)) {
      final String flowName =
          (String) slaOption.getInfo().get(SlaOption.INFO_FLOW_NAME);
      final String duration =
          (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
      final String basicinfo =
          "SLA Alert: Your flow " + flowName + " failed to FINISH within "
              + duration + "<br/>";
      final String expected =
          "Here is details : <br/>" + "Flow " + flowName + " in execution "
              + execId + " is expected to FINISH within " + duration + " from "
              + fmt.print(new DateTime(flow.getStartTime())) + "<br/>";
      final String actual = "Actual flow status is " + flow.getStatus();
      return basicinfo + expected + actual;
    } else if (type.equals(SlaOption.TYPE_FLOW_SUCCEED)) {
      final String flowName =
          (String) slaOption.getInfo().get(SlaOption.INFO_FLOW_NAME);
      final String duration =
          (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
      final String basicinfo =
          "SLA Alert: Your flow " + flowName + " failed to SUCCEED within "
              + duration + "<br/>";
      final String expected =
          "Here is details : <br/>" + "Flow " + flowName + " in execution "
              + execId + " expected to FINISH within " + duration + " from "
              + fmt.print(new DateTime(flow.getStartTime())) + "<br/>";
      final String actual = "Actual flow status is " + flow.getStatus();
      return basicinfo + expected + actual;
    } else if (type.equals(SlaOption.TYPE_JOB_FINISH)) {
      final String jobName =
          (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      final String duration =
          (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
      return "SLA Alert: Your job " + jobName + " failed to FINISH within "
          + duration + " in execution " + execId;
    } else if (type.equals(SlaOption.TYPE_JOB_SUCCEED)) {
      final String jobName =
          (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      final String duration =
          (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
      return "SLA Alert: Your job " + jobName + " failed to SUCCEED within "
          + duration + " in execution " + execId;
    } else {
      return "Unrecognized SLA type " + type;
    }
  }

  public String getType() {
    return this.type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public Map<String, Object> getInfo() {
    return this.info;
  }

  public void setInfo(final Map<String, Object> info) {
    this.info = info;
  }

  public List<String> getActions() {
    return this.actions;
  }

  public void setActions(final List<String> actions) {
    this.actions = actions;
  }

  public Map<String, Object> toObject() {
    final HashMap<String, Object> slaObj = new HashMap<>();

    slaObj.put("type", this.type);
    slaObj.put("info", this.info);
    slaObj.put("actions", this.actions);

    return slaObj;
  }

  public Object toWebObject() {
    final HashMap<String, Object> slaObj = new HashMap<>();

    if (this.type.equals(TYPE_FLOW_FINISH) || this.type.equals(TYPE_FLOW_SUCCEED)) {
      slaObj.put("id", "");
    } else {
      slaObj.put("id", this.info.get(INFO_JOB_NAME));
    }
    slaObj.put("duration", this.info.get(INFO_DURATION));
    if (this.type.equals(TYPE_FLOW_FINISH) || this.type.equals(TYPE_JOB_FINISH)) {
      slaObj.put("rule", "FINISH");
    } else {
      slaObj.put("rule", "SUCCESS");
    }
    final List<String> actionsObj = new ArrayList<>();
    for (final String act : this.actions) {
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

}
