/*
 * Copyright 2019 LinkedIn Corp.
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
import azkaban.sla.SlaType.ComponentType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * SLA option, which can be associated with a flow or job.
 */
public class SlaOption {
  public static final String ALERT_TYPE_EMAIL = "email";
  public static final String ACTION_CANCEL_FLOW = "SlaCancelFlow";
  public static final String ACTION_ALERT = "SlaAlert";
  public static final String ACTION_KILL_JOB = "SlaKillJob";

  public static final String WEB_ID = "id";
  public static final String WEB_DURATION = "duration";
  public static final String WEB_STATUS = "rule";
  public static final String WEB_ACTIONS = "actions";
  public static final String WEB_ACTION_EMAIL = "EMAIL";
  public static final String WEB_ACTION_KILL = "KILL";

  private static final DateTimeFormatter fmt = DateTimeFormat
      .forPattern("MM/dd, YYYY HH:mm");

  final private SlaType type;
  final private String flowName;
  final private String jobName;
  final private String duration;
  final private boolean alert;
  final private boolean kill;
  final private List<String> emails;

  /**
   * Constructor.
   *
   * @param type the SLA type.
   * @param flowName The name of the flow.
   * @param jobName The name of the job, if the SLA is for a job.
   * @param duration The duration (time to wait before the SLA would take effect).
   * @param alert if the user should be alerted for the SLA.
   * @param kill if the job or flow should be killed or canceled for the SLA.
   * @param emails list of emails to send an alert to, for the SLA.
   */
  public SlaOption(final SlaType type,
      String flowName, String jobName, String duration, boolean alert, boolean kill,
      List<String> emails) {
    this.type = type;
    this.flowName = flowName;
    this.jobName = jobName;
    this.duration = duration;
    this.alert = alert;
    this.kill = kill;
    this.emails = emails;
  }

  /**
   * Construct the SLA Option from the original SlaOption, which has been renamed to
   * {@link SlaOptionDeprecated}.
   */
  public SlaOption(SlaOptionDeprecated slaOption) {
    String type = slaOption.getType();
    switch (type) {
      case SlaOptionDeprecated.TYPE_FLOW_FINISH:
        this.type = SlaType.FLOW_FINISH;
        break;
      case SlaOptionDeprecated.TYPE_FLOW_SUCCEED:
        this.type = SlaType.FLOW_SUCCEED;
        break;
      case SlaOptionDeprecated.TYPE_JOB_FINISH:
        this.type = SlaType.JOB_FINISH;
        break;
      case SlaOptionDeprecated.TYPE_JOB_SUCCEED:
        this.type = SlaType.JOB_SUCCEED;
        break;
      default:
        throw new IllegalArgumentException("Unrecognized type " + type);
    }
    this.flowName = (String)slaOption.getInfo().get(SlaOptionDeprecated.INFO_FLOW_NAME);
    this.jobName = (String)slaOption.getInfo().get(SlaOptionDeprecated.INFO_JOB_NAME);
    this.duration = (String)slaOption.getInfo().get(SlaOptionDeprecated.INFO_DURATION);

    boolean alert = false;
    boolean kill = false;
    for (String action: slaOption.getActions()) {
      switch (action) {
        case SlaOptionDeprecated.ACTION_ALERT:
          alert = true;
          break;
        case SlaOptionDeprecated.ACTION_CANCEL_FLOW:
        case SlaOptionDeprecated.ACTION_KILL_JOB:
          kill = true;
          break;
      }
    }
    this.alert = alert;
    this.kill = kill;

    this.emails = (List<String>)slaOption.getInfo().get(SlaOptionDeprecated.INFO_EMAIL_LIST);
  }

  public SlaType getType() {
    return type;
  }

  public String getJobName() {
    return jobName;
  }

  public String getDuration() {
    return duration;
  }

  public boolean isAlert() {
    return alert;
  }

  public boolean isKill() {
    return kill;
  }

  public String getFlowName() {
    return flowName;
  }

  public List<String> getEmails() {
    return emails;
  }

  /**
   * Convert the SLA option to the original JSON format, used by {@link SlaOptionDeprecated}.
   *
   * @return the JSON format for {@link SlaOptionDeprecated}.
   */
  public Map<String, Object> toObject() {
    final List<String> slaActions = new ArrayList<>();
    final Map<String, Object> slaInfo = new HashMap<>();

    slaInfo.put(SlaOptionDeprecated.INFO_FLOW_NAME, this.flowName);
    if (this.alert) {
      slaActions.add(SlaOptionDeprecated.ACTION_ALERT);
      slaInfo.put(SlaOptionDeprecated.ALERT_TYPE, ALERT_TYPE_EMAIL);
    }
    if (this.kill) {
      if (this.type.getComponent() == ComponentType.FLOW) {
        slaActions.add(SlaOptionDeprecated.ACTION_CANCEL_FLOW);
      } else { // JOB
        slaActions.add(SlaOptionDeprecated.ACTION_KILL_JOB);
      }
    }
    if (this.type.getComponent() == ComponentType.JOB) {
      slaInfo.put(SlaOptionDeprecated.INFO_JOB_NAME, this.jobName);
    }

    String slaType;
    switch (this.type) {
      case FLOW_FINISH:
            slaType = SlaOptionDeprecated.TYPE_FLOW_FINISH;
            break;
      case FLOW_SUCCEED:
            slaType = SlaOptionDeprecated.TYPE_FLOW_SUCCEED;
            break;
      case JOB_FINISH:
             slaType = SlaOptionDeprecated.TYPE_JOB_FINISH;
            break;
            case JOB_SUCCEED:
            slaType = SlaOptionDeprecated.TYPE_JOB_SUCCEED;
            break;
          default:
            throw new IllegalStateException("unsupported SLA type " + this.type.getName());
    }

    slaInfo.put(SlaOptionDeprecated.INFO_DURATION, this.duration);
    slaInfo.put(SlaOptionDeprecated.INFO_EMAIL_LIST, emails);

    SlaOptionDeprecated slaOption = new SlaOptionDeprecated(slaType, slaActions, slaInfo);
    return slaOption.toObject();

  }

  /**
   * Convert the original JSON format, used by {@link SlaOptionDeprecated}, to an SLA option.
   *
   * @param json the original JSON format for {@link SlaOptionDeprecated}.
   * @return the SLA option.
   */
  static public SlaOption fromObject(Object json) {
    return new SlaOption(SlaOptionDeprecated.fromObject(json));
  }

  /** @return the web object representation for the SLA option. */
  public Object toWebObject() {
    final HashMap<String, Object> slaObj = new HashMap<>();

    if (this.type.getComponent() == SlaType.ComponentType.FLOW) {
      slaObj.put(WEB_ID, "");
    } else {
      slaObj.put(WEB_ID, this.jobName);
    }
    slaObj.put(WEB_DURATION, this.duration);
    slaObj.put(WEB_STATUS, this.type.getStatus().toString());

    final List<String> actionsObj = new ArrayList<>();
    if (this.alert) {
      actionsObj.add(WEB_ACTION_EMAIL);
    }
    if (this.kill) {
      actionsObj.add(WEB_ACTION_KILL);
    }
    slaObj.put(WEB_ACTIONS, actionsObj);

    return slaObj;
  }

  /**
   * Construct the message for the SLA.
   *
   * @param flow the executable flow.
   * @return the SLA message.
   */
  public String createSlaMessage(final ExecutableFlow flow) {
    final int execId = flow.getExecutionId();
    switch (this.type.getComponent()) {
      case FLOW:
        final String basicinfo =
            "SLA Alert: Your flow " + this.flowName + " failed to " + this.type.getStatus()
                + " within " + this.duration + "<br/>";
        final String expected =
            "Here are details : <br/>" + "Flow " + this.flowName + " in execution "
                + execId + " is expected to FINISH within " + this.duration + " from "
                + fmt.print(new DateTime(flow.getStartTime())) + "<br/>";
        final String actual = "Actual flow status is " + flow.getStatus();
        return basicinfo + expected + actual;
       case JOB:
         return "SLA Alert: Your job " + this.jobName + " failed to " + this.type.getStatus()
             + " within " + this.duration + " in execution " + execId;
      default:
        return "Unrecognized SLA component type " + this.type.getComponent();
    }
  }

  /**
   * @param options a list of SLA options.
   * @return the job level SLA options.
   */
  static public List<SlaOption> getJobLevelSLAOptions(List<SlaOption> options) {
    return options.stream().filter(rule -> rule.type.getComponent() == SlaType.ComponentType.JOB).collect
        (Collectors.toList());
  }

  /**
   * @param options a list of SLA options.
   * @return the flow level SLA options.
   */
  static public List<SlaOption> getFlowLevelSLAOptions(List<SlaOption> options) {
    return options.stream().filter(rule -> rule.type.getComponent() == SlaType.ComponentType.FLOW).collect
        (Collectors.toList());
  }
}
