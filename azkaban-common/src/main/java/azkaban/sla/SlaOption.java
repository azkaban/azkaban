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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private static final char MINUTE_DURATION_UNIT = 'm';
  private static final char INVALID_DURATION_UNIT = 'n';

  private static final DateTimeFormatter fmt = DateTimeFormat
      .forPattern("MM/dd, YYYY HH:mm");

  final private SlaType type;
  final private String flowName;
  final private String jobName;
  final private Duration duration;
  final private Set<SlaAction> actions;
  final private ImmutableList<String> emails;

  /**
   * Constructor.
   *
   * @param type the SLA type.
   * @param flowName The name of the flow.
   * @param jobName The name of the job, if the SLA is for a job.
   * @param duration The duration (time to wait before the SLA would take effect).
   * @param actions actions to take for the SLA.
   * @param emails list of emails to send an alert to, for the SLA.
   */
  public SlaOption(final SlaType type,
      String flowName, String jobName, Duration duration, Set<SlaAction> actions,
      List<String> emails) {
    Preconditions.checkNotNull(type, "type is null");
    Preconditions.checkNotNull(actions, "actions is null");
    Preconditions.checkState(actions.size() > 0, "An action must be specified for the SLA");
    this.type = type;
    this.flowName = Preconditions.checkNotNull(flowName, "flowName is null");
    this.jobName = jobName;
    this.duration = Preconditions.checkNotNull(duration, "duration is null");
    this.actions = ImmutableSet.copyOf(actions);
    if (emails == null) {
      this.emails = ImmutableList.of();
    } else {
      this.emails = ImmutableList.copyOf(emails);
    }
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
    this.duration = parseDuration((String)slaOption.getInfo().get(SlaOptionDeprecated
        .INFO_DURATION));

    Set<SlaAction> actions = new HashSet<>();
    for (String action: slaOption.getActions()) {
      switch (action) {
        case SlaOptionDeprecated.ACTION_ALERT:
          actions.add(SlaAction.ALERT);
          break;
        case SlaOptionDeprecated.ACTION_CANCEL_FLOW:
        case SlaOptionDeprecated.ACTION_KILL_JOB:
          actions.add(SlaAction.KILL);
          break;
      }
    }
    this.actions = ImmutableSet.copyOf(actions);

    this.emails = ImmutableList.copyOf((List<String>)slaOption.getInfo().get(SlaOptionDeprecated
        .INFO_EMAIL_LIST));
  }

  public static List<Object> convertToObjects(List<SlaOption> slaOptions) {
    if (slaOptions != null) {
      final List<Object> slaOptionsObject = new ArrayList<>();
      for (final SlaOption sla : slaOptions) {
        slaOptionsObject.add(sla.toObject());
      }
      return slaOptionsObject;
    }
    return null;
  }

  private Duration parseDuration(final String durationStr) {
    final char durationUnit = durationStr.charAt(durationStr.length() - 1);
    if (durationStr.equals("null") || durationUnit == INVALID_DURATION_UNIT) {
      return null;
    }

    if (durationUnit != MINUTE_DURATION_UNIT) {
      throw new IllegalArgumentException("Invalid SLA duration unit '"
          + durationUnit);
    }
    final int durationInt =
        Integer.parseInt(durationStr.substring(0, durationStr.length() - 1));
    return Duration.ofMinutes(durationInt);
  }

  private String durationToString(Duration duration) {
    return Long.toString(duration.toMinutes()) + MINUTE_DURATION_UNIT;
  }

  public SlaType getType() {
    return type;
  }

  public String getJobName() {
    return jobName;
  }

  public Duration getDuration() {
    return duration;
  }

  public boolean hasAlert() {
    return actions.contains(SlaAction.ALERT);
  }

  public boolean hasKill() {
    return actions.contains(SlaAction.KILL);
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
    if (hasAlert()) {
      slaActions.add(SlaOptionDeprecated.ACTION_ALERT);
      slaInfo.put(SlaOptionDeprecated.ALERT_TYPE, ALERT_TYPE_EMAIL);
    }
    if (hasKill()) {
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

    slaInfo.put(SlaOptionDeprecated.INFO_DURATION, durationToString(this.duration));
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
    slaObj.put(WEB_DURATION, durationToString(this.duration));
    slaObj.put(WEB_STATUS, this.type.getStatus().toString());

    final List<String> actionsObj = new ArrayList<>();
    if (hasAlert()) {
      actionsObj.add(WEB_ACTION_EMAIL);
    }
    if (hasKill()) {
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
    final String durationStr = durationToString(this.duration);
    switch (this.type.getComponent()) {
      case FLOW:
        final String basicinfo =
            "SLA Alert: Your flow " + this.flowName + " failed to " + this.type.getStatus()
                + " within " + durationStr + "<br/>";
        final String expected =
            "Here are details : <br/>" + "Flow " + this.flowName + " in execution "
                + execId + " is expected to FINISH within " + durationStr + " from "
                + fmt.print(new DateTime(flow.getStartTime())) + "<br/>";
        final String actual = "Actual flow status is " + flow.getStatus();
        return basicinfo + expected + actual;
       case JOB:
         return "SLA Alert: Your job " + this.jobName + " failed to " + this.type.getStatus()
             + " within " + durationStr + " in execution " + execId;
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

  /**
   * Builder for {@link SlaOption}.
   */
  public static class SlaOptionBuilder {
    final private SlaType type;
    final private String flowName;
    private String jobName = null;
    final private Duration duration;
    private Set<SlaAction> actions;
    private ImmutableList<String> emails = null;

    public SlaOptionBuilder(SlaType type, String flowName, Duration duration) {
      this.type = type;
      this.flowName = flowName;
      this.duration = duration;
      this.actions = new HashSet<>();
    }

    public SlaOptionBuilder setJobName(String jobName) {
      this.jobName = jobName;
      return this;
    }

    public SlaOptionBuilder setAlert() {
      actions.add(SlaAction.ALERT);
      return this;
    }

    public SlaOptionBuilder setKill() {
      actions.add(SlaAction.KILL);
      return this;
    }

    public SlaOptionBuilder setActions(Set<SlaAction> actions) {
      this.actions.addAll(actions);
      return this;
    }

    public SlaOptionBuilder setEmails(List<String> emails) {
      this.emails = ImmutableList.copyOf(emails);
      return this;
    }

    public SlaOption createSlaOption() {
      return new SlaOption(type, flowName, jobName, duration, actions, emails);
    }
  }
}
