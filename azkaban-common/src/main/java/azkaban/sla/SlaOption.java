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

import azkaban.sla.SlaType.ComponentType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.GsonBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

  final private SlaType type;
  final private String flowName;
  final private String jobName;
  final private Duration duration;
  final private Set<SlaAction> actions;
  final private ImmutableList<String> emails;
  final private ImmutableMap<String, Map<String, List<String>>> alertersConfigs;

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
      List<String> emails, Map<String, Map<String, List<String>>> alertersConfigs) {
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

    if (alertersConfigs == null) {
      this.alertersConfigs = ImmutableMap.of();
    } else {
      this.alertersConfigs = ImmutableMap.copyOf(alertersConfigs);
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
    this.flowName = (String) slaOption.getInfo().get(SlaOptionDeprecated.INFO_FLOW_NAME);
    this.jobName = (String) slaOption.getInfo().get(SlaOptionDeprecated.INFO_JOB_NAME);
    this.duration = parseDuration((String) slaOption.getInfo().get(SlaOptionDeprecated
        .INFO_DURATION));

    Set<SlaAction> actions = new HashSet<>();
    for (String action : slaOption.getActions()) {
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

    this.emails = ImmutableList.copyOf(
        (List<String>) slaOption.getInfo().get(SlaOptionDeprecated.INFO_EMAIL_LIST));

    Map<String, Map<String, List<String>>> alertersConfs =
        (Map<String, Map<String, List<String>>>) slaOption.getInfo()
            .getOrDefault(SlaOptionDeprecated.INFO_ALERTERS_CONFIGS, ImmutableMap.of());
    this.alertersConfigs = ImmutableMap.copyOf(alertersConfs);
  }

  public static List<Object> convertToObjects(List<SlaOption> slaOptions) {
    if (slaOptions != null) {
      final List<Object> slaOptionsObject = new ArrayList<>();
      for (final SlaOption sla : slaOptions) {
        if (sla == null) continue;
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

  public String durationToString(Duration duration) {
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

  public Map<String, Map<String, List<String>>> getAlertersConfigs() {
    return this.alertersConfigs;
  }

  /**
   * Check the SlaType's ComponentType of this SlaOption's
   *
   * @param componentType component Type
   * @return true/false
   */
  public boolean isComponentType (SlaType.ComponentType componentType) {
    return this.type.getComponent() == componentType;
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
    slaInfo.put(SlaOptionDeprecated.INFO_EMAIL_LIST, this.emails);
    slaInfo.put(SlaOptionDeprecated.INFO_ALERTERS_CONFIGS, this.alertersConfigs);

    SlaOptionDeprecated slaOption = new SlaOptionDeprecated(slaType, slaActions, slaInfo);
    return slaOption.toObject();

  }

  public String toJSON() {
    return new GsonBuilder().setPrettyPrinting().create().toJson(toObject());
  }

  /**
   * Convert the original JSON format, used by {@link SlaOptionDeprecated}, to an SLA option.
   *
   * @param json the original JSON format for {@link SlaOptionDeprecated}.
   * @return the SLA option.
   */
  public static SlaOption fromObject(Object json) {
    return new SlaOption(SlaOptionDeprecated.fromObject(json));
  }

  /**
   * @return the web object representation for the SLA option.
   */
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
   * @param options a list of SLA options.
   * @return the job level SLA options.
   */
  public static List<SlaOption> getJobLevelSLAOptions(List<SlaOption> options) {
    return filterSLAOptionsByComponentType(options, ComponentType.JOB);
  }

  /**
   * @param options a list of SLA options.
   * @return the flow level SLA options.
   */
  public static List<SlaOption> getFlowLevelSLAOptions(List<SlaOption> options) {
    return filterSLAOptionsByComponentType(options, ComponentType.FLOW);
  }

  private static List<SlaOption> filterSLAOptionsByComponentType(
      List<SlaOption> options, ComponentType componentType) {
    return options.stream()
        .filter(option -> option.isComponentType(componentType))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "SlaOption{" +
        "type=" + this.type +
        ", flowName='" + this.flowName + '\'' +
        ", jobName='" + this.jobName + '\'' +
        ", duration=" + this.duration +
        ", actions=" + this.actions +
        ", emails=" + this.emails +
        ", alertersConfigs=" + this.alertersConfigs +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SlaOption slaOption = (SlaOption) o;
    return type == slaOption.type &&
        Objects.equals(flowName, slaOption.flowName) &&
        Objects.equals(jobName, slaOption.jobName) &&
        Objects.equals(duration, slaOption.duration) &&
        Objects.equals(actions, slaOption.actions) &&
        Objects.equals(emails, slaOption.emails) &&
        Objects.equals(alertersConfigs, slaOption.alertersConfigs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, flowName, jobName, duration, actions, emails, alertersConfigs);
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
    private List<String> emails = null;
    private Map<String, Map<String, List<String>>> alertersConfigs = null;

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
      this.emails = emails;
      return this;
    }

    public SlaOptionBuilder setAlertersConfigs(Map<String, Map<String, List<String>>> alertersConfigs) {
      this.alertersConfigs = alertersConfigs;
      return this;
    }

    public SlaOption createSlaOption() {
      return new SlaOption(type, flowName, jobName, duration, actions, emails, alertersConfigs);
    }
  }
}
