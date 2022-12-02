/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.trigger;

import static java.util.Objects.requireNonNull;

import azkaban.scheduler.MissedSchedulesManager;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.JSONUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;


public class Trigger {

  private static final Logger logger = Logger.getLogger(Trigger.class);
  private static ActionTypeLoader actionTypeLoader;
  private static MissedSchedulesManager missedSchedulesManager;
  private final long submitTime;
  private final String submitUser;
  private final String source;
  private final List<TriggerAction> actions;
  private final List<TriggerAction> expireActions;
  private Condition expireCondition;
  private Condition triggerCondition;
  private int triggerId = -1;
  private long lastModifyTime;
  private TriggerStatus status = TriggerStatus.READY;
  private Map<String, Object> info = new HashMap<>();
  private Map<String, Object> context = new HashMap<>();
  private boolean resetOnTrigger = true;
  private boolean resetOnExpire = true;

  private long nextCheckTime = -1;

  private Trigger() throws TriggerManagerException {
    throw new TriggerManagerException("Triggers should always be specified");
  }

  private Trigger(final int triggerId, final long lastModifyTime, final long submitTime,
      final String submitUser, final String source, final Condition triggerCondition,
      final Condition expireCondition, final List<TriggerAction> actions,
      final List<TriggerAction> expireActions, final Map<String, Object> info,
      final Map<String, Object> context) {
    requireNonNull(submitUser);
    requireNonNull(source);
    requireNonNull(triggerCondition);
    requireNonNull(expireActions);
    requireNonNull(info);
    requireNonNull(context);

    this.lastModifyTime = lastModifyTime;
    this.submitTime = submitTime;
    this.submitUser = submitUser;
    this.source = source;
    this.triggerCondition = triggerCondition;
    this.expireCondition = expireCondition;
    this.actions = actions;
    this.triggerId = triggerId;
    this.expireActions = expireActions;
    this.info = info;
    this.context = context;
  }

  public static ActionTypeLoader getActionTypeLoader() {
    return actionTypeLoader;
  }

  public static synchronized void setActionTypeLoader(final ActionTypeLoader loader) {
    Trigger.actionTypeLoader = loader;
  }

  public static synchronized void setMissedScheduleManager(final MissedSchedulesManager missedSchedulesManager) {
    Trigger.missedSchedulesManager = missedSchedulesManager;
  }

  public static Trigger fromJson(final Object obj) throws Exception {

    if (actionTypeLoader == null) {
      throw new Exception("Trigger Action Type loader not initialized.");
    }
    final Map<String, Object> jsonObj = (HashMap<String, Object>) obj;

    Trigger trigger = null;
    try {
      logger.info("Decoding for " + JSONUtils.toJSON(obj));
      final Condition triggerCond = Condition.fromJson(jsonObj.get("triggerCondition"));
      final Condition expireCond = Condition.fromJson(jsonObj.get("expireCondition"));
      final List<TriggerAction> actions = new ArrayList<>();
      final List<Object> actionsJson = (List<Object>) jsonObj.get("actions");
      for (final Object actObj : actionsJson) {
        final Map<String, Object> oneActionJson = (HashMap<String, Object>) actObj;
        final String type = (String) oneActionJson.get("type");
        final TriggerAction act =
            actionTypeLoader.createActionFromJson(type,
                oneActionJson.get("actionJson"));
        actions.add(act);
      }
      final List<TriggerAction> expireActions = new ArrayList<>();
      final List<Object> expireActionsJson =
          (List<Object>) jsonObj.get("expireActions");
      for (final Object expireActObj : expireActionsJson) {
        final Map<String, Object> oneExpireActionJson =
            (HashMap<String, Object>) expireActObj;
        final String type = (String) oneExpireActionJson.get("type");
        final TriggerAction expireAct =
            actionTypeLoader.createActionFromJson(type,
                oneExpireActionJson.get("actionJson"));
        expireActions.add(expireAct);
      }
      final boolean resetOnTrigger =
          Boolean.valueOf((String) jsonObj.get("resetOnTrigger"));
      final boolean resetOnExpire =
          Boolean.valueOf((String) jsonObj.get("resetOnExpire"));
      final String submitUser = (String) jsonObj.get("submitUser");
      final String source = (String) jsonObj.get("source");
      final long submitTime = Long.valueOf((String) jsonObj.get("submitTime"));
      final long lastModifyTime =
          Long.valueOf((String) jsonObj.get("lastModifyTime"));
      final int triggerId = Integer.valueOf((String) jsonObj.get("triggerId"));
      final TriggerStatus status =
          TriggerStatus.valueOf((String) jsonObj.get("status"));
      final Map<String, Object> info = (Map<String, Object>) jsonObj.get("info");
      Map<String, Object> context =
          (Map<String, Object>) jsonObj.get("context");
      if (context == null) {
        context = new HashMap<>();
      }
      for (final ConditionChecker checker : triggerCond.getCheckers().values()) {
        checker.setContext(context);
      }
      for (final ConditionChecker checker : expireCond.getCheckers().values()) {
        checker.setContext(context);
      }
      for (final TriggerAction action : actions) {
        action.setContext(context);
      }
      for (final TriggerAction action : expireActions) {
        action.setContext(context);
      }

      trigger = new Trigger.TriggerBuilder(submitUser,
          source,
          triggerCond,
          expireCond,
          actions)
          .setId(triggerId)
          .setLastModifyTime(lastModifyTime)
          .setSubmitTime(submitTime)
          .setExpireActions(expireActions)
          .setInfo(info)
          .setContext(context)
          .build();

      trigger.setResetOnExpire(resetOnExpire);
      trigger.setResetOnTrigger(resetOnTrigger);
      trigger.setStatus(status);
    } catch (final Exception e) {
      e.printStackTrace();
      logger.error("Failed to decode the trigger.", e);
      throw new Exception("Failed to decode the trigger.", e);
    }

    return trigger;
  }

  public void updateNextCheckTime() {
    this.nextCheckTime = Math.min(this.triggerCondition.getNextCheckTime(),
        this.expireCondition.getNextCheckTime());
  }

  public long getNextCheckTime() {
    return this.nextCheckTime;
  }

  public void setNextCheckTime(final long nct) {
    this.nextCheckTime = nct;
  }

  public long getSubmitTime() {
    return this.submitTime;
  }

  public String getSubmitUser() {
    return this.submitUser;
  }

  public TriggerStatus getStatus() {
    return this.status;
  }

  public void setStatus(final TriggerStatus status) {
    this.status = status;
  }

  public Condition getTriggerCondition() {
    return this.triggerCondition;
  }

  public void setTriggerCondition(final Condition triggerCondition) {
    this.triggerCondition = triggerCondition;
  }

  public Condition getExpireCondition() {
    return this.expireCondition;
  }

  public void setExpireCondition(final Condition expireCondition) {
    this.expireCondition = expireCondition;
  }

  public List<TriggerAction> getActions() {
    return this.actions;
  }

  public List<TriggerAction> getExpireActions() {
    return this.expireActions;
  }

  public Map<String, Object> getInfo() {
    return this.info;
  }

  public void setInfo(final Map<String, Object> info) {
    this.info = info;
  }

  public Map<String, Object> getContext() {
    return this.context;
  }

  public void setContext(final Map<String, Object> context) {
    this.context = context;
  }

  public boolean isResetOnTrigger() {
    return this.resetOnTrigger;
  }

  public void setResetOnTrigger(final boolean resetOnTrigger) {
    this.resetOnTrigger = resetOnTrigger;
  }

  public boolean isResetOnExpire() {
    return this.resetOnExpire;
  }

  public void setResetOnExpire(final boolean resetOnExpire) {
    this.resetOnExpire = resetOnExpire;
  }

  public long getLastModifyTime() {
    return this.lastModifyTime;
  }

  public void setLastModifyTime(final long lastModifyTime) {
    this.lastModifyTime = lastModifyTime;
  }

  public int getTriggerId() {
    return this.triggerId;
  }

  public void setTriggerId(final int id) {
    this.triggerId = id;
  }

  public boolean triggerConditionMet() {
    return this.triggerCondition.isMet();
  }

  public boolean expireConditionMet() {
    return this.expireCondition.isMet();
  }

  public void resetTriggerConditions() {
    this.triggerCondition.resetCheckers();
    updateNextCheckTime();
  }

  public void sendTaskToMissedScheduleManager() {
    if (this.triggerCondition.getMissedCheckTimes().isEmpty()) {
      return;
    }
    for (final TriggerAction action : actions) {
      if (action instanceof ExecuteFlowAction) {
        // when successfully send task to missedScheduleManager, clear the missed schedule times
        if (missedSchedulesManager.addMissedSchedule(
            this.triggerCondition.getMissedCheckTimes(), (ExecuteFlowAction) action, false)) {
          this.triggerCondition.getMissedCheckTimes().clear();
        } else {
          logger.error("failed to add miss schedule task for trigger " + this);
        }
      }
    }
  }

  public void resetExpireCondition() {
    this.expireCondition.resetCheckers();
    updateNextCheckTime();
  }

  public List<TriggerAction> getTriggerActions() {
    return this.actions;
  }

  public Map<String, Object> toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("triggerCondition", this.triggerCondition.toJson());
    jsonObj.put("expireCondition", this.expireCondition.toJson());
    final List<Object> actionsJson = new ArrayList<>();
    for (final TriggerAction action : this.actions) {
      final Map<String, Object> oneActionJson = new HashMap<>();
      oneActionJson.put("type", action.getType());
      oneActionJson.put("actionJson", action.toJson());
      actionsJson.add(oneActionJson);
    }
    jsonObj.put("actions", actionsJson);
    final List<Object> expireActionsJson = new ArrayList<>();
    for (final TriggerAction expireAction : this.expireActions) {
      final Map<String, Object> oneExpireActionJson = new HashMap<>();
      oneExpireActionJson.put("type", expireAction.getType());
      oneExpireActionJson.put("actionJson", expireAction.toJson());
      expireActionsJson.add(oneExpireActionJson);
    }
    jsonObj.put("expireActions", expireActionsJson);

    jsonObj.put("resetOnTrigger", String.valueOf(this.resetOnTrigger));
    jsonObj.put("resetOnExpire", String.valueOf(this.resetOnExpire));
    jsonObj.put("submitUser", this.submitUser);
    jsonObj.put("source", this.source);
    jsonObj.put("submitTime", String.valueOf(this.submitTime));
    jsonObj.put("lastModifyTime", String.valueOf(this.lastModifyTime));
    jsonObj.put("triggerId", String.valueOf(this.triggerId));
    jsonObj.put("status", this.status.toString());
    jsonObj.put("info", this.info);
    jsonObj.put("context", this.context);
    return jsonObj;
  }

  public String getSource() {
    return this.source;
  }

  public String getDescription() {
    final StringBuffer actionsString = new StringBuffer();
    for (final TriggerAction act : this.actions) {
      actionsString.append(", ");
      actionsString.append(act.getDescription());
    }
    return "Trigger from " + getSource() + " with trigger condition of "
        + this.triggerCondition.getExpression() + " and expire condition of "
        + this.expireCondition.getExpression() + actionsString;
  }

  public void stopCheckers() {
    for (final ConditionChecker checker : this.triggerCondition.getCheckers().values()) {
      checker.stopChecker();
    }
    for (final ConditionChecker checker : this.expireCondition.getCheckers().values()) {
      checker.stopChecker();
    }
  }

  @Override
  public String toString() {
    return "Trigger Id: " + getTriggerId() + ", Description: " + getDescription();
  }

  public static class TriggerBuilder {

    private final String submitUser;
    private final String source;
    private final TriggerStatus status = TriggerStatus.READY;
    private final Condition triggerCondition;
    private final List<TriggerAction> actions;
    private final Condition expireCondition;
    private int triggerId = -1;
    private long lastModifyTime;
    private long submitTime;
    private List<TriggerAction> expireActions = new ArrayList<>();

    private Map<String, Object> info = new HashMap<>();
    private Map<String, Object> context = new HashMap<>();

    public TriggerBuilder(final String submitUser,
        final String source,
        final Condition triggerCondition,
        final Condition expireCondition,
        final List<TriggerAction> actions) {
      this.submitUser = submitUser;
      this.source = source;
      this.triggerCondition = triggerCondition;
      this.actions = actions;
      this.expireCondition = expireCondition;
      final long now = DateTime.now().getMillis();
      this.submitTime = now;
      this.lastModifyTime = now;
    }

    public TriggerBuilder setId(final int id) {
      this.triggerId = id;
      return this;
    }

    public TriggerBuilder setSubmitTime(final long time) {
      this.submitTime = time;
      return this;
    }

    public TriggerBuilder setLastModifyTime(final long time) {
      this.lastModifyTime = time;
      return this;
    }

    public TriggerBuilder setExpireActions(final List<TriggerAction> actions) {
      this.expireActions = actions;
      return this;
    }

    public TriggerBuilder setInfo(final Map<String, Object> info) {
      this.info = info;
      return this;
    }

    public TriggerBuilder setContext(final Map<String, Object> context) {
      this.context = context;
      return this;
    }

    public Trigger build() {
      return new Trigger(this.triggerId,
          this.lastModifyTime,
          this.submitTime,
          this.submitUser,
          this.source,
          this.triggerCondition,
          this.expireCondition,
          this.actions,
          this.expireActions,
          this.info,
          this.context);
    }
  }

}
