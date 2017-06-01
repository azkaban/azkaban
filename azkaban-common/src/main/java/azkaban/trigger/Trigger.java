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
  private final long submitTime;
  private final String submitUser;
  private final String source;
  private final Condition triggerCondition;
  private final Condition expireCondition;
  private final List<TriggerAction> actions;
  private final List<TriggerAction> expireActions;
  private int triggerId = -1;
  private long lastModifyTime;
  private TriggerStatus status = TriggerStatus.READY;
  private Map<String, Object> info = new HashMap<>();
  private Map<String, Object> context = new HashMap<>();
  private boolean resetOnTrigger = true;
  private boolean resetOnExpire = true;

  private long nextCheckTime = -1;

  @SuppressWarnings("unused")
  private Trigger() throws TriggerManagerException {
    throw new TriggerManagerException("Triggers should always be specified");
  }

  private Trigger(int triggerId, long lastModifyTime, long submitTime,
      String submitUser, String source, Condition triggerCondition,
      Condition expireCondition, List<TriggerAction> actions,
      List<TriggerAction> expireActions, Map<String, Object> info,
      Map<String, Object> context) {
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

  public static synchronized void setActionTypeLoader(ActionTypeLoader loader) {
    Trigger.actionTypeLoader = loader;
  }

  @SuppressWarnings("unchecked")
  public static Trigger fromJson(Object obj) throws Exception {

    if (actionTypeLoader == null) {
      throw new Exception("Trigger Action Type loader not initialized.");
    }

    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;

    Trigger trigger = null;
    try {
      logger.info("Decoding for " + JSONUtils.toJSON(obj));
      Condition triggerCond =
          Condition.fromJson(jsonObj.get("triggerCondition"));
      Condition expireCond = Condition.fromJson(jsonObj.get("expireCondition"));
      List<TriggerAction> actions = new ArrayList<>();
      List<Object> actionsJson = (List<Object>) jsonObj.get("actions");
      for (Object actObj : actionsJson) {
        Map<String, Object> oneActionJson = (HashMap<String, Object>) actObj;
        String type = (String) oneActionJson.get("type");
        TriggerAction act =
            actionTypeLoader.createActionFromJson(type,
                oneActionJson.get("actionJson"));
        actions.add(act);
      }
      List<TriggerAction> expireActions = new ArrayList<>();
      List<Object> expireActionsJson =
          (List<Object>) jsonObj.get("expireActions");
      for (Object expireActObj : expireActionsJson) {
        Map<String, Object> oneExpireActionJson =
            (HashMap<String, Object>) expireActObj;
        String type = (String) oneExpireActionJson.get("type");
        TriggerAction expireAct =
            actionTypeLoader.createActionFromJson(type,
                oneExpireActionJson.get("actionJson"));
        expireActions.add(expireAct);
      }
      boolean resetOnTrigger =
          Boolean.valueOf((String) jsonObj.get("resetOnTrigger"));
      boolean resetOnExpire =
          Boolean.valueOf((String) jsonObj.get("resetOnExpire"));
      String submitUser = (String) jsonObj.get("submitUser");
      String source = (String) jsonObj.get("source");
      long submitTime = Long.valueOf((String) jsonObj.get("submitTime"));
      long lastModifyTime =
          Long.valueOf((String) jsonObj.get("lastModifyTime"));
      int triggerId = Integer.valueOf((String) jsonObj.get("triggerId"));
      TriggerStatus status =
          TriggerStatus.valueOf((String) jsonObj.get("status"));
      Map<String, Object> info = (Map<String, Object>) jsonObj.get("info");
      Map<String, Object> context =
          (Map<String, Object>) jsonObj.get("context");
      if (context == null) {
        context = new HashMap<>();
      }
      for (ConditionChecker checker : triggerCond.getCheckers().values()) {
        checker.setContext(context);
      }
      for (ConditionChecker checker : expireCond.getCheckers().values()) {
        checker.setContext(context);
      }
      for (TriggerAction action : actions) {
        action.setContext(context);
      }
      for (TriggerAction action : expireActions) {
        action.setContext(context);
      }

      trigger = new Trigger.TriggerBuilder("azkaban",
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
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Failed to decode the trigger.", e);
      throw new Exception("Failed to decode the trigger.", e);
    }

    return trigger;
  }

  public void updateNextCheckTime() {
    this.nextCheckTime =
        Math.min(triggerCondition.getNextCheckTime(),
            expireCondition.getNextCheckTime());
  }

  public long getNextCheckTime() {
    return nextCheckTime;
  }

  public void setNextCheckTime(long nct) {
    this.nextCheckTime = nct;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public String getSubmitUser() {
    return submitUser;
  }

  public TriggerStatus getStatus() {
    return status;
  }

  public void setStatus(TriggerStatus status) {
    this.status = status;
  }

  public Condition getTriggerCondition() {
    return triggerCondition;
  }

  public Condition getExpireCondition() {
    return expireCondition;
  }

  public List<TriggerAction> getActions() {
    return actions;
  }

  public List<TriggerAction> getExpireActions() {
    return expireActions;
  }

  public Map<String, Object> getInfo() {
    return info;
  }

  public void setInfo(Map<String, Object> info) {
    this.info = info;
  }

  public Map<String, Object> getContext() {
    return context;
  }

  public void setContext(Map<String, Object> context) {
    this.context = context;
  }

  public boolean isResetOnTrigger() {
    return resetOnTrigger;
  }

  public void setResetOnTrigger(boolean resetOnTrigger) {
    this.resetOnTrigger = resetOnTrigger;
  }

  public boolean isResetOnExpire() {
    return resetOnExpire;
  }

  public void setResetOnExpire(boolean resetOnExpire) {
    this.resetOnExpire = resetOnExpire;
  }

  public long getLastModifyTime() {
    return lastModifyTime;
  }

  public void setLastModifyTime(long lastModifyTime) {
    this.lastModifyTime = lastModifyTime;
  }

  public int getTriggerId() {
    return triggerId;
  }

  public void setTriggerId(int id) {
    this.triggerId = id;
  }

  public boolean triggerConditionMet() {
    return triggerCondition.isMet();
  }

  public boolean expireConditionMet() {
    return expireCondition.isMet();
  }

  public void resetTriggerConditions() {
    triggerCondition.resetCheckers();
    updateNextCheckTime();
  }

  public void resetExpireCondition() {
    expireCondition.resetCheckers();
    updateNextCheckTime();
  }

  public List<TriggerAction> getTriggerActions() {
    return actions;
  }

  public Map<String, Object> toJson() {
    Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("triggerCondition", triggerCondition.toJson());
    jsonObj.put("expireCondition", expireCondition.toJson());
    List<Object> actionsJson = new ArrayList<>();
    for (TriggerAction action : actions) {
      Map<String, Object> oneActionJson = new HashMap<>();
      oneActionJson.put("type", action.getType());
      oneActionJson.put("actionJson", action.toJson());
      actionsJson.add(oneActionJson);
    }
    jsonObj.put("actions", actionsJson);
    List<Object> expireActionsJson = new ArrayList<>();
    for (TriggerAction expireAction : expireActions) {
      Map<String, Object> oneExpireActionJson = new HashMap<>();
      oneExpireActionJson.put("type", expireAction.getType());
      oneExpireActionJson.put("actionJson", expireAction.toJson());
      expireActionsJson.add(oneExpireActionJson);
    }
    jsonObj.put("expireActions", expireActionsJson);

    jsonObj.put("resetOnTrigger", String.valueOf(resetOnTrigger));
    jsonObj.put("resetOnExpire", String.valueOf(resetOnExpire));
    jsonObj.put("submitUser", submitUser);
    jsonObj.put("source", source);
    jsonObj.put("submitTime", String.valueOf(submitTime));
    jsonObj.put("lastModifyTime", String.valueOf(lastModifyTime));
    jsonObj.put("triggerId", String.valueOf(triggerId));
    jsonObj.put("status", status.toString());
    jsonObj.put("info", info);
    jsonObj.put("context", context);
    return jsonObj;
  }

  public String getSource() {
    return source;
  }

  public String getDescription() {
    StringBuffer actionsString = new StringBuffer();
    for (TriggerAction act : actions) {
      actionsString.append(", ");
      actionsString.append(act.getDescription());
    }
    return "Trigger from " + getSource() + " with trigger condition of "
        + triggerCondition.getExpression() + " and expire condition of "
        + expireCondition.getExpression() + actionsString;
  }

  public void stopCheckers() {
    for (ConditionChecker checker : triggerCondition.getCheckers().values()) {
      checker.stopChecker();
    }
    for (ConditionChecker checker : expireCondition.getCheckers().values()) {
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

    public TriggerBuilder(String submitUser,
                          String source,
                          Condition triggerCondition,
                          Condition expireCondition,
                          List<TriggerAction> actions) {
      this.submitUser = submitUser;
      this.source = source;
      this.triggerCondition = triggerCondition;
      this.actions = actions;
      this.expireCondition = expireCondition;
      long now = DateTime.now().getMillis();
      this.submitTime = now;
      this.lastModifyTime = now;
    }

    public TriggerBuilder setId(int id) {
      this.triggerId = id;
      return this;
    }

    public TriggerBuilder setSubmitTime(long time) {
      this.submitTime = time;
      return this;
    }

    public TriggerBuilder setLastModifyTime(long time) {
      this.lastModifyTime = time;
      return this;
    }

    public TriggerBuilder setExpireActions(List<TriggerAction> actions) {
      this.expireActions = actions;
      return this;
    }

    public TriggerBuilder setInfo(Map<String, Object> info) {
      this.info = info;
      return this;
    }

    public TriggerBuilder setContext(Map<String, Object> context) {
      this.context = context;
      return this;
    }

    public Trigger build() {
      return new Trigger(triggerId,
                        lastModifyTime,
                        submitTime,
                        submitUser,
                        source,
                        triggerCondition,
                        expireCondition,
                        actions,
                        expireActions,
                        info,
                        context);
    }
  }

}
