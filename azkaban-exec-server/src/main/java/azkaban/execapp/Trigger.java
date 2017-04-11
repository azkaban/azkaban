package azkaban.execapp;

import azkaban.trigger.Condition;
import azkaban.trigger.TriggerAction;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;


public class Trigger implements Runnable {
  private static Logger logger = Logger.getLogger(azkaban.execapp.Trigger.class);
  private final int execId;

  // condition to trigger actions(ex. flow running longer than X mins)
  private final Condition triggerCondition;
  // condition to expire this trigger(ex. flow finishes before violating SLA)
  private final Condition expireCondition;
  private boolean isExpired;
  private long nextCheckTime;
  private final List<TriggerAction> actions;

  public Trigger(int execId, Condition triggerCondition, Condition expireCondition, List<TriggerAction> actions) {
    this.execId = execId;
    this.triggerCondition = triggerCondition;
    this.expireCondition = expireCondition;
    this.actions = actions;
    this.isExpired = false;
    this.nextCheckTime = -1;
  }

  public void updateNextCheckTime() {
    this.nextCheckTime =
        Math.min(triggerCondition.getNextCheckTime(),
            expireCondition.getNextCheckTime());
  }

  public long getNextCheckTime() {
    return nextCheckTime;
  }


  /**
   * Perform the action if trigger condition is met
   *
   */
  @Override
  public void run() {
    if(isTriggerExpired()) {
      return ;
    }

    boolean isTriggerConditionMet = triggerCondition.isMet();

    if (isTriggerConditionMet) {
      logger.info("Condition " + triggerCondition.getExpression() + " met");
      for (TriggerAction action : actions) {
        try {
          action.doAction();
        } catch (Exception e) {
          logger.error("Failed to do action " + action.getDescription() + " for execution " + azkaban.execapp.Trigger.this.execId,
              e);
        }
      }

      // the trigger has been triggered, make it expired.
      this.isExpired = true;
    }
    else {
      updateNextCheckTime();
    }
  }

  /**
   * Check if the trigger is expired and reset isExpired
   * @return true if trigger is expired
   */
  public boolean isTriggerExpired() {
    isExpired = isExpired || expireCondition.isMet();
    return isExpired;
  }

  public String toString() {
    StringBuffer actionsString = new StringBuffer();
    for (TriggerAction act : actions) {
      actionsString.append(", ");
      actionsString.append(act.getDescription());
    }

    return "Trigger for execution " + execId + " with trigger condition of "
        + triggerCondition.getExpression() + " and expire condition of "
        + expireCondition.getExpression() + actionsString;
  }
}
