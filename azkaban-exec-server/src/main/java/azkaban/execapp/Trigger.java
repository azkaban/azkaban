/*
 * Copyright 2017 LinkedIn Corp.
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


package azkaban.execapp;

import java.util.List;

import azkaban.trigger.Condition;
import azkaban.trigger.TriggerAction;
import org.apache.log4j.Logger;


public class Trigger implements Runnable {
  private static Logger logger = Logger.getLogger(azkaban.execapp.Trigger.class);
  private final int execId;

  // condition to trigger actions(ex. flow running longer than X mins)
  private final Condition triggerCondition;
  // condition to expire this trigger(ex. flow finishes before violating SLA)
  private final Condition expireCondition;
  private final List<TriggerAction> actions;

  public Trigger(int execId,
                 Condition triggerCondition,
                 Condition expireCondition,
                 List<TriggerAction> actions)
  {
    this.execId = execId;
    this.triggerCondition = triggerCondition;
    this.expireCondition = expireCondition;
    this.actions = actions;
  }


  /**
   * Perform the action if trigger condition is met
   */
  @Override
  public void run() {
    if(isTriggerExpired()) {
      logger.info(this + " expired");
      return ;
    }

    boolean isTriggerConditionMet = triggerCondition.isMet();

    if (isTriggerConditionMet) {
      logger.info("Condition " + triggerCondition.getExpression() + " met");
      for (TriggerAction action : actions) {
        try {
          action.doAction();
        } catch (Exception e) {
          logger.error("Failed to do action " + action.getDescription()
              + " for execution " + azkaban.execapp.Trigger.this.execId, e);
        }
      }
    }
  }

  /**
   * Check if the trigger is expired and reset isExpired
   * @return true if trigger is expired
   */
  public boolean isTriggerExpired() {
    return expireCondition.isMet();
  }

  public String toString() {
    StringBuilder actionsString = new StringBuilder();
    for (TriggerAction act : actions) {
      actionsString.append(", ");
      actionsString.append(act.getDescription());
    }

    return "Trigger for execution " + execId + " with trigger condition of "
        + triggerCondition.getExpression() + " and expire condition of "
        + expireCondition.getExpression() + actionsString;
  }
}
