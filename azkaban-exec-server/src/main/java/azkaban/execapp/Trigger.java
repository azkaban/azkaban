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

import azkaban.trigger.Condition;
import azkaban.trigger.TriggerAction;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Trigger implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Trigger.class);
  private final int execId;

  // condition to trigger actions(ex. flow running longer than X mins)
  private final Condition triggerCondition;
  // condition to expire this trigger(ex. flow finishes before violating SLA)
  private final Condition expireCondition;
  private final List<TriggerAction> actions;

  public Trigger(final int execId,
      final Condition triggerCondition,
      final Condition expireCondition,
      final List<TriggerAction> actions) {
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
    if (isTriggerExpired()) {
      LOG.info(this + " expired");
      return;
    }

    final boolean isTriggerConditionMet = this.triggerCondition.isMet();
    if (isTriggerConditionMet) {
      LOG.info("Condition " + this.triggerCondition.getExpression() + " met");
      for (final TriggerAction action : this.actions) {
        try {
          action.doAction();
        } catch (final Exception e) {
          LOG.error("Failed to do action " + action.getDescription()
              + " for execution " + azkaban.execapp.Trigger.this.execId, e);
        }
      }
    }
  }

  /**
   * Check if the trigger is expired and reset isExpired
   *
   * @return true if trigger is expired
   */
  public boolean isTriggerExpired() {
    return this.expireCondition.isMet();
  }

  @Override
  public String toString() {
    final StringBuilder actionsString = new StringBuilder();
    for (final TriggerAction act : this.actions) {
      actionsString.append(", ");
      actionsString.append(act.getDescription());
    }

    return "Trigger for execution " + this.execId + " with trigger condition of "
        + this.triggerCondition.getExpression() + " and expire condition of "
        + this.expireCondition.getExpression() + actionsString;
  }
}
