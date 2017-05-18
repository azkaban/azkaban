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

import azkaban.sla.SlaOption;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.builtin.SlaAlertAction;
import azkaban.trigger.builtin.SlaChecker;
import azkaban.utils.Utils;
import azkaban.execapp.action.KillExecutionAction;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.joda.time.ReadablePeriod;


public class TriggerManager {
  private static Logger logger = Logger.getLogger(TriggerManager.class);
  private static final int SCHEDULED_THREAD_POOL_SIZE = 4;
  private final ScheduledExecutorService scheduledService;

  @Inject
  public TriggerManager() {
    this.scheduledService = Executors.newScheduledThreadPool(SCHEDULED_THREAD_POOL_SIZE);
  }

  private Condition createCondition(SlaOption sla, int execId, String checkerName, String checkerMethod) {
    SlaChecker slaFailChecker = new SlaChecker(checkerName, sla, execId);
    Map<String, ConditionChecker> slaCheckers = new HashMap<>();
    slaCheckers.put(slaFailChecker.getId(), slaFailChecker);
    return new Condition(slaCheckers, slaFailChecker.getId() + "." + checkerMethod);
  }

  private List<TriggerAction> createActions(SlaOption sla, int execId) {
    List<TriggerAction> actions = new ArrayList<>();
    List<String> slaActions = sla.getActions();
    for (String act : slaActions) {
      TriggerAction action = null;
      switch (act) {
        case SlaOption.ACTION_ALERT:
          action = new SlaAlertAction("slaAlert", sla, execId);
          break;
        case SlaOption.ACTION_CANCEL_FLOW:
          action = new KillExecutionAction("killExecution", execId);
          break;
        default:
          logger.info("Unknown action type " + act);
          break;
      }
      if (action != null) {
        actions.add(action);
      }
    }
    return actions;
  }

  public void addTrigger(int execId, List<SlaOption> slaOptions) {
    for (SlaOption sla : slaOptions) {
      Condition triggerCond = createCondition(sla, execId, "slaFailChecker", "isSlaFailed()");

      // if whole flow finish before violating sla, just expire the checker
      Condition expireCond = createCondition(sla, execId, "slaPassChecker", "isSlaPassed()");

      List<TriggerAction> actions = createActions(sla, execId);
      Trigger trigger = new Trigger(execId, triggerCond, expireCond, actions);

      ReadablePeriod duration = Utils.parsePeriodString((String) sla.getInfo().get(SlaOption.INFO_DURATION));
      long durationInMillis = duration.toPeriod().toStandardDuration().getMillis();

      logger.info("Adding sla trigger " + sla.toString() + " to execution " + execId + ", scheduled to trigger in " + durationInMillis/1000 + " seconds");
      scheduledService.schedule(trigger, durationInMillis, TimeUnit.MILLISECONDS);
    }
  }

  public void shutdown() {
    scheduledService.shutdownNow();
  }
}
