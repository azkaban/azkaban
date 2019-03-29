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

import azkaban.execapp.action.KillExecutionAction;
import azkaban.execapp.action.KillJobAction;
import azkaban.sla.SlaOption;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.builtin.SlaAlertAction;
import azkaban.trigger.builtin.SlaChecker;
import azkaban.utils.TimeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;
import org.joda.time.ReadablePeriod;


@Singleton
public class TriggerManager {

  private static final int SCHEDULED_THREAD_POOL_SIZE = 4;
  private static final Logger logger = Logger.getLogger(TriggerManager.class);
  private final ScheduledExecutorService scheduledService;

  @Inject
  public TriggerManager() {
    this.scheduledService = Executors.newScheduledThreadPool(SCHEDULED_THREAD_POOL_SIZE);
  }

  private Condition createCondition(final SlaOption sla, final int execId, final String checkerName,
      final String checkerMethod) {
    final SlaChecker slaFailChecker = new SlaChecker(checkerName, sla, execId);
    final Map<String, ConditionChecker> slaCheckers = new HashMap<>();
    slaCheckers.put(slaFailChecker.getId(), slaFailChecker);
    return new Condition(slaCheckers, slaFailChecker.getId() + "." + checkerMethod);
  }

  private List<TriggerAction> createActions(final SlaOption sla, final int execId) {
    final List<TriggerAction> actions = new ArrayList<>();
    final List<String> slaActions = sla.getActions();
    for (final String act : slaActions) {
      TriggerAction action = null;
      switch (act) {
        case SlaOption.ACTION_ALERT:
          action = new SlaAlertAction(SlaOption.ACTION_ALERT, sla, execId);
          break;
        case SlaOption.ACTION_CANCEL_FLOW:
          action = new KillExecutionAction(SlaOption.ACTION_CANCEL_FLOW, execId);
          break;
        case SlaOption.ACTION_KILL_JOB:
          final String jobId = (String) sla.getInfo().get(SlaOption.INFO_JOB_NAME);
          action = new KillJobAction(SlaOption.ACTION_KILL_JOB, execId, jobId);
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

  @SuppressWarnings("FutureReturnValueIgnored")
  public void addTrigger(final int execId, final List<SlaOption> slaOptions) {
    for (final SlaOption sla : slaOptions) {
      final Condition triggerCond = createCondition(sla, execId, "slaFailChecker", "isSlaFailed()");

      // if whole flow finish before violating sla, just expire the checker
      final Condition expireCond = createCondition(sla, execId, "slaPassChecker", "isSlaPassed()");

      final List<TriggerAction> actions = createActions(sla, execId);
      final Trigger trigger = new Trigger(execId, triggerCond, expireCond, actions);
      final ReadablePeriod duration = TimeUtils
          .parsePeriodString((String) sla.getInfo().get(SlaOption.INFO_DURATION));
      final long durationInMillis = duration.toPeriod().toStandardDuration().getMillis();

      logger.info("Adding sla trigger " + sla.toString() + " to execution " + execId
          + ", scheduled to trigger in " + durationInMillis / 1000 + " seconds");
      this.scheduledService.schedule(trigger, durationInMillis, TimeUnit.MILLISECONDS);
    }
  }

  public void shutdown() {
    this.scheduledService.shutdownNow();
  }
}
