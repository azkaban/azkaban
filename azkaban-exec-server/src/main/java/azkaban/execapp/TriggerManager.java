package azkaban.execapp;

import azkaban.sla.SlaOption;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.builtin.KillExecutionAction;
import azkaban.trigger.builtin.SlaAlertAction;
import azkaban.trigger.builtin.SlaChecker;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


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
    return new Condition(slaCheckers, slaFailChecker.getId() + "."+checkerMethod);
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
          break;
      }
      if (action != null) {
        actions.add(action);
      }
    }
    return actions;
  }

  public void addMonitor(int execId, List<SlaOption> slaOptions) {
    for (SlaOption sla : slaOptions) {
      Condition triggerCond = createCondition(sla, execId, "slaFailChecker", "isSlaFailed()");

      // if whole flow finish before violating sla, just expire the checker
      Condition expireCond = createCondition(sla, execId, "slaPassChecker", "isSlaPassed()");

      List<TriggerAction> actions = createActions(sla, execId);
      Trigger trigger = new Trigger(execId, triggerCond, expireCond, actions);
      long delay = trigger.getNextCheckTime() - System.currentTimeMillis();
      logger.info("Adding sla trigger " + sla.toString() + " to execution " + execId + ", schedule to check in " + delay/1000 + " seconds");
      scheduledService.schedule(trigger, delay, TimeUnit.MILLISECONDS);
    }
  }

  public void shutdown() {
    scheduledService.shutdownNow();
  }
}
