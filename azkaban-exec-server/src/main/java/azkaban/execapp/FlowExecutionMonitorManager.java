package azkaban.execapp;

import azkaban.sla.SlaOption;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.builtin.KillExecutionAction;
import azkaban.trigger.builtin.SlaAlertAction;
import azkaban.trigger.builtin.SlaChecker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;


public class FlowExecutionMonitorManager extends Thread {
  private static Logger logger = Logger.getLogger(FlowExecutionMonitorManager.class);
  private BlockingQueue<Trigger> triggers = null;
  private static final Duration CHECK_INTERVAL = Duration.ofMillis(15 * 1000);

  public FlowExecutionMonitorManager() {
    this.setName("FlowExecutionMonitorManager-Thread");
    this.setDaemon(true);
    this.triggers = new PriorityBlockingQueue<>(1, new TriggerComparator());
  }

  public Trigger[] getAllTriggers() {
    return triggers.toArray(new Trigger[triggers.size()]);
  }

  private class TriggerComparator implements Comparator<Trigger> {
    @Override
    public int compare(Trigger arg0, Trigger arg1) {
      long first = arg1.getNextCheckTime();
      long second = arg0.getNextCheckTime();

      if (first == second) {
        return 0;
      } else if (first < second) {
        return 1;
      }
      return -1;
    }
  }

  public void addMonitor(int execId, List<SlaOption> slaOptions) {
    for (SlaOption sla : slaOptions) {
      logger.info("Adding sla trigger " + sla.toString() + " to execution " + execId);
      SlaChecker slaFailChecker = new SlaChecker("slaFailChecker", sla, execId);
      Map<String, ConditionChecker> slaCheckers = new HashMap<>();
      slaCheckers.put(slaFailChecker.getId(), slaFailChecker);
      Condition triggerCond = new Condition(slaCheckers, slaFailChecker.getId() + ".isSlaFailed()");

      // if whole flow finish before violate sla, just expire
      SlaChecker slaPassChecker = new SlaChecker("slaPassChecker", sla, execId);
      Map<String, ConditionChecker> expireCheckers = new HashMap<String, ConditionChecker>();
      expireCheckers.put(slaPassChecker.getId(), slaPassChecker);
      Condition expireCond = new Condition(expireCheckers, slaPassChecker.getId() + ".isSlaPassed()");

      List<TriggerAction> actions = new ArrayList<>();
      List<String> slaActions = sla.getActions();
      for (String act : slaActions) {
        if (act.equals(SlaOption.ACTION_ALERT)) {
          SlaAlertAction slaAlert = new SlaAlertAction("slaAlert", sla, execId);
          actions.add(slaAlert);
        } else if (act.equals(SlaOption.ACTION_CANCEL_FLOW)) {
          KillExecutionAction killAct = new KillExecutionAction("killExecution", execId);
          actions.add(killAct);
        }
      }
      triggers.add(new Trigger(execId, triggerCond, expireCond, actions));
    }
  }

  /**
   * Check all triggers. If no trigger is available then wait.
   * Remove expired triggers from the queue if any.
   *
   */
  private void checkAllTriggers() throws InterruptedException {
    Trigger trigger = null;
    List<Trigger> validTriggers = new ArrayList<>();

    while ((trigger = triggers.take()) != null) {
      trigger.run();
      if (!trigger.isTriggerExpired()) {
        validTriggers.add(trigger);
      }
      else {
        logger.info("trigger " + trigger + "expired, removed from triggers");
      }
    }

    for(Trigger validTrigger : validTriggers) {
      triggers.add(validTrigger);
    }
  }

  @Override
  public void run() {
    while (true) {
      try {
        checkAllTriggers();
        Thread.sleep(CHECK_INTERVAL.toMillis());
      } catch (InterruptedException e) {
        logger.error(e);
      }
    }
  }

}
