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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import azkaban.event.Event;
import azkaban.event.EventHandler;
import azkaban.event.EventListener;
import azkaban.event.Event.Type;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.utils.Props;

public class TriggerManager extends EventHandler implements
    TriggerManagerAdapter {
  private static Logger logger = Logger.getLogger(TriggerManager.class);
  public static final long DEFAULT_SCANNER_INTERVAL_MS = 60000;

  private static Map<Integer, Trigger> triggerIdMap =
      new ConcurrentHashMap<Integer, Trigger>();

  private CheckerTypeLoader checkerTypeLoader;
  private ActionTypeLoader actionTypeLoader;
  private TriggerLoader triggerLoader;

  private final TriggerScannerThread runnerThread;
  private long lastRunnerThreadCheckTime = -1;
  private long runnerThreadIdleTime = -1;
  private LocalTriggerJMX jmxStats = new LocalTriggerJMX();

  private ExecutorManagerEventListener listener =
      new ExecutorManagerEventListener();

  private final Object syncObj = new Object();

  private String scannerStage = "";

  public TriggerManager(Props props, TriggerLoader triggerLoader,
      ExecutorManager executorManager) throws TriggerManagerException {

    this.triggerLoader = triggerLoader;

    long scannerInterval =
        props.getLong("trigger.scan.interval", DEFAULT_SCANNER_INTERVAL_MS);
    runnerThread = new TriggerScannerThread(scannerInterval);

    checkerTypeLoader = new CheckerTypeLoader();
    actionTypeLoader = new ActionTypeLoader();

    try {
      checkerTypeLoader.init(props);
      actionTypeLoader.init(props);
    } catch (Exception e) {
      throw new TriggerManagerException(e);
    }

    Condition.setCheckerLoader(checkerTypeLoader);
    Trigger.setActionTypeLoader(actionTypeLoader);

    executorManager.addListener(listener);

    logger.info("TriggerManager loaded.");
  }

  @Override
  public void start() throws TriggerManagerException {

    try {
      // expect loader to return valid triggers
      List<Trigger> triggers = triggerLoader.loadTriggers();
      for (Trigger t : triggers) {
        runnerThread.addTrigger(t);
        triggerIdMap.put(t.getTriggerId(), t);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new TriggerManagerException(e);
    }

    runnerThread.start();
  }

  protected CheckerTypeLoader getCheckerLoader() {
    return checkerTypeLoader;
  }

  protected ActionTypeLoader getActionLoader() {
    return actionTypeLoader;
  }

  public void insertTrigger(Trigger t) throws TriggerManagerException {
    synchronized (syncObj) {
      try {
        triggerLoader.addTrigger(t);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
      runnerThread.addTrigger(t);
      triggerIdMap.put(t.getTriggerId(), t);
    }
  }

  public void removeTrigger(int id) throws TriggerManagerException {
    synchronized (syncObj) {
      Trigger t = triggerIdMap.get(id);
      if (t != null) {
        removeTrigger(triggerIdMap.get(id));
      }
    }
  }

  public void updateTrigger(int id) throws TriggerManagerException {
    synchronized (syncObj) {
      if (!triggerIdMap.containsKey(id)) {
        throw new TriggerManagerException("The trigger to update " + id
            + " doesn't exist!");
      }

      Trigger t;
      try {
        t = triggerLoader.loadTrigger(id);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
      updateTrigger(t);
    }
  }

  public void updateTrigger(Trigger t) throws TriggerManagerException {
    synchronized (syncObj) {
      runnerThread.deleteTrigger(triggerIdMap.get(t.getTriggerId()));
      runnerThread.addTrigger(t);
      triggerIdMap.put(t.getTriggerId(), t);
    }
  }

  public void removeTrigger(Trigger t) throws TriggerManagerException {
    synchronized (syncObj) {
      runnerThread.deleteTrigger(t);
      triggerIdMap.remove(t.getTriggerId());
      try {
        t.stopCheckers();
        triggerLoader.removeTrigger(t);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
    }
  }

  public List<Trigger> getTriggers() {
    return new ArrayList<Trigger>(triggerIdMap.values());
  }

  public Map<String, Class<? extends ConditionChecker>> getSupportedCheckers() {
    return checkerTypeLoader.getSupportedCheckers();
  }

  private class TriggerScannerThread extends Thread {
    private BlockingQueue<Trigger> triggers;
    private Map<Integer, ExecutableFlow> justFinishedFlows;
    private boolean shutdown = false;
    private final long scannerInterval;

    public TriggerScannerThread(long scannerInterval) {
      triggers = new PriorityBlockingQueue<Trigger>(1, new TriggerComparator());
      justFinishedFlows = new ConcurrentHashMap<Integer, ExecutableFlow>();
      this.setName("TriggerRunnerManager-Trigger-Scanner-Thread");
      this.scannerInterval = scannerInterval;
    }

    public void shutdown() {
      logger.error("Shutting down trigger manager thread " + this.getName());
      shutdown = true;
      this.interrupt();
    }

    public void addJustFinishedFlow(ExecutableFlow flow) {
      synchronized (syncObj) {
        justFinishedFlows.put(flow.getExecutionId(), flow);
      }
    }

    public void addTrigger(Trigger t) {
      synchronized (syncObj) {
        t.updateNextCheckTime();
        triggers.add(t);
      }
    }

    public void deleteTrigger(Trigger t) {
      triggers.remove(t);
    }

    public void run() {
      while (!shutdown) {
        synchronized (syncObj) {
          try {
            lastRunnerThreadCheckTime = System.currentTimeMillis();

            scannerStage =
                "Ready to start a new scan cycle at "
                    + lastRunnerThreadCheckTime;

            try {
              checkAllTriggers();
              justFinishedFlows.clear();
            } catch (Exception e) {
              e.printStackTrace();
              logger.error(e.getMessage());
            } catch (Throwable t) {
              t.printStackTrace();
              logger.error(t.getMessage());
            }

            scannerStage = "Done flipping all triggers.";

            runnerThreadIdleTime =
                scannerInterval
                    - (System.currentTimeMillis() - lastRunnerThreadCheckTime);

            if (runnerThreadIdleTime < 0) {
              logger.error("Trigger manager thread " + this.getName()
                  + " is too busy!");
            } else {
              syncObj.wait(runnerThreadIdleTime);
            }
          } catch (InterruptedException e) {
            logger.info("Interrupted. Probably to shut down.");
          }
        }
      }
    }

    private void checkAllTriggers() throws TriggerManagerException {
      long now = System.currentTimeMillis();

      // sweep through the rest of them
      for (Trigger t : triggers) {
        try {
          scannerStage = "Checking for trigger " + t.getTriggerId();

          boolean shouldSkip = true;
          if (shouldSkip && t.getInfo() != null && t.getInfo().containsKey("monitored.finished.execution")) {
            int execId = Integer.valueOf((String) t.getInfo().get("monitored.finished.execution"));
            if (justFinishedFlows.containsKey(execId)) {
              logger.info("Monitored execution has finished. Checking trigger earlier " + t.getTriggerId());
              shouldSkip = false;
            }
          }
          if (shouldSkip && t.getNextCheckTime() > now) {
            shouldSkip = false;
          }

          logger.info("Get Next Check Time =" + t.getNextCheckTime() + "  now = " + now );
          if (shouldSkip) {
            logger.info("Skipping trigger" + t.getTriggerId() + " until " + t.getNextCheckTime());
          }

          if (logger.isDebugEnabled()) {
            logger.info("Checking trigger " + t.getTriggerId());
          }
          if (t.getStatus().equals(TriggerStatus.READY)) {
            if (t.triggerConditionMet()) {
              onTriggerTrigger(t);
            } else if (t.expireConditionMet()) {
              onTriggerExpire(t);
            }
          }
          if (t.getStatus().equals(TriggerStatus.EXPIRED) && t.getSource().equals("azkaban")) {
            removeTrigger(t);
          } else {
            t.updateNextCheckTime();
          }
        } catch (Throwable th) {
          //skip this trigger, moving on to the next one
          logger.error("Failed to process trigger with id : " + t.getTriggerId(), th);
        }
      }
    }

    private void onTriggerTrigger(Trigger t) throws TriggerManagerException {
      List<TriggerAction> actions = t.getTriggerActions();
      for (TriggerAction action : actions) {
        try {
          logger.info("Doing trigger actions");
          action.doAction();
        } catch (Exception e) {
          logger.error("Failed to do action " + action.getDescription(), e);
        } catch (Throwable th) {
          logger.error("Failed to do action " + action.getDescription(), th);
        }
      }
      if (t.isResetOnTrigger()) {
        t.resetTriggerConditions();
        t.resetExpireCondition();
      } else {
        t.setStatus(TriggerStatus.EXPIRED);
      }
      try {
        triggerLoader.updateTrigger(t);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
    }

    private void onTriggerExpire(Trigger t) throws TriggerManagerException {
      List<TriggerAction> expireActions = t.getExpireActions();
      for (TriggerAction action : expireActions) {
        try {
          logger.info("Doing expire actions");
          action.doAction();
        } catch (Exception e) {
          logger.error("Failed to do expire action " + action.getDescription(),
              e);
        } catch (Throwable th) {
          logger.error("Failed to do expire action " + action.getDescription(),
              th);
        }
      }
      if (t.isResetOnExpire()) {
        t.resetTriggerConditions();
        t.resetExpireCondition();
      } else {
        t.setStatus(TriggerStatus.EXPIRED);
      }
      try {
        triggerLoader.updateTrigger(t);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
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
  }

  public Trigger getTrigger(int triggerId) {
    synchronized (syncObj) {
      return triggerIdMap.get(triggerId);
    }
  }

  public void expireTrigger(int triggerId) {
    Trigger t = getTrigger(triggerId);
    t.setStatus(TriggerStatus.EXPIRED);
  }

  @Override
  public List<Trigger> getTriggers(String triggerSource) {
    List<Trigger> triggers = new ArrayList<Trigger>();
    for (Trigger t : triggerIdMap.values()) {
      if (t.getSource().equals(triggerSource)) {
        triggers.add(t);
      }
    }
    return triggers;
  }

  @Override
  public List<Trigger> getTriggerUpdates(String triggerSource,
      long lastUpdateTime) throws TriggerManagerException {
    List<Trigger> triggers = new ArrayList<Trigger>();
    for (Trigger t : triggerIdMap.values()) {
      if (t.getSource().equals(triggerSource)
          && t.getLastModifyTime() > lastUpdateTime) {
        triggers.add(t);
      }
    }
    return triggers;
  }

  @Override
  public List<Trigger> getAllTriggerUpdates(long lastUpdateTime)
      throws TriggerManagerException {
    List<Trigger> triggers = new ArrayList<Trigger>();
    for (Trigger t : triggerIdMap.values()) {
      if (t.getLastModifyTime() > lastUpdateTime) {
        triggers.add(t);
      }
    }
    return triggers;
  }

  @Override
  public void insertTrigger(Trigger t, String user)
      throws TriggerManagerException {
    insertTrigger(t);
  }

  @Override
  public void removeTrigger(int id, String user) throws TriggerManagerException {
    removeTrigger(id);
  }

  @Override
  public void updateTrigger(Trigger t, String user)
      throws TriggerManagerException {
    updateTrigger(t);
  }

  @Override
  public void shutdown() {
    runnerThread.shutdown();
  }

  @Override
  public TriggerJMX getJMX() {
    return this.jmxStats;
  }

  private class LocalTriggerJMX implements TriggerJMX {

    @Override
    public long getLastRunnerThreadCheckTime() {
      return lastRunnerThreadCheckTime;
    }

    @Override
    public boolean isRunnerThreadActive() {
      return runnerThread.isAlive();
    }

    @Override
    public String getPrimaryServerHost() {
      return "local";
    }

    @Override
    public int getNumTriggers() {
      return triggerIdMap.size();
    }

    @Override
    public String getTriggerSources() {
      Set<String> sources = new HashSet<String>();
      for (Trigger t : triggerIdMap.values()) {
        sources.add(t.getSource());
      }
      return sources.toString();
    }

    @Override
    public String getTriggerIds() {
      return triggerIdMap.keySet().toString();
    }

    @Override
    public long getScannerIdleTime() {
      return runnerThreadIdleTime;
    }

    @Override
    public Map<String, Object> getAllJMXMbeans() {
      return new HashMap<String, Object>();
    }

    @Override
    public String getScannerThreadStage() {
      return scannerStage;
    }

  }

  @Override
  public void registerCheckerType(String name,
      Class<? extends ConditionChecker> checker) {
    checkerTypeLoader.registerCheckerType(name, checker);
  }

  @Override
  public void registerActionType(String name,
      Class<? extends TriggerAction> action) {
    actionTypeLoader.registerActionType(name, action);
  }

  private class ExecutorManagerEventListener implements EventListener {
    public ExecutorManagerEventListener() {
    }

    @Override
    public void handleEvent(Event event) {
      // this needs to be fixed for perf
      synchronized (syncObj) {
        ExecutableFlow flow = (ExecutableFlow) event.getRunner();
        if (event.getType() == Type.FLOW_FINISHED) {
          logger.info("Flow finish event received. " + flow.getExecutionId());
          runnerThread.addJustFinishedFlow(flow);
        }
      }
    }
  }

}
