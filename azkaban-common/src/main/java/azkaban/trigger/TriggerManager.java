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

import azkaban.event.EventHandler;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.NoSuchAzkabanResourceException;
import azkaban.metrics.MetricsManager;
import azkaban.scheduler.MissedSchedulesManager;
import azkaban.utils.Props;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;


@Singleton
public class TriggerManager extends EventHandler implements TriggerManagerAdapter {

  public static final long DEFAULT_SCANNER_INTERVAL_MS = 60000;
  private static final Logger logger = Logger.getLogger(TriggerManager.class);
  private static final Map<Integer, Trigger> triggerIdMap = new ConcurrentHashMap<>();
  private static final Set<Integer> removedTriggerIds = new HashSet<>();
  private final TriggerScannerThread runnerThread;
  private final MetricsManager metricsManager;
  private final Meter heartbeatMeter;
  private final Timer scannerThreadLatencyMetrics;
  private final CheckerTypeLoader checkerTypeLoader;
  private final ActionTypeLoader actionTypeLoader;
  private final TriggerLoader triggerLoader;
  private final LocalTriggerJMX jmxStats = new LocalTriggerJMX();
  private long lastRunnerThreadCheckTime = -1;
  private long runnerThreadIdleTime = -1;
  private String scannerStage = "";

  @Inject
  public TriggerManager(final Props props, final TriggerLoader triggerLoader,
      final ExecutorManagerAdapter executorManagerAdapter, final MetricsManager metricsManager,
      final MissedSchedulesManager missedScheduleManager) throws TriggerManagerException {

    requireNonNull(props);
    requireNonNull(executorManagerAdapter);
    this.triggerLoader = requireNonNull(triggerLoader);

    final long scannerInterval = props.getLong("trigger.scan.interval", DEFAULT_SCANNER_INTERVAL_MS);
    this.runnerThread = new TriggerScannerThread(scannerInterval);

    this.checkerTypeLoader = new CheckerTypeLoader();
    this.actionTypeLoader = new ActionTypeLoader();
    this.metricsManager = metricsManager;
    this.heartbeatMeter = this.metricsManager.addMeter("cron-scheduler-heartbeat");
    this.scannerThreadLatencyMetrics = this.metricsManager.addTimer("cron-scheduler-thread-latency");
    this.metricsManager.addGauge("cron-schedules-count-gauge", this.runnerThread.triggerSize());

    try {
      this.checkerTypeLoader.init(props);
      this.actionTypeLoader.init(props);
    } catch (final Exception e) {
      throw new TriggerManagerException(e);
    }

    Condition.setCheckerLoader(this.checkerTypeLoader);
    Trigger.setActionTypeLoader(this.actionTypeLoader);
    Trigger.setMissedScheduleManager(missedScheduleManager);

    logger.info("TriggerManager loaded.");
  }

  @Override
  public void start() throws TriggerManagerException {

    try {
      // expect loader to return valid triggers
      final List<Trigger> triggers = this.triggerLoader.loadTriggers();
      for (final Trigger t : triggers) {
        this.runnerThread.addTrigger(t);
        triggerIdMap.put(t.getTriggerId(), t);
      }
    } catch (final Exception e) {
      logger.error(e);
      throw new TriggerManagerException(e);
    }
    this.runnerThread.start();
  }

  protected CheckerTypeLoader getCheckerLoader() {
    return this.checkerTypeLoader;
  }

  protected ActionTypeLoader getActionLoader() {
    return this.actionTypeLoader;
  }

  public void insertTrigger(final Trigger t) throws TriggerManagerException {
    t.lock();
    logger.info("Inserting trigger " + t + " in TriggerManager");
    try {
      this.triggerLoader.addTrigger(t);
      this.runnerThread.addTrigger(t);
      triggerIdMap.put(t.getTriggerId(), t);
    } catch (final TriggerLoaderException e) {
      throw new TriggerManagerException(e);
    } finally {
      t.unlock();
    }
  }

  public void removeTrigger(final int id) throws TriggerManagerException {
    logger.info("Removing trigger with id: " + id + " from TriggerManager");
    final Trigger t = triggerIdMap.get(id);
    removeTrigger(triggerIdMap.get(id));
  }

  public void updateTrigger(final Trigger t) throws TriggerManagerException {
    t.lock();
    logger.info("Updating trigger " + t + " in TriggerManager");
    this.runnerThread.deleteTrigger(triggerIdMap.get(t.getTriggerId()));
    this.runnerThread.addTrigger(t);
    triggerIdMap.put(t.getTriggerId(), t);
    try {
      this.triggerLoader.updateTrigger(t);
    } catch (final TriggerLoaderException e) {
      throw new TriggerManagerException(e);
    } finally {
      t.unlock();
    }
  }

  public void removeTrigger(final Trigger t) throws TriggerManagerException {
    t.lock();
    logger.info("Removing trigger " + t + " from TriggerManager");

    this.runnerThread.deleteTrigger(t);
    triggerIdMap.remove(t.getTriggerId());
    removedTriggerIds.add(t.getTriggerId());
    try {
      t.stopCheckers();
      this.triggerLoader.removeTrigger(t);
    } catch (final TriggerLoaderException e) {
      throw new TriggerManagerException(e);
    } finally {
      t.unlock();
    }
  }

  @Override
  public List<Trigger> getTriggers() {
    return new ArrayList<>(triggerIdMap.values());
  }

  // get a list of removed triggers and clear the list
  @Override
  public List<Integer> getRemovedTriggerIds() {
    List<Integer> removedTriggerIdsCopy = new ArrayList<>(removedTriggerIds);
    removedTriggerIds.clear();
    return removedTriggerIdsCopy;
  }

  public Map<String, Class<? extends ConditionChecker>> getSupportedCheckers() {
    return this.checkerTypeLoader.getSupportedCheckers();
  }

  public Trigger getTrigger(final int triggerId) {
    return triggerIdMap.get(triggerId);
  }

  public void expireTrigger(final int triggerId) {
    final Trigger t = getTrigger(triggerId);
    t.setStatus(TriggerStatus.EXPIRED);
  }

  @Override
  public List<Trigger> getTriggers(final String triggerSource) {
    final List<Trigger> triggers = new ArrayList<>();
    for (final Trigger t : triggerIdMap.values()) {
      if (t.getSource().equals(triggerSource)) {
        triggers.add(t);
      }
    }
    return triggers;
  }

  @Override
  public List<Trigger> getTriggerUpdates(final String triggerSource,
      final Map<Integer, Long> triggerToLastCheckTime) throws TriggerManagerException {
    final List<Trigger> triggers = new ArrayList<>();
    for (final Trigger t : triggerIdMap.values()) {
      t.lock();
      if (t.getSource().equals(triggerSource)
          && t.getLastModifyTime() > triggerToLastCheckTime.getOrDefault(t.getTriggerId(), -1l)) {
        triggers.add(t);
      }
      t.unlock();
    }
    return triggers;
  }

  @Override
  public Optional<Trigger> getUpdatedTriggerById(final int triggerId, final long lastUpdateTime) {
    Trigger t = triggerIdMap.get(triggerId);
    return t.getLastModifyTime() > lastUpdateTime ? Optional.of(t) : Optional.empty();
  }

  @Override
  public void insertTrigger(final Trigger t, final String user)
      throws TriggerManagerException {
    insertTrigger(t);
  }

  @Override
  public void removeTrigger(final int id, final String user) throws TriggerManagerException {
    removeTrigger(id);
  }

  @Override
  public void updateTrigger(final Trigger t, final String user)
      throws TriggerManagerException {
    updateTrigger(t);
  }

  @Override
  public void shutdown() {
    this.runnerThread.shutdown();
  }

  @Override
  public TriggerJMX getJMX() {
    return this.jmxStats;
  }

  @Override
  public void registerCheckerType(final String name,
      final Class<? extends ConditionChecker> checker) {
    this.checkerTypeLoader.registerCheckerType(name, checker);
  }

  @Override
  public void registerActionType(final String name,
      final Class<? extends TriggerAction> action) {
    this.actionTypeLoader.registerActionType(name, action);
  }

  private class TriggerScannerThread extends Thread {

    private final long scannerInterval;
    private final BlockingQueue<Trigger> triggers;
    private boolean shutdown = false;

    public TriggerScannerThread(final long scannerInterval) {
      this.triggers = new PriorityBlockingQueue<>(1, new TriggerComparator());
      this.setName("TriggerRunnerManager-Trigger-Scanner-Thread");
      this.scannerInterval = scannerInterval;
    }

    private Supplier<Integer> triggerSize() {
      return this.triggers::size;
    }

    public void shutdown() {
      TriggerManager.logger.error("Shutting down trigger manager thread " + this.getName());
      this.shutdown = true;
      this.interrupt();
    }

    public void addTrigger(final Trigger t) {
      t.updateNextCheckTime();
      this.triggers.add(t);
    }

    public void deleteTrigger(final Trigger t) {
      this.triggers.remove(t);
    }

    @Override
    public void run() {
      while (!this.shutdown) {
        TriggerManager.this.heartbeatMeter.mark();
          try {
            TriggerManager.this.lastRunnerThreadCheckTime = System.currentTimeMillis();

            TriggerManager.this.scannerStage =
                "Ready to start a new scan cycle at "
                    + TriggerManager.this.lastRunnerThreadCheckTime;

            try {
              checkAllTriggers();
            } catch (final Exception e) {
              e.printStackTrace();
              TriggerManager.logger.error(e.getMessage());
            } catch (final Throwable t) {
              t.printStackTrace();
              TriggerManager.logger.error(t.getMessage());
            }

            TriggerManager.this.scannerStage = "Done flipping all triggers.";
            final long triggerScanTime = System.currentTimeMillis() - TriggerManager.this.lastRunnerThreadCheckTime;
            TriggerManager.this.scannerThreadLatencyMetrics.update(triggerScanTime, TimeUnit.MILLISECONDS);

            TriggerManager.this.runnerThreadIdleTime = this.scannerInterval - triggerScanTime;

            if (TriggerManager.this.runnerThreadIdleTime <= 0) {
              TriggerManager.logger.error(
                  "Trigger manager thread " + this.getName() + " is too busy! Remaining idle time in ms: "
                      + TriggerManager.this.runnerThreadIdleTime);
            } else {
              Thread.sleep(TriggerManager.this.runnerThreadIdleTime);
              TriggerManager.logger.debug("trigger manager wait on " + TriggerManager.this.runnerThreadIdleTime);
            }
          } catch (final InterruptedException e) {
            TriggerManager.logger.info("Interrupted. Probably to shut down.");
          }

      }
    }

    private void checkAllTriggers() throws TriggerManagerException {
      // sweep through the rest of them
      for (final Trigger t : this.triggers) {
        t.lock();
        try {
          TriggerManager.this.scannerStage = "Checking for trigger " + t.getTriggerId();
          if (t.getStatus().equals(TriggerStatus.INVALID)) {
            removeTrigger(t);
            continue;
          }

          if (t.getStatus().equals(TriggerStatus.READY)) {

            /**
             * Prior to this change, expiration condition should never be called though
             * we have some related code here. ExpireCondition used the same BasicTimeChecker
             * as triggerCondition do. As a consequence, we need to figure out a way to distinguish
             * the previous ExpireCondition and this commit's ExpireCondition.
             */
            if (t.getExpireCondition().getExpression().contains("EndTimeChecker") && t.expireConditionMet()) {
              onTriggerPause(t);
            } else if (t.triggerConditionMet()) {
              onTriggerTrigger(t);
            }
          }
          if ((t.getStatus().equals(TriggerStatus.EXPIRED) && t.getSource().equals("azkaban"))
              || t.getStatus().equals(TriggerStatus.INVALID)) {
            removeTrigger(t);
          } else {
            t.updateNextCheckTime();
          }
        } catch (final Throwable th) {
          //skip this trigger, moving on to the next one
          TriggerManager.logger.error("Failed to process trigger with id : " + t, th);
        } finally {
          t.unlock();
        }
      }
    }

    private void onTriggerTrigger(final Trigger t) throws TriggerManagerException {
      final List<TriggerAction> actions = t.getTriggerActions();
      for (final TriggerAction action : actions) {
        try {
          TriggerManager.logger.info("Doing trigger actions " + action.getDescription() + " for " + t);
          action.doAction();
        } catch (NoSuchAzkabanResourceException e) {
          logger.warn("find no matching projects/flows for the trigger " + t.getTriggerId() + ", mark trigger invalid");
          t.setStatus(TriggerStatus.INVALID);
          return;
        } catch (final ExecutorManagerException e) {
          if (e.getReason() == ExecutorManagerException.Reason.SkippedExecution) {
            TriggerManager.logger.info(
                "Skipped action [" + action.getDescription() + "] for [" + t + "] because: " + e.getMessage());
          } else {
            TriggerManager.logger.error("Failed to do action [" + action.getDescription() + "] for [" + t + "]", e);
          }
        } catch (final Throwable th) {
          TriggerManager.logger.error("Failed to do action [" + action.getDescription() + "] for [" + t + "]", th);
        }
      }

      if (t.isResetOnTrigger()) {
        t.resetTriggerConditions();
        try {
          t.sendTaskToMissedScheduleManager();
        } catch (NoSuchAzkabanResourceException e) {
          logger.warn("find no matching projects/flows for the trigger " + t.getTriggerId() + ", mark trigger invalid");
          t.setStatus(TriggerStatus.INVALID);
          return;
        }
      } else {
        TriggerManager.logger.info(
            "NextCheckTime did not change. Setting status to expired for trigger" + t.getTriggerId());
        t.setStatus(TriggerStatus.EXPIRED);
      }
      try {
        TriggerManager.this.triggerLoader.updateTrigger(t);
      } catch (final TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
    }

    private void onTriggerPause(final Trigger t) throws TriggerManagerException {
      final List<TriggerAction> expireActions = t.getExpireActions();
      for (final TriggerAction action : expireActions) {
        try {
          TriggerManager.logger.info("Doing expire actions for " + action.getDescription() + " for " + t);
          action.doAction();
        } catch (final Exception e) {
          TriggerManager.logger.error("Failed to do expire action " + action.getDescription() + " for " + t, e);
        } catch (final Throwable th) {
          TriggerManager.logger.error("Failed to do expire action " + action.getDescription() + " for " + t, th);
        }
      }
      TriggerManager.logger.info("Pausing Trigger " + t.getDescription());
      t.setStatus(TriggerStatus.PAUSED);
      try {
        TriggerManager.this.triggerLoader.updateTrigger(t);
      } catch (final TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
    }

    private class TriggerComparator implements Comparator<Trigger> {

      @Override
      public int compare(final Trigger arg0, final Trigger arg1) {
        final long first = arg1.getNextCheckTime();
        final long second = arg0.getNextCheckTime();

        if (first == second) {
          return 0;
        } else if (first < second) {
          return 1;
        }
        return -1;
      }
    }
  }

  private class LocalTriggerJMX implements TriggerJMX {

    @Override
    public long getLastRunnerThreadCheckTime() {
      return TriggerManager.this.lastRunnerThreadCheckTime;
    }

    @Override
    public boolean isRunnerThreadActive() {
      return TriggerManager.this.runnerThread.isAlive();
    }

    @Override
    public String getPrimaryServerHost() {
      return "local";
    }

    @Override
    public int getNumTriggers() {
      return TriggerManager.triggerIdMap.size();
    }

    @Override
    public String getTriggerSources() {
      final Set<String> sources = new HashSet<>();
      for (final Trigger t : TriggerManager.triggerIdMap.values()) {
        sources.add(t.getSource());
      }
      return sources.toString();
    }

    @Override
    public String getTriggerIds() {
      return TriggerManager.triggerIdMap.keySet().toString();
    }

    @Override
    public long getScannerIdleTime() {
      return TriggerManager.this.runnerThreadIdleTime;
    }

    @Override
    public Map<String, Object> getAllJMXMbeans() {
      return new HashMap<>();
    }

    @Override
    public String getScannerThreadStage() {
      return TriggerManager.this.scannerStage;
    }
  }
}
