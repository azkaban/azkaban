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

package azkaban.flowtrigger;

import azkaban.Constants;
import azkaban.Constants.FlowTriggerProps;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginException;
import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginManager;
import azkaban.project.FlowTrigger;
import azkaban.project.FlowTriggerDependency;
import azkaban.project.Project;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FlowTriggerService is a singleton class in the AZ web server to
 * process all trigger-related operations. Externally it provides following
 * operations -
 * 1. Create a trigger instance based on trigger definition.
 * 2. Cancel a trigger instance.
 * 3. Query running and historic trigger instances.
 * 4. Recover incomplete trigger instances.
 *
 * Internally, it
 * 1. maintains the list of running trigger instance in memory.
 * 2. updates status, starttime/endtime of trigger instance.
 * 3. persists trigger instance to DB.
 *
 * FlowTriggerService will be leveraged by Quartz scheduler, our new AZ scheduler to schedule
 * triggers.
 *
 * After construction, call {@link #start()} to start the service.
 */

@SuppressWarnings("FutureReturnValueIgnored")
@Singleton
public class FlowTriggerService {

  private static final Logger logger = LoggerFactory.getLogger(FlowTriggerService.class);

  private static final Duration CANCELLING_GRACE_PERIOD_AFTER_RESTART = Duration.ofMinutes(1);
  private static final int RECENTLY_FINISHED_TRIGGER_LIMIT = 50;
  private static final int CANCEL_EXECUTOR_POOL_SIZE = 32;
  private static final int TIMEOUT_EXECUTOR_POOL_SIZE = 8;

  private final ExecutorService flowTriggerExecutorService;
  private final ExecutorService cancelExecutorService;
  private final ScheduledExecutorService timeoutService;
  private final List<TriggerInstance> runningTriggers;
  private final FlowTriggerDependencyPluginManager triggerPluginManager;
  private final TriggerInstanceProcessor triggerProcessor;
  private final FlowTriggerInstanceLoader flowTriggerInstanceLoader;
  private final DependencyInstanceProcessor dependencyProcessor;
  private final FlowTriggerExecutionCleaner cleaner;

  @Inject
  public FlowTriggerService(final FlowTriggerDependencyPluginManager pluginManager,
      final TriggerInstanceProcessor triggerProcessor, final DependencyInstanceProcessor
      dependencyProcessor, final FlowTriggerInstanceLoader flowTriggerInstanceLoader,
      final FlowTriggerExecutionCleaner cleaner) {
    // Give the thread a name to make debugging easier.
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("FlowTrigger-service").build();
    this.flowTriggerExecutorService = Executors.newSingleThreadExecutor(namedThreadFactory);
    namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("FlowTrigger-cancel").build();
    this.cancelExecutorService = Executors
        .newFixedThreadPool(CANCEL_EXECUTOR_POOL_SIZE, namedThreadFactory);
    this.timeoutService = Executors.newScheduledThreadPool(TIMEOUT_EXECUTOR_POOL_SIZE);
    this.runningTriggers = new ArrayList<>();
    this.triggerPluginManager = pluginManager;
    this.triggerProcessor = triggerProcessor;
    this.dependencyProcessor = dependencyProcessor;
    this.flowTriggerInstanceLoader = flowTriggerInstanceLoader;
    this.cleaner = cleaner;
  }

  public void start() throws FlowTriggerDependencyPluginException {
    this.triggerPluginManager.loadAllPlugins();
    this.recoverIncompleteTriggerInstances();
    this.cleaner.start();
  }

  private DependencyInstanceContext createDepContext(final FlowTriggerDependency dep, final long
      startTimeInMills, final String triggerInstId) throws Exception {
    final DependencyCheck dependencyCheck = this.triggerPluginManager
        .getDependencyCheck(dep.getType());
    final DependencyInstanceCallback callback = new DependencyInstanceCallbackImpl(this);

    final Map<String, String> depInstConfig = new HashMap<>();
    depInstConfig.putAll(dep.getProps());
    depInstConfig.put(FlowTriggerProps.DEP_NAME, dep.getName());

    final DependencyInstanceConfigImpl config = new DependencyInstanceConfigImpl(depInstConfig);
    final DependencyInstanceRuntimeProps runtimeProps = new DependencyInstanceRuntimePropsImpl
        (ImmutableMap
            .of(FlowTriggerProps.START_TIME, String.valueOf(startTimeInMills), FlowTriggerProps
                .TRIGGER_INSTANCE_ID, triggerInstId));
    return dependencyCheck.run(config, runtimeProps, callback);
  }

  private TriggerInstance createTriggerInstance(final FlowTrigger flowTrigger, final String flowId,
      final int flowVersion, final String submitUser, final Project project) {
    final String triggerInstId = generateId();
    final long startTime = System.currentTimeMillis();
    // create a list of dependency instances
    final List<DependencyInstance> depInstList = new ArrayList<>();
    for (final FlowTriggerDependency dep : flowTrigger.getDependencies()) {
      final String depName = dep.getName();
      DependencyInstanceContext context = null;
      try {
        context = createDepContext(dep, startTime, triggerInstId);
      } catch (final Exception ex) {
        logger.error("unable to create dependency context for trigger instance[id = {}]",
            triggerInstId, ex);
      }
      // if dependency instance context fails to be created, then its status is cancelled and
      // cause is failure
      final Status status = context == null ? Status.CANCELLED : Status.RUNNING;
      final CancellationCause cause =
          context == null ? CancellationCause.FAILURE : CancellationCause.NONE;
      final long endTime = context == null ? System.currentTimeMillis() : 0;
      final DependencyInstance depInst = new DependencyInstance(depName, startTime, endTime,
          context, status, cause);
      depInstList.add(depInst);
    }

    final TriggerInstance triggerInstance = new TriggerInstance(triggerInstId, flowTrigger,
        flowId, flowVersion, submitUser, depInstList, Constants.UNASSIGNED_EXEC_ID, project);

    return triggerInstance;
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }

  private void scheduleKill(final TriggerInstance triggerInst, final Duration duration, final
  CancellationCause cause) {
    logger
        .debug("cancel trigger instance {} in {} secs", triggerInst.getId(), duration
            .getSeconds());
    this.timeoutService.schedule(() -> {
      cancelTriggerInstance(triggerInst, cause);
    }, duration.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * @return the list of running trigger instances
   */
  public Collection<TriggerInstance> getRunningTriggers() {
    return this.flowTriggerInstanceLoader.getRunning();
  }

  /**
   * @return the list of running trigger instances
   */
  public Collection<TriggerInstance> getRecentlyFinished() {
    return this.flowTriggerInstanceLoader.getRecentlyFinished(RECENTLY_FINISHED_TRIGGER_LIMIT);
  }

  public TriggerInstance findTriggerInstanceById(final String triggerInstanceId) {
    return this.flowTriggerInstanceLoader.getTriggerInstanceById(triggerInstanceId);
  }

  public TriggerInstance findTriggerInstanceByExecId(final int flowExecId) {
    return this.flowTriggerInstanceLoader.getTriggerInstanceByFlowExecId(flowExecId);
  }

  private boolean isDoneButFlowNotExecuted(final TriggerInstance triggerInstance) {
    return triggerInstance.getStatus() == Status.SUCCEEDED && triggerInstance.getFlowExecId() ==
        Constants.UNASSIGNED_EXEC_ID;
  }

  private void recoverRunningOrCancelling(final TriggerInstance triggerInstance) {
    final FlowTrigger flowTrigger = triggerInstance.getFlowTrigger();
    for (final DependencyInstance depInst : triggerInstance.getDepInstances()) {
      if (depInst.getStatus() == Status.RUNNING || depInst.getStatus() == Status.CANCELLING) {
        final FlowTriggerDependency dependency = flowTrigger
            .getDependencyByName(depInst.getDepName());
        DependencyInstanceContext context = null;
        try {
          //recreate dependency instance context
          context = createDepContext(dependency, depInst.getStartTime(), depInst
              .getTriggerInstance().getId());
        } catch (final Exception ex) {
          logger
              .error(
                  "unable to create dependency context for trigger instance[id ="
                      + " {}]", triggerInstance.getId(), ex);
        }
        depInst.setDependencyInstanceContext(context);
        if (context == null) {
          depInst.setStatus(Status.CANCELLED);
          depInst.setCancellationCause(CancellationCause.FAILURE);
        }
      }
    }

    if (triggerInstance.getStatus() == Status.CANCELLING) {
      addToRunningListAndCancel(triggerInstance);
    } else if (triggerInstance.getStatus() == Status.RUNNING) {
      final long remainingTime = remainingTimeBeforeTimeout(triggerInstance);
      addToRunningListAndScheduleKill(triggerInstance, Duration.ofMillis(remainingTime).plus
          (CANCELLING_GRACE_PERIOD_AFTER_RESTART), CancellationCause.TIMEOUT);
    }
  }

  private void recoverTriggerInstance(final TriggerInstance triggerInstance) {
    this.flowTriggerExecutorService.submit(() -> recover(triggerInstance));
  }

  private void recover(final TriggerInstance triggerInstance) {
    logger.info("recovering pending trigger instance {}", triggerInstance.getId());
    if (isDoneButFlowNotExecuted(triggerInstance)) {
      // if trigger instance succeeds but the associated flow hasn't been started yet, then start
      // the flow
      this.triggerProcessor.processSucceed(triggerInstance);
    } else {
      recoverRunningOrCancelling(triggerInstance);
    }
  }

  /**
   * Resume executions of all incomplete trigger instances by recovering the state from db.
   */
  public void recoverIncompleteTriggerInstances() {
    final Collection<TriggerInstance> unfinishedTriggerInstances = this.flowTriggerInstanceLoader
        .getIncompleteTriggerInstances();
    for (final TriggerInstance triggerInstance : unfinishedTriggerInstances) {
      if (triggerInstance.getFlowTrigger() != null) {
        recoverTriggerInstance(triggerInstance);
      } else {
        logger.error("cannot recover the trigger instance {}, flow trigger is null,"
            + " cancelling it ", triggerInstance.getId());

        //finalize unrecoverable trigger instances
        // the following situation would cause trigger instances unrecoverable:
        // 1. project A with flow A associated with flow trigger A is uploaded
        // 2. flow trigger A starts to run
        // 3. project A with flow B without any flow trigger is uploaded
        // 4. web server restarts
        // in this case, flow trigger instance of flow trigger A will be equipped with latest
        // project, thus failing to find the flow trigger since new project doesn't contain flow
        // trigger at all
        if (isDoneButFlowNotExecuted(triggerInstance)) {
          triggerInstance.setFlowExecId(Constants.FAILED_EXEC_ID);
          this.flowTriggerInstanceLoader.updateAssociatedFlowExecId(triggerInstance);
        } else {
          for (final DependencyInstance depInst : triggerInstance.getDepInstances()) {
            if (!Status.isDone(depInst.getStatus())) {
              processStatusAndCancelCauseUpdate(depInst, Status.CANCELLED,
                  CancellationCause.FAILURE);
              this.triggerProcessor.processTermination(depInst.getTriggerInstance());
            }
          }
        }
      }
    }
  }

  private void addToRunningListAndScheduleKill(final TriggerInstance triggerInst, final
  Duration durationBeforeKill, final CancellationCause cause) {
    // if trigger instance is already done
    if (!Status.isDone(triggerInst.getStatus())) {
      this.runningTriggers.add(triggerInst);
      scheduleKill(triggerInst, durationBeforeKill, cause);
    }
  }

  private CancellationCause getCancelleationCause(final TriggerInstance triggerInst) {
    final Set<CancellationCause> causes = triggerInst.getDepInstances().stream()
        .map(DependencyInstance::getCancellationCause).collect(Collectors.toSet());

    if (causes.contains(CancellationCause.FAILURE) || causes
        .contains(CancellationCause.CASCADING)) {
      return CancellationCause.CASCADING;
    } else if (causes.contains(CancellationCause.TIMEOUT)) {
      return CancellationCause.TIMEOUT;
    } else if (causes.contains(CancellationCause.MANUAL)) {
      return CancellationCause.MANUAL;
    } else {
      return CancellationCause.NONE;
    }
  }


  private void cancelTriggerInstance(final TriggerInstance triggerInst) {
    logger.debug("cancelling trigger instance of exec id" + triggerInst.getId());
    final CancellationCause cause = getCancelleationCause(triggerInst);
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      if (depInst.getStatus() == Status.CANCELLING) {
        cancelContextAsync(depInst.getContext());
      } else if (depInst.getStatus() == Status.RUNNING) {
        // sometimes dependency instances of trigger instance in cancelling status can be running.
        // e.x. dep inst1: failure, dep inst2: running -> trigger inst is in killing
        this.processStatusAndCancelCauseUpdate(depInst, Status.CANCELLING, cause);
        cancelContextAsync(depInst.getContext());
      }
    }
  }

  private void addToRunningListAndCancel(final TriggerInstance triggerInst) {
    this.runningTriggers.add(triggerInst);
    cancelTriggerInstance(triggerInst);
  }


  private void updateDepInstStatus(final DependencyInstance depInst, final Status newStatus) {
    depInst.setStatus(newStatus);
    if (Status.isDone(depInst.getStatus())) {
      depInst.setEndTime(System.currentTimeMillis());
    }
  }

  private void processStatusUpdate(final DependencyInstance depInst, final Status newStatus) {
    logger.debug("process status update for " + depInst);
    updateDepInstStatus(depInst, newStatus);
    this.dependencyProcessor.processStatusUpdate(depInst);
  }

  private void processStatusAndCancelCauseUpdate(final DependencyInstance depInst, final Status
      newStatus, final CancellationCause cause) {
    depInst.setCancellationCause(cause);
    updateDepInstStatus(depInst, newStatus);
    this.dependencyProcessor.processStatusUpdate(depInst);
  }


  private long remainingTimeBeforeTimeout(final TriggerInstance triggerInst) {
    final long now = System.currentTimeMillis();
    return Math.max(0,
        triggerInst.getFlowTrigger().getMaxWaitDuration().get().toMillis() - (now - triggerInst
            .getStartTime()));
  }

  /**
   * Start the trigger. The method will be scheduled to invoke by azkaban scheduler.
   */
  public void startTrigger(final FlowTrigger flowTrigger, final String flowId,
      final int flowVersion, final String submitUser, final Project project) {
    final TriggerInstance triggerInst = createTriggerInstance(flowTrigger, flowId, flowVersion,
        submitUser, project);
    this.flowTriggerExecutorService.submit(() -> {
      logger.info("Starting the flow trigger [trigger instance id: {}] by {}",
          triggerInst.getId(), submitUser);
      start(triggerInst);
    });
  }

  private void start(final TriggerInstance triggerInst) {
    this.triggerProcessor.processNewInstance(triggerInst);
    if (triggerInst.getStatus() == Status.CANCELLED) {
      // all dependency instances failed
      logger.info(
          "Trigger instance[id: {}] is cancelled since all dependency instances fail to be created",
          triggerInst.getId());
      this.triggerProcessor.processTermination(triggerInst);
    } else if (triggerInst.getStatus() == Status.CANCELLING) {
      // some of the dependency instances failed
      logger.info(
          "Trigger instance[id: {}] is being cancelled since some dependency instances fail to be created",
          triggerInst.getId());
      addToRunningListAndCancel(triggerInst);
    } else if (triggerInst.getStatus() == Status.SUCCEEDED) {
      this.triggerProcessor.processSucceed(triggerInst);
    } else {
      // todo chengren311: it's possible web server restarts before the db update, then
      // new instance will not be recoverable from db.
      addToRunningListAndScheduleKill(triggerInst, triggerInst.getFlowTrigger()
          .getMaxWaitDuration().get(), CancellationCause.TIMEOUT);
    }
  }

  public TriggerInstance findRunningTriggerInstById(final String triggerInstId) {
    final Future<TriggerInstance> future = this.flowTriggerExecutorService.submit(
        () -> getTriggerInstanceById(triggerInstId)
    );
    try {
      return future.get();
    } catch (final Exception e) {
      logger.error("exception when finding trigger instance by id" + triggerInstId, e);
      return null;
    }
  }

  private TriggerInstance getTriggerInstanceById(final String triggerInstId) {
    return this.runningTriggers.stream()
        .filter(triggerInst -> triggerInst.getId().equals(triggerInstId)).findFirst()
        .orElse(null);
  }

  private void cancelContextAsync(final DependencyInstanceContext context) {
    this.cancelExecutorService.submit(() -> context.cancel());
  }

  /**
   * Cancel a trigger instance
   *
   * @param triggerInst trigger instance to be cancelled
   * @param cause cause of cancelling
   */
  public void cancelTriggerInstance(final TriggerInstance triggerInst,
      final CancellationCause cause) {
    if (triggerInst.getStatus() == Status.RUNNING) {
      this.flowTriggerExecutorService.submit(() -> cancel(triggerInst, cause));
    }
  }

  private void cancel(final TriggerInstance triggerInst, final CancellationCause cause) {
    logger.info("cancelling trigger instance with id {}", triggerInst.getId());
    if (triggerInst != null) {
      for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
        // cancel running dependencies only, no need to cancel a killed/successful dependency
        // instance
        if (depInst.getStatus() == Status.RUNNING) {
          this.processStatusAndCancelCauseUpdate(depInst, Status.CANCELLING, cause);
          cancelContextAsync(depInst.getContext());
        }
      }
    } else {
      logger.debug("unable to cancel a trigger instance in non-running state with id {}",
          triggerInst.getId());
    }
  }

  private DependencyInstance findDependencyInstanceByContext(
      final DependencyInstanceContext context) {
    return this.runningTriggers.stream()
        .flatMap(triggerInst -> triggerInst.getDepInstances().stream()).filter(
            depInst -> depInst.getContext() != null && depInst.getContext() == context)
        .findFirst().orElse(null);
  }

  /**
   * Mark the dependency instance context as success
   */
  public void markDependencySuccess(final DependencyInstanceContext context) {
    this.flowTriggerExecutorService.submit(() -> markSuccess(context));
  }

  private void markSuccess(final DependencyInstanceContext context) {
    final DependencyInstance depInst = findDependencyInstanceByContext(context);
    if (depInst != null) {
      if (Status.isDone(depInst.getStatus())) {
        logger.warn("OnSuccess of dependency instance[id: {}, name: {}] is ignored",
            depInst.getTriggerInstance().getId(), depInst.getDepName());
        return;
      }

      // if the status transits from cancelling to succeeded, then cancellation cause was set,
      // we need to unset cancellation cause.
      this.processStatusAndCancelCauseUpdate(depInst, Status.SUCCEEDED, CancellationCause.NONE);
      // if associated trigger instance becomes success, then remove it from running list
      if (depInst.getTriggerInstance().getStatus() == Status.SUCCEEDED) {
        logger.info("trigger instance[id: {}] succeeded", depInst.getTriggerInstance().getId());
        this.triggerProcessor.processSucceed(depInst.getTriggerInstance());
        this.runningTriggers.remove(depInst.getTriggerInstance());
      }
    } else {
      logger.debug("unable to find trigger instance with context {} when marking it success",
          context);
    }
  }

  private boolean cancelledByAzkaban(final DependencyInstance depInst) {
    return depInst.getStatus() == Status.CANCELLING;
  }

  private boolean cancelledByDependencyPlugin(final DependencyInstance depInst) {
    // When onKill is called by the dependency plugin not through flowTriggerService, we treat it
    // as cancelled by dependency due to failure on dependency side. In this case, cancel cause
    // remains unset.
    return depInst.getStatus() == Status.RUNNING;
  }

  public void markDependencyCancelled(final DependencyInstanceContext context) {
    this.flowTriggerExecutorService.submit(() -> {
      markCancelled(context);
    });
  }

  private void markCancelled(final DependencyInstanceContext context) {
    final DependencyInstance depInst = findDependencyInstanceByContext(context);
    if (depInst != null) {
      if (cancelledByDependencyPlugin(depInst)) {
        processStatusAndCancelCauseUpdate(depInst, Status.CANCELLED, CancellationCause.FAILURE);
        cancelTriggerInstance(depInst.getTriggerInstance());
      } else if (cancelledByAzkaban(depInst)) {
        processStatusUpdate(depInst, Status.CANCELLED);
      } else {
        logger.warn("OnCancel of dependency instance[id: {}, name: {}] is ignored",
            depInst.getTriggerInstance().getId(), depInst.getDepName());
        return;
      }

      if (depInst.getTriggerInstance().getStatus() == Status.CANCELLED) {
        logger.info("trigger instance with execId {} is cancelled",
            depInst.getTriggerInstance().getId());
        this.triggerProcessor.processTermination(depInst.getTriggerInstance());
        this.runningTriggers.remove(depInst.getTriggerInstance());
      }
    } else {
      logger.warn("unable to find trigger instance with context {} when marking "
          + "it cancelled", context);
    }
  }

  /**
   * Shuts down the service immediately.
   */
  public void shutdown() {
    this.flowTriggerExecutorService.shutdown();
    this.cancelExecutorService.shutdown();
    this.timeoutService.shutdown();

    this.flowTriggerExecutorService.shutdownNow();
    this.cancelExecutorService.shutdownNow();
    this.timeoutService.shutdownNow();

    this.triggerProcessor.shutdown();
    this.triggerPluginManager.shutdown();
    this.cleaner.shutdown();
  }

  public Collection<TriggerInstance> getTriggerInstances(final int projectId, final String flowId,
      final int from, final int length) {
    return this.flowTriggerInstanceLoader.getTriggerInstances(projectId, flowId, from, length);
  }
}
