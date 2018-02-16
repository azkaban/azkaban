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
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
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

  private static final Duration CANCELLING_GRACE_PERIOD_AFTER_RESTART = Duration.ofMinutes(1);
  private static final int RECENTLY_FINISHED_TRIGGER_LIMIT = 20;
  private static final String START_TIME = "starttime";
  private static final Logger logger = LoggerFactory.getLogger(FlowTriggerService.class);
  private final ExecutorService executorService;
  private final List<TriggerInstance> runningTriggers;
  private final ScheduledExecutorService timeoutService;
  private final FlowTriggerDependencyPluginManager triggerPluginManager;
  private final TriggerInstanceProcessor triggerProcessor;
  private final FlowTriggerInstanceLoader flowTriggerInstanceLoader;
  private final DependencyInstanceProcessor dependencyProcessor;

  @Inject
  public FlowTriggerService(final FlowTriggerDependencyPluginManager pluginManager,
      final TriggerInstanceProcessor triggerProcessor, final DependencyInstanceProcessor
      dependencyProcessor, final FlowTriggerInstanceLoader flowTriggerInstanceLoader) {
    // Give the thread a name to make debugging easier.
    final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("FlowTrigger-service").build();
    this.executorService = Executors.newSingleThreadExecutor(namedThreadFactory);
    this.timeoutService = Executors.newScheduledThreadPool(8);
    this.runningTriggers = new ArrayList<>();
    this.triggerPluginManager = pluginManager;
    this.triggerProcessor = triggerProcessor;
    this.dependencyProcessor = dependencyProcessor;
    this.flowTriggerInstanceLoader = flowTriggerInstanceLoader;
  }

  public void start() throws FlowTriggerDependencyPluginException {
    this.triggerPluginManager.loadAllPlugins();
    this.recoverIncompleteTriggerInstances();
  }

  private DependencyInstanceContext createDepContext(final FlowTriggerDependency dep, final long
      starttimeInMills) throws Exception {
    final DependencyCheck dependencyCheck = this.triggerPluginManager
        .getDependencyCheck(dep.getType());
    final DependencyInstanceCallback callback = new DependencyInstanceCallbackImpl(this);
    final DependencyInstanceConfigImpl config = new DependencyInstanceConfigImpl(dep.getProps());
    final DependencyInstanceRuntimeProps runtimeProps = new DependencyInstanceRuntimePropsImpl
        (ImmutableMap.of(START_TIME, String.valueOf(starttimeInMills)));
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
      final Date startDate = new Date(startTime);
      DependencyInstanceContext context = null;
      try {
        context = createDepContext(dep, startTime);
      } catch (final Exception ex) {
        logger.error(String.format("unable to create dependency context for trigger instance[id ="
            + " %s]", triggerInstId), ex);
      }
      // if dependency instance context fails to be created, then its status is cancelled and
      // cause is failure
      final Status status = context == null ? Status.CANCELLED : Status.RUNNING;
      final CancellationCause cause =
          context == null ? CancellationCause.FAILURE : CancellationCause.NONE;
      final Date endTime = context == null ? new Date() : null;
      final DependencyInstance depInst = new DependencyInstance(depName, startDate, endTime,
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
        .debug(String.format("Cancel trigger instance %s in %s secs", triggerInst.getId(), duration
            .getSeconds()));
    this.timeoutService.schedule(() -> {
      cancel(triggerInst, cause);
    }, duration.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * @return the list of running trigger instances
   */
  public Collection<TriggerInstance> getRunningTriggers() {
    final Future future = this.executorService.submit(
        (Callable) () -> FlowTriggerService.this.runningTriggers);

    List<TriggerInstance> triggerInstanceList = new ArrayList<>();
    try {
      triggerInstanceList = (List<TriggerInstance>) future.get();
    } catch (final Exception ex) {
      logger.error("error in getting running triggers", ex);
    }
    return triggerInstanceList;
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
          context = createDepContext(dependency, depInst.getStartTime().getTime());
        } catch (final Exception ex) {
          logger
              .error(
                  String.format("unable to create dependency context for trigger instance[id ="
                      + " %s]", triggerInstance.getId()), ex);
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

  private void recover(final TriggerInstance triggerInstance) {
    this.executorService.submit(() -> {
      logger.info(String.format("recovering pending trigger instance %s", triggerInstance.getId
          ()));
      if (isDoneButFlowNotExecuted(triggerInstance)) {
        // if trigger instance succeeds but the associated flow hasn't been started, then start
        // the flow
        this.triggerProcessor.processSucceed(triggerInstance);
      } else {
        recoverRunningOrCancelling(triggerInstance);
      }
    });
  }

  /**
   * Resume executions of all incomplete trigger instances by recovering the state from db.
   */
  public void recoverIncompleteTriggerInstances() {
    final Collection<TriggerInstance> unfinishedTriggerInstances = this.flowTriggerInstanceLoader
        .getIncompleteTriggerInstances();
    //todo chengren311: what if flow trigger is not found?
    for (final TriggerInstance triggerInstance : unfinishedTriggerInstances) {
      if (triggerInstance.getFlowTrigger() != null) {
        recover(triggerInstance);
      } else {
        logger.error(String.format("cannot recover the trigger instance %s, flow trigger is null ",
            triggerInstance.getId()));
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
        depInst.getContext().cancel();
      } else if (depInst.getStatus() == Status.RUNNING) {
        // sometimes dependency instances of trigger instance in cancelling status can be running.
        // e.x. dep inst1: failure, dep inst2: running -> trigger inst is in killing
        this.processStatusAndCancelCauseUpdate(depInst, Status.CANCELLING, cause);
        depInst.getContext().cancel();
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
      depInst.setEndTime(new Date());
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
    return Math.max(0, triggerInst.getFlowTrigger().getMaxWaitDuration().toMillis() - (now -
        triggerInst.getStartTime().getTime()));
  }

  /**
   * Start the trigger. The method will be scheduled to invoke by azkaban scheduler.
   */
  public void startTrigger(final FlowTrigger flowTrigger, final String flowId,
      final int flowVersion, final String submitUser, final Project project) {
    this.executorService.submit(() -> {
      final TriggerInstance triggerInst = createTriggerInstance(flowTrigger, flowId, flowVersion,
          submitUser, project);

      logger.info(
          String.format("Starting the flow trigger %s[trigger instance id: %s] by %s", flowTrigger,
              triggerInst.getId(), submitUser));

      this.triggerProcessor.processNewInstance(triggerInst);
      if (triggerInst.getStatus() == Status.CANCELLED) {
        // all dependency instances failed
        logger.info(String.format("Trigger instance[id: %s] is cancelled since all dependency "
            + "instances fail to be created", triggerInst.getId()));
        this.triggerProcessor.processTermination(triggerInst);
      } else if (triggerInst.getStatus() == Status.CANCELLING) {
        // some of the dependency instances failed
        logger.info(
            String.format("Trigger instance[id: %s] is being cancelled since some dependency "
                + "instances fail to be created", triggerInst.getId()));
        addToRunningListAndCancel(triggerInst);
      } else if (triggerInst.getStatus() == Status.SUCCEEDED) {
        this.triggerProcessor.processSucceed(triggerInst);
      } else {
        // todo chengren311: it's possible web server restarts before the db update, then
        // new instance will not be recoverable from db.
        addToRunningListAndScheduleKill(triggerInst, triggerInst.getFlowTrigger()
            .getMaxWaitDuration(), CancellationCause.TIMEOUT);
      }
    });
  }

  private FlowTriggerDependency getFlowTriggerDepByName(final FlowTrigger flowTrigger,
      final String depName) {
    return flowTrigger.getDependencies().stream().filter(ftd -> ftd.getName().equals(depName))
        .findFirst().orElse(null);
  }

  public TriggerInstance findRunningTriggerInstById(final String triggerInstId) {
    //todo chengren311: make the method single threaded
    final Future<TriggerInstance> future = this.executorService.submit(
        () -> this.runningTriggers.stream()
            .filter(triggerInst -> triggerInst.getId().equals(triggerInstId)).findFirst()
            .orElse(null)
    );
    try {
      return future.get();
    } catch (final Exception e) {
      logger.error("exception when finding trigger instance by id" + triggerInstId, e);
      return null;
    }
  }

  /**
   * Cancel a trigger instance
   *
   * @param triggerInst trigger instance to be cancelled
   * @param cause cause of cancelling
   */
  public void cancel(final TriggerInstance triggerInst, final CancellationCause cause) {
    this.executorService.submit(
        () -> {
          logger.info(
              String.format("cancelling trigger instance with id %s", triggerInst.getId()));
          if (triggerInst != null) {
            for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
              // cancel only running dependencies, no need to cancel a killed/successful dependency
              // instance
              if (depInst.getStatus() == Status.RUNNING) {
                this.processStatusAndCancelCauseUpdate(depInst, Status.CANCELLING, cause);
                depInst.getContext().cancel();
              }
            }
          } else {
            logger.debug(String
                .format("unable to cancel a trigger instance in non-running state with id %s",
                    triggerInst.getId()));
          }
        }
    );
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
    this.executorService.submit(() -> {
      final DependencyInstance depInst = findDependencyInstanceByContext(context);
      if (depInst != null) {
        if (Status.isDone(depInst.getStatus())) {
          logger.warn(String.format("OnSuccess of dependency instance[id: %s, name: %s] is ignored",
              depInst.getTriggerInstance().getId(), depInst.getDepName()));
          return;
        }

        logger.info(
            String.format("setting dependency instance[id: %s, name: %s] status to succeeded",
                depInst.getTriggerInstance().getId(), depInst.getDepName()));
        processStatusUpdate(depInst, Status.SUCCEEDED);
        // if associated trigger instance becomes success, then remove it from running list
        if (depInst.getTriggerInstance().getStatus() == Status.SUCCEEDED) {
          logger.info(String.format("trigger instance[id: %s] succeeded",
              depInst.getTriggerInstance().getId()));
          this.triggerProcessor.processSucceed(depInst.getTriggerInstance());
          this.runningTriggers.remove(depInst.getTriggerInstance());
        }
      } else {
        logger.debug(String.format("unable to find trigger instance with context %s when marking "
                + "it success",
            context));
      }
    });
  }

  private boolean cancelledByAzkaban(final DependencyInstance depInst) {
    return depInst.getStatus() == Status.CANCELLING && (
        depInst.getCancellationCause() == CancellationCause
            .MANUAL || depInst.getCancellationCause() == CancellationCause.TIMEOUT || depInst
            .getCancellationCause() == CancellationCause.CASCADING);
  }

  private boolean cancelledByDependencyPlugin(final DependencyInstance depInst) {
    // When onKill is called by the dependency plugin not through flowTriggerService, we treat it
    // as cancelled by dependency due to failure on dependency side. In this case, cancel cause
    // remains unset.
    return depInst.getStatus() == Status.CANCELLED && (depInst.getCancellationCause()
        == CancellationCause.NONE);
  }

  public void markDependencyCancelled(final DependencyInstanceContext context) {
    this.executorService.submit(() -> {
      final DependencyInstance depInst = findDependencyInstanceByContext(context);
      if (depInst != null) {
        logger.info(
            String.format("setting dependency instance[id: %s, name: %s] status to cancelled",
                depInst.getTriggerInstance().getId(), depInst.getDepName()));
        if (cancelledByDependencyPlugin(depInst)) {
          processStatusAndCancelCauseUpdate(depInst, Status.CANCELLING, CancellationCause.FAILURE);
          cancelTriggerInstance(depInst.getTriggerInstance());
        } else if (cancelledByAzkaban(depInst)) {
          processStatusUpdate(depInst, Status.CANCELLED);
        } else {
          logger.warn(String.format("OnCancel of dependency instance[id: %s, name: %s] is ignored",
              depInst.getTriggerInstance().getId(), depInst.getDepName()));
          return;
        }

        if (depInst.getTriggerInstance().getStatus() == Status.CANCELLED) {
          logger.info(
              String.format("trigger instance with execId %s is cancelled",
                  depInst.getTriggerInstance().getId()));
          this.triggerProcessor.processTermination(depInst.getTriggerInstance());
          this.runningTriggers.remove(depInst.getTriggerInstance());
        }
      } else {
        logger.warn(String.format("unable to find trigger instance with context %s when marking "
            + "it cancelled", context));
      }
    });
  }

  /**
   * Shuts down the service immediately.
   */
  public void shutdown() {
    this.executorService.shutdown(); // Disable new tasks from being submitted
    this.executorService.shutdownNow(); // Cancel currently executing tasks
    this.triggerPluginManager.shutdown();
  }
}
