/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.executor.container;

import static azkaban.Constants.ConfigurationKeys.*;
import static azkaban.Constants.ContainerizedDispatchManagerProperties;

import azkaban.Constants.FlowParameters;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provides the ability to cleanup execution containers which exceed the maximum allowed
 * duration for a flow. Following extensions will be considered in future. (1) Convert this to a
 * general background process for any other containerization related periodic tasks. (2) Add ability
 * to track containers which may not have been deleted despite prior attempts. (3) It's assumed
 * final status updates to the db are handled by other parts, such as flow-container. That can also
 * be added here as a failsafe if needed.
 */
@Singleton
public class ContainerCleanupManager {

  public static final int DEFAULT_AZKABAN_MAX_FLOW_RUNNING_MINS = -1;
  private static final Logger logger = LoggerFactory.getLogger(ContainerCleanupManager.class);
  private static final Duration DEFAULT_STALE_EXECUTION_CLEANUP_INTERVAL = Duration.ofMinutes(10);
  private static final Duration DEFAULT_STALE_CONTAINER_CLEANUP_INTERVAL = Duration.ofMinutes(60);
  private static final int DEFAULT_AZKABAN_MAX_FLOW_DISPATCHING_MINS = 10;
  private static final int DEFAULT_AZKABAN_MAX_FLOW_PREPARINGING_MINS = 15;
  private static final int DEFAULT_AZKABAN_MAX_FLOW_KILLING_MINS = 15;
  private static final int DEFAULT_AZKABAN_MAX_FLOW_EXEC_STOPPED_MINS = 15;

  private final long executionCleanupIntervalMin;
  private final long containerCleanupIntervalMin;
  private final ScheduledExecutorService cleanupService;
  private final ExecutorLoader executorLoader;
  private final ContainerizedImpl containerizedImpl;
  private final ContainerizedDispatchManager containerizedDispatchManager;

  private static final String SUBMIT_TIME = "submit_time";
  private static final String START_TIME = "start_time";
  private static final String UPDATE_TIME = "update_time";
  // Defines the validity duration associated with certain statuses from the
  // submit/start/update time.
  private final ImmutableMap<Status, Pair<Duration, String>> validityMap;

  public ImmutableMap<Status, Pair<Duration, String>> getValidityMap() {
    return this.validityMap;
  }

  @Inject
  public ContainerCleanupManager(final Props azkProps, final ExecutorLoader executorLoader,
      final ContainerizedImpl containerizedImpl,
      final ContainerizedDispatchManager containerizedDispatchManager) {
    // Get all the intervals
    this.executionCleanupIntervalMin = azkProps
        .getLong(
            ContainerizedDispatchManagerProperties.CONTAINERIZED_STALE_EXECUTION_CLEANUP_INTERVAL_MIN,
            DEFAULT_STALE_EXECUTION_CLEANUP_INTERVAL.toMinutes());
    this.containerCleanupIntervalMin = azkProps.getLong(
        ContainerizedDispatchManagerProperties.CONTAINERIZED_STALE_CONTAINER_CLEANUP_INTERVAL_MIN,
        DEFAULT_STALE_CONTAINER_CLEANUP_INTERVAL.toMinutes());
    // Get all the validity durations for the validityMap
    int maxDispatchingValidity = azkProps.getInt(
        AZKABAN_MAX_FLOW_DISPATCHING_MINS, DEFAULT_AZKABAN_MAX_FLOW_DISPATCHING_MINS);
    int maxPreparingValidity = azkProps.getInt(
        AZKABAN_MAX_FLOW_PREPARING_MINS, DEFAULT_AZKABAN_MAX_FLOW_PREPARINGING_MINS);
    int maxKillingValidity = azkProps.getInt(
        AZKABAN_MAX_FLOW_KILLING_MINS, DEFAULT_AZKABAN_MAX_FLOW_KILLING_MINS);
    int maxExecStoppedValidity = azkProps.getInt(
        AZKABAN_MAX_FLOW_EXEC_STOPPED_MINS, DEFAULT_AZKABAN_MAX_FLOW_EXEC_STOPPED_MINS);
    this.cleanupService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("azk-container-cleanup").build());
    this.executorLoader = executorLoader;
    this.containerizedImpl = containerizedImpl;
    this.containerizedDispatchManager = containerizedDispatchManager;
    long runningFlowValidity = DEFAULT_AZKABAN_MAX_FLOW_RUNNING_MINS;
    try {
      // Added extra buffer of an hour to not conflict with the flow runner cancellation.
      runningFlowValidity = azkProps
          .getLong(AZKABAN_MAX_FLOW_RUNNING_MINS, DEFAULT_AZKABAN_MAX_FLOW_RUNNING_MINS);
      runningFlowValidity = runningFlowValidity > 0 ? runningFlowValidity + 60 :
          runningFlowValidity;
    } catch (NumberFormatException ne) {
      logger
          .info("NumberFormatException while parsing value for: " + AZKABAN_MAX_FLOW_RUNNING_MINS);
    }

    this.validityMap = new Builder<Status,
        Pair<Duration, String>>()
        .put(Status.DISPATCHING, new Pair<>(Duration.ofMinutes(maxDispatchingValidity), SUBMIT_TIME))
        .put(Status.PREPARING, new Pair<>(Duration.ofMinutes(maxPreparingValidity), SUBMIT_TIME))
        .put(Status.RUNNING, new Pair<>(Duration.ofMinutes(runningFlowValidity), START_TIME))
        .put(Status.PAUSED, new Pair<>(Duration.ofMinutes(runningFlowValidity), START_TIME))
        .put(Status.KILLING, new Pair<>(Duration.ofMinutes(maxKillingValidity), UPDATE_TIME))
        .put(Status.EXECUTION_STOPPED, new Pair<>(Duration.ofMinutes(maxExecStoppedValidity), UPDATE_TIME))
        .put(Status.FAILED_FINISHING, new Pair<>(Duration.ofMinutes(runningFlowValidity), START_TIME))
        .build();

  }

  public void cleanUpStaleFlows() {
    this.validityMap.entrySet().stream().filter(e -> !e.getValue().getFirst().isNegative()).map(
        Entry::getKey).forEach(this::cleanUpStaleFlows);
  }

  /**
   * Try to clean the stale containers that are older than maximum azkaban flow running time
   */
  public void cleanUpStaleContainers() {
    Duration containerValidity = validityMap.get(Status.RUNNING).getFirst();
    if (!containerValidity.isNegative()) {
      try {
        this.containerizedImpl.deleteAgedContainers(containerValidity);
      } catch (final Exception e) {
        logger.error("Exception occurred while cleaning up stale pods and services." + e);
      }
    }
  }

  /**
   * Try cleaning the stale flows for a given status. This will try to cancel the flow, if
   * unreachable, flow will be finalized. Pod Container will be deleted.
   *
   * @param status
   */
  public void cleanUpStaleFlows(final Status status) {
    logger.info("Cleaning up stale flows for status: " + status.name());
    List<ExecutableFlow> staleFlows;
    try {
      staleFlows = this.executorLoader.fetchStaleFlowsForStatus(status, this.validityMap);
    } catch (final Exception e) {
      logger.error("Exception occurred while fetching stale flows during clean up." + e);
      return;
    }
    for (final ExecutableFlow flow : staleFlows) {
      if (shouldIgnore(flow, status)) {
        continue;
      }
      Status originalStatus = flow.getStatus();
      cancelFlowQuietly(flow, originalStatus);
      retryFlowQuietly(flow, originalStatus);
      deleteContainerQuietly(flow.getExecutionId());
    }
  }

  /**
   * Handles special cases. If the flow is in PREPARING STATE and enable.dev.pod=true then ignore
   * executions from the last 2 days.
   *
   * @param flow
   * @param status
   * @return
   */
  private boolean shouldIgnore(final ExecutableFlow flow, final Status status) {
    if (status != Status.PREPARING) {
      return false;
    }
    final ExecutionOptions executionOptions = flow.getExecutionOptions();
    if (null == executionOptions) {
      return false;
    }
    boolean isDevPod = Boolean.parseBoolean(
        executionOptions.getFlowParameters().get(FlowParameters.FLOW_PARAM_ENABLE_DEV_POD));
    if (!isDevPod) {
      return false;
    }
    return flow.getSubmitTime() > System.currentTimeMillis() - Duration.ofDays(2).toMillis();
  }

  /**
   * Try to quietly cancel the flow. Cancel flow tries to gracefully kill the executions if they are
   * reachable, otherwise, flow will be finalized.
   *
   * @param flow
   */
  private void cancelFlowQuietly(ExecutableFlow flow, Status originalStatus) {
    try {
      logger.info(
          "Cleaning up stale flow " + flow.getExecutionId() + " in state " + originalStatus
              .name());
      this.containerizedDispatchManager.cancelFlow(flow, flow.getSubmitUser());
    } catch (final Exception e) {
      logger.error("Unexpected Exception while canceling and finalizing flow during clean up." + e);
    }
  }

  /**
   * Quietly retry flow if it is terminated in statuses prior to RUNNING
   * @param flow
   * @param originalStatus
   */
  private void retryFlowQuietly(ExecutableFlow flow, Status originalStatus) {
    try {
      logger.info("Restarting cleaned up flow " + flow.getExecutionId());
      ExecutionControllerUtils.restartFlow(flow, originalStatus);
    } catch (final Exception e) {
      logger.error("Unexpected Exception while restarting flow during clean up." + e);
    }
  }

  // Deletes the container specified by executionId while logging and consuming any exceptions.
  // Note that while this method is not async it's still expected to return 'quickly'. This is true
  // for Kubernetes as it's declarative API will only submit the request for deleting container
  // resources. In future we can consider making this async to eliminate any chance of the cleanup
  // thread getting blocked.
  private void deleteContainerQuietly(final int executionId) {
    try {
      this.containerizedImpl.deleteContainer(executionId);
    } catch (final Exception e) {
      logger.error("Unexpected exception while deleting container.", e);
    }
  }

  /**
   * Start periodic deletions of per-container resources for stale executions.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    logger.info("Start container cleanup service");
    // Default execution clean up interval is 10 min, container clean up interval is 60 min
    this.cleanupService.scheduleAtFixedRate(this::cleanUpStaleFlows, 0L,
        this.executionCleanupIntervalMin, TimeUnit.MINUTES);
    this.cleanupService.scheduleAtFixedRate(this::cleanUpStaleContainers, 0L,
        this.containerCleanupIntervalMin, TimeUnit.MINUTES);
  }

  /**
   * Stop the periodic container cleanups.
   */
  public void shutdown() {
    logger.info("Shutdown container cleanup service");
    this.cleanupService.shutdown();
    try {
      if (!this.cleanupService.awaitTermination(30, TimeUnit.SECONDS)) {
        this.cleanupService.shutdownNow();
      }
    } catch (final InterruptedException ex) {
      this.cleanupService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
