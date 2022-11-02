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
import static azkaban.utils.YarnUtils.YARN_CONF_DIRECTORY_PROPERTY;

import azkaban.Constants.FlowParameters;
import azkaban.cluster.Cluster;
import azkaban.cluster.ClusterRouter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jetbrains.annotations.NotNull;
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

  private static final Logger logger = LoggerFactory.getLogger(ContainerCleanupManager.class);
  private static final Duration DEFAULT_STALE_EXECUTION_CLEANUP_INTERVAL = Duration.ofMinutes(10);
  private static final Duration DEFAULT_STALE_CONTAINER_CLEANUP_INTERVAL = Duration.ofMinutes(60);
  private static final int DEFAULT_AZKABAN_MAX_FLOW_DISPATCHING_MINS = 10;
  private static final int DEFAULT_AZKABAN_MAX_FLOW_PREPARINGING_MINS = 15;
  private static final int DEFAULT_AZKABAN_MAX_FLOW_RUNNING_MINS = 10 * 24 * 60; // 10 days
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
  private final ImmutableMap<Status, Pair<Duration, String>> executionStoppedMap;
  private final ClusterRouter clusterRouter;
  private Map<String, Cluster> allClusters = new HashMap<>();

  public ImmutableMap<Status, Pair<Duration, String>> getValidityMap() {
    return this.validityMap;
  }

  private final ContainerizationMetrics containerizationMetrics;

  @Inject
  public ContainerCleanupManager(
      final Props azkProps,
      final ExecutorLoader executorLoader,
      final ClusterRouter clusterRouter,
      final ContainerizedImpl containerizedImpl,
      final ContainerizedDispatchManager containerizedDispatchManager,
      final ContainerizationMetrics containerizationMetrics) {
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
    // check for flows execution_stopped within 15 minutes
    int maxExecStoppedValidity = azkProps.getInt(
        AZKABAN_MAX_FLOW_EXEC_STOPPED_MINS, DEFAULT_AZKABAN_MAX_FLOW_EXEC_STOPPED_MINS);
    this.cleanupService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("azk-container-cleanup").build());
    this.executorLoader = executorLoader;
    this.containerizedImpl = containerizedImpl;
    this.clusterRouter = clusterRouter;
    this.containerizedDispatchManager = containerizedDispatchManager;
    this.containerizationMetrics = containerizationMetrics;
    long runningFlowValidity = DEFAULT_AZKABAN_MAX_FLOW_RUNNING_MINS;
    try {
      // Added extra buffer of an hour to not conflict with the flow runner cancellation.
      runningFlowValidity = azkProps
          .getLong(AZKABAN_MAX_FLOW_RUNNING_MINS, DEFAULT_AZKABAN_MAX_FLOW_RUNNING_MINS);
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
        .put(Status.FAILED_FINISHING, new Pair<>(Duration.ofMinutes(runningFlowValidity), START_TIME))
        .build();

    this.executionStoppedMap = new Builder<Status, Pair<Duration, String>>()
        .put(Status.EXECUTION_STOPPED,
            new Pair<>(Duration.ofMinutes(maxExecStoppedValidity), UPDATE_TIME))
        .build();

    // get yarn config for instance without robin enabled
    logger.info("AzkProps: hadoop.conf.dir.path=" + azkProps.getString(HADOOP_CONF_DIR_PATH, ""));

    // get config in instance using robin, so as can connect to multiple yarn cluster RMs
    for (Entry<String, Cluster> entry : this.clusterRouter.getAllClusters().entrySet()) {
      logger.info("Now printing cluster named: " + entry.getKey());
      Cluster cluster = entry.getValue();
      logger.info("Cluster detail: " + cluster);
      if (!cluster.getProperties().containsKey(YARN_CONF_DIRECTORY_PROPERTY)) {
        logger.warn("Cluster has no " + YARN_CONF_DIRECTORY_PROPERTY + "defined, skipping");
        continue;
      }
      // this is to avoid "default" cluster collides with other clusters in the map
      String yarnConfDir = cluster.getProperties().getString(YARN_CONF_DIRECTORY_PROPERTY, null);
      if (!allClusters.containsKey(yarnConfDir)) {
        allClusters.put(yarnConfDir, cluster);
      }
    }
  }

  public void cleanUpStaleFlows() {
    this.validityMap.entrySet().stream().filter(e -> !e.getValue().getFirst().isNegative()).map(
        Entry::getKey).forEach(this::cleanUpStaleFlows);
  }

  /**
   * Delete the still alive pods & services of those terminated flows to release resources
   */
  public void cleanUpContainersInTerminalStatuses() {
    logger.info("Cleaning up pods of terminated flow executions");
    Set<Integer> containers = getContainersOfTerminatedFlows();

    for (int executionId : containers) {
      logger.info("Cleaning up the stale pod and service for finished execution: {}",
          executionId);
      deleteContainerQuietly(executionId);
    }
  }

  /**
   * From all pods in the current namespace and Azkaban cluster, select the pods whose executions
   * are not in unfinished statuses, e.g. DISPATCHING, PREPARING, RUNNING, PAUSED, KILLING,
   * FAILED_FINISHING in DB (select pods whose executions are finished/terminated)
   */
  public Set<Integer> getContainersOfTerminatedFlows() {
    Set<Integer> activeFlows = new HashSet<>();
    Set<Integer> currentNameSpacedPods = new HashSet<>();
    Set<Integer> result = new HashSet<>();

    // The unfinished statuses DISPATCHING, PREPARING, RUNNING, PAUSED, KILLING, FAILED_FINISHING.
    // This map will be used to find out all executions of above statuses where
    // submit_time < current system time - 0 from the DB
    ImmutableMap<Status, Pair<Duration, String>> unFinishedStatusesMap =
        this.validityMap.keySet().stream().collect(ImmutableMap.toImmutableMap(e -> e,
            e -> new Pair<>(Duration.ZERO, SUBMIT_TIME)));

    // Obtain all pods in current namespace which are dispatched from current Azkaban cluster
    try {
      currentNameSpacedPods = this.containerizedImpl.getContainersByDuration(Duration.ZERO);
    } catch (final ExecutorManagerException e) {
      logger.error("Unable to obtain the pods in the current namespace and Azkaban cluster", e);
    }
    // Obtain all containerized executions that are in unfinished statuses
    for (Status status : unFinishedStatusesMap.keySet()) {
      try {
        List<ExecutableFlow> flows = this.executorLoader.fetchStaleFlowsForStatus(status,
            unFinishedStatusesMap);
        activeFlows.addAll(flows.stream().map(ExecutableFlow::getExecutionId).collect(Collectors.toSet()));
      } catch (final ExecutorManagerException e) {
        logger.error("Unable to obtain current flows executions of status {}, cannot cross-check "
                + "to-be-killed pods, returning", status, e);
        return result;
      }
    }

    // cross-check: the pod needs to be cleaned up if its execution status is finished
    for (int executionId : currentNameSpacedPods) {
      if (!activeFlows.contains(executionId)) {
        result.add(executionId);
      }
    }
    return result;
  }

  /**
   * Get the flows of EXECUTION_STOPPED status
   */
  @NotNull
  Set<Integer> getExecutionStoppedFlows() {
    Set<Integer> executionStoppedFlows = new HashSet<>();
    if (this.executionStoppedMap.containsKey(Status.EXECUTION_STOPPED)) {
      try {
        List<ExecutableFlow> flows = this.executorLoader.fetchFreshFlowsForStatus(
            Status.EXECUTION_STOPPED, this.executionStoppedMap);
        executionStoppedFlows.addAll(
            flows.stream().map(ExecutableFlow::getExecutionId).collect(Collectors.toSet()));
      } catch (final ExecutorManagerException e) {
        logger.error("Unable to obtain current flows executions of Status.EXECUTION_STOPPED", e);
      }
    }
    return executionStoppedFlows;
  }

  /**
   * Find the executions that their yarn applications needs to be killed, then find the set of to be
   * killed yarn applications, and kill them.
   */
  public void cleanUpDanglingYarnApplications() {
    /*
      1. find those executions of status EXECUTION_STOPPED status within a recent period
      2. find those executions being killed but the pod is still unfinished, and delete the pods
      3. for each of the yarn clusters, do:
        i. get all unfinished yarn applications
        ii. filter and get the yarn applications that within the union set of above executions
        iii. kill these yarn applications
     */
    logger.info("cleanUpDanglingYarnApplications start ");

    Set<Integer> executionStoppedFlows = getExecutionStoppedFlows();
    logger.info("Get executionStoppedFlows: " +
        executionStoppedFlows.stream().map(Object::toString).collect(Collectors.joining(",")));

    // get those flows terminated but containers are still alive (failed to properly killed)
    Set<Integer> toBeCleanedContainers = getContainersOfTerminatedFlows();
    logger.info("Get terminatedContainers: " +
        toBeCleanedContainers.stream().map(Object::toString).collect(Collectors.joining(",")));

    // combine both sets
    toBeCleanedContainers.addAll(executionStoppedFlows);
    logger.info("Get the set of all executions: " +
        toBeCleanedContainers.stream().map(Object::toString).collect(Collectors.joining(",")));

    // TODO: For each of yarn clusters: get applications and kill those in the above set
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
      // If pod is cleaned up when flow is in DISPATCHING or PREPARING state, the
      // container-dispatch-fail meter should be incremented
      if (originalStatus == Status.DISPATCHING || originalStatus == Status.PREPARING) {
        this.containerizationMetrics.markContainerDispatchFail();
      }
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
    this.cleanupService.scheduleAtFixedRate(this::cleanUpContainersInTerminalStatuses, 0L,
        this.containerCleanupIntervalMin, TimeUnit.MINUTES);

    // TODO: currently run-once for development; change to a fixed schedule after development
    //  complete
    this.cleanupService.schedule(this::cleanUpDanglingYarnApplications, 0L, TimeUnit.MINUTES);
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
