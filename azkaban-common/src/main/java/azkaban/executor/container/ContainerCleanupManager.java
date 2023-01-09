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
import static azkaban.utils.YarnUtils.createYarnClient;
import static azkaban.utils.YarnUtils.getAllAliveAppReportsByExecIDs;

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
import azkaban.utils.YarnUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
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
  private static final Duration DEFAULT_YARN_APP_CLEANUP_INTERVAL = Duration.ofMinutes(10);
  private static final int DEFAULT_AZKABAN_MAX_FLOW_DISPATCHING_MINS = 10;
  private static final int DEFAULT_AZKABAN_MAX_FLOW_PREPARINGING_MINS = 15;
  private static final int DEFAULT_AZKABAN_MAX_FLOW_RUNNING_MINS = 10 * 24 * 60; // 10 days
  private static final int DEFAULT_AZKABAN_MAX_FLOW_KILLING_MINS = 15;
  private static final int DEFAULT_AZKABAN_FLOW_RECENT_TERMINATION_MINS = 15;
  private static final int DEFAULT_AZKABAN_YARN_BATCH_KILL_TIMEOUT_IN_MINUTE = 10;
  private static final int DEFAULT_AZKABAN_YARN_BATCH_KILL_PARALLELISM = 5;

  private final long executionCleanupIntervalMin;
  private final long containerCleanupIntervalMin;
  private final long yarnAppCleanupIntervalMin;
  private final int yarnAppKillTimeoutMin;
  private final int yarnAppKillParallelism;

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
  private final ImmutableMap<Status, Pair<Duration, String>> recentTerminatedStatusMap;
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
    this.yarnAppCleanupIntervalMin = azkProps.getLong(
        ContainerizedDispatchManagerProperties.CONTAINERIZED_YARN_APPLICATION_CLEANUP_INTERVAL_MIN,
        DEFAULT_YARN_APP_CLEANUP_INTERVAL.toMinutes());
    this.yarnAppKillTimeoutMin = azkProps.getInt(
        ContainerizedDispatchManagerProperties.CONTAINERIZED_YARN_APPLICATION_CLEANUP_TIMEOUT_MIN,
        DEFAULT_AZKABAN_YARN_BATCH_KILL_TIMEOUT_IN_MINUTE);
    this.yarnAppKillParallelism = azkProps.getInt(
        ContainerizedDispatchManagerProperties.CONTAINERIZED_YARN_APPLICATION_CLEANUP_PARALLELISM,
        DEFAULT_AZKABAN_YARN_BATCH_KILL_PARALLELISM);
    // Get all the validity durations for the validityMap
    int maxDispatchingValidity = azkProps.getInt(
        AZKABAN_MAX_FLOW_DISPATCHING_MINS, DEFAULT_AZKABAN_MAX_FLOW_DISPATCHING_MINS);
    int maxPreparingValidity = azkProps.getInt(
        AZKABAN_MAX_FLOW_PREPARING_MINS, DEFAULT_AZKABAN_MAX_FLOW_PREPARINGING_MINS);
    int maxKillingValidity = azkProps.getInt(
        AZKABAN_MAX_FLOW_KILLING_MINS, DEFAULT_AZKABAN_MAX_FLOW_KILLING_MINS);
    // check for recently (last 15 minutes) terminated flows: status in (execution_stopped, killed,
    // killing, failed, failed_finishing, failed_succeeded, canceled)
    int recentTerminationValidity = azkProps.getInt(
        AZKABAN_FLOW_RECENT_TERMINATION_MINS, DEFAULT_AZKABAN_FLOW_RECENT_TERMINATION_MINS);

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
        .put(Status.DISPATCHING,
            new Pair<>(Duration.ofMinutes(maxDispatchingValidity), SUBMIT_TIME))
        .put(Status.PREPARING, new Pair<>(Duration.ofMinutes(maxPreparingValidity), SUBMIT_TIME))
        .put(Status.RUNNING, new Pair<>(Duration.ofMinutes(runningFlowValidity), START_TIME))
        .put(Status.PAUSED, new Pair<>(Duration.ofMinutes(runningFlowValidity), START_TIME))
        .put(Status.KILLING, new Pair<>(Duration.ofMinutes(maxKillingValidity), UPDATE_TIME))
        .put(Status.FAILED_FINISHING,
            new Pair<>(Duration.ofMinutes(runningFlowValidity), START_TIME))
        .build();

    this.recentTerminatedStatusMap = new Builder<Status, Pair<Duration, String>>()
        .put(Status.EXECUTION_STOPPED,
            new Pair<>(Duration.ofMinutes(recentTerminationValidity), UPDATE_TIME))
        .put(Status.KILLED,
            new Pair<>(Duration.ofMinutes(recentTerminationValidity), UPDATE_TIME))
        .put(Status.KILLING,
            new Pair<>(Duration.ofMinutes(recentTerminationValidity), UPDATE_TIME))
        .put(Status.FAILED,
            new Pair<>(Duration.ofMinutes(recentTerminationValidity), UPDATE_TIME))
        .put(Status.FAILED_FINISHING,
            new Pair<>(Duration.ofMinutes(recentTerminationValidity), UPDATE_TIME))
        .put(Status.FAILED_SUCCEEDED,
            new Pair<>(Duration.ofMinutes(recentTerminationValidity), UPDATE_TIME))
        .put(Status.CANCELLED,
            new Pair<>(Duration.ofMinutes(recentTerminationValidity), UPDATE_TIME))
        .build();

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
    if (allClusters.isEmpty()) {
      // get yarn config for instance without robin enabled
      String hadoopConfDir = azkProps.getString(HADOOP_CONF_DIR_PATH, "");
      if (hadoopConfDir.isEmpty()) {
        logger.warn("No Cluster config or default hadoop-conf-dir-path specified,"
            + " yarn app cleanup will not work");
        return;
      }
      logger.info("AzkProps: hadoop.conf.dir.path=" + hadoopConfDir);
      Props defaultClusterProps = Props.of(YARN_CONF_DIRECTORY_PROPERTY, hadoopConfDir);
      allClusters.put(hadoopConfDir, new Cluster("default", defaultClusterProps));
    }
  }

  public void cleanUpStaleFlows() {
    try {
      this.validityMap.entrySet().stream().filter(e -> !e.getValue().getFirst().isNegative()).map(
          Entry::getKey).forEach(this::cleanUpStaleFlows);
    } catch (Throwable t) {
      logger.warn("Encounter unexpected throwable during cleanup stale flows, "
          + "skipping one schedule run", t);
    }
  }

  /**
   * Delete the still alive pods & services of those terminated flows to release resources
   */
  public void cleanUpContainersInTerminalStatuses() {
    try {
      logger.info("Cleaning up pods of terminated flow executions");
      Set<Integer> containers = getContainersOfTerminatedFlows();

      for (int executionId : containers) {
        logger.info("Cleaning up the stale pod and service for finished execution: {}",
            executionId);
        deleteContainerQuietly(executionId);
      }
    } catch (Throwable t) {
      logger.warn("Encounter unexpected throwable during cleanup containers in terminal statuses, "
          + "skipping one schedule run", t);
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
        activeFlows.addAll(
            flows.stream().map(ExecutableFlow::getExecutionId).collect(Collectors.toSet()));
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
   * Get the flows recently (last 15 minutes) terminated: status in (execution_stopped, killed,
   * killing, failed, failed_finishing, failed_succeeded, canceled)
   */
  @NotNull
  Set<Integer> getRecentlyTerminatedFlows() {
    Set<Integer> recentlyTerminatedFlows = new HashSet<>();
    this.recentTerminatedStatusMap.forEach((status, value) -> {
      try {
        List<ExecutableFlow> flows = this.executorLoader.fetchFreshFlowsForStatus(
            status, this.recentTerminatedStatusMap);
        recentlyTerminatedFlows.addAll(
            flows.stream().map(ExecutableFlow::getExecutionId).collect(Collectors.toSet()));
        logger.info("Got recently terminated flows executions of Status " + status + ": " +
            flows.stream().map(ExecutableFlow::getExecutionId).map(Objects::toString)
                .collect(Collectors.joining(",")));
      } catch (final ExecutorManagerException e) {
        logger.error("Unable to obtain current flows executions of Status " + status, e);
      }
    });
    return recentlyTerminatedFlows;
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
    try {
      logger.info("cleanUpDanglingYarnApplications start ");

      Set<Integer> recentlyTerminatedFlows = getRecentlyTerminatedFlows();
      // get those flows terminated but containers are still alive (failed to properly killed)
      Set<Integer> toBeCleanedContainers = getContainersOfTerminatedFlows();
      logger.info("Get terminatedContainers: " +
          toBeCleanedContainers.stream().map(Object::toString).collect(Collectors.joining(",")));

      // combine both sets
      toBeCleanedContainers.addAll(recentlyTerminatedFlows);
      logger.info("The whole set of all executions to clean yarn apps: " +
          toBeCleanedContainers.stream().map(Object::toString).collect(Collectors.joining(",")));

      if (toBeCleanedContainers.isEmpty()) {
        logger.info("No execution needs to kill yarn application, exit");
        return;
      }

      // For each of yarn clusters: find applications of the above executionIDs and kill them
      for (Entry<String, Cluster> entry : this.allClusters.entrySet()) {
        logger.info("clean up yarn applications in cluster:" + entry.getValue().getClusterId());
        cleanUpYarnApplicationsInCluster(toBeCleanedContainers, entry.getValue());
      }
    } catch (Throwable t) {
      logger.warn("Encounter unexpected throwable during cleanup dangling yarn app, "
          + "skipping one schedule run", t);
    }
  }

  void cleanUpYarnApplicationsInCluster(Set<Integer> toBeCleanedContainers,
      Cluster cluster) {
    org.apache.log4j.Logger apacheLogger =
        org.apache.log4j.Logger.getLogger(ContainerCleanupManager.class);

    List<ApplicationReport> aliveApplications;
    try {
      logger.debug("Getting all yarn apps for cluster:" + cluster.getClusterId());
      YarnClient yarnClient = createYarnClient(cluster.getProperties(), apacheLogger);
      aliveApplications = getAllAliveAppReportsByExecIDs(
          yarnClient, toBeCleanedContainers, apacheLogger);
      logger.info("aliveApplications.size() = " + aliveApplications.size());
      logger.info("appsToBeKilled = " +
          aliveApplications.stream().map(app -> app.getApplicationId().toString())
              .collect(Collectors.joining(",")));
    } catch (Exception e) {
      logger.error("fail to get yarn applications by execution IDs from cluster "
          + cluster.getClusterId() + ", exiting", e);
      containerizationMetrics.markYarnGetApplicationsFail();
      return;
    }

    // Use a fix thread pool to concurrently kill yarn apps
    Map<String, Boolean> appsSuccessfulKilled = new ConcurrentHashMap<>();
    aliveApplications.forEach(
        app -> appsSuccessfulKilled.put(app.getApplicationId().toString(), false));

    ExecutorService yarnKillThreadPool = Executors.newFixedThreadPool(
        this.yarnAppKillParallelism);
    aliveApplications.forEach(app ->
        yarnKillThreadPool.execute(new Runnable() {
          @Override
          public void run() {
            try {
              YarnUtils.killApplicationAsProxyUser(cluster, app, apacheLogger);
              appsSuccessfulKilled.put(app.getApplicationId().toString(), true);
            } catch (Exception e) {
              logger.warn("Error killing yarn application: " + app.getApplicationId(), e);
            }
          }
        })
    );
    // wait for them to finish for up to a <timeout value> period of time
    try {
      yarnKillThreadPool.shutdown();
      if (yarnKillThreadPool.awaitTermination(this.yarnAppKillTimeoutMin, TimeUnit.MINUTES)) {
        logger.info("Yarn application killing threads all successfully terminated");
      } else {
        logger.info("Yarn application killing threads not all terminated as expected");
      }
    } catch (InterruptedException e) {
      logger.warn("Error awaiting the termination of all the Yarn application killing threads",
          e);
    }

    // report the kill results
    logger.info(
        "Successfully killed yarn applications: " + appsSuccessfulKilled.entrySet().stream()
            .filter(Entry::getValue).map(Entry::getKey).collect(Collectors.joining(",")));
    if (appsSuccessfulKilled.containsValue(false)) {
      List<String> failed = appsSuccessfulKilled.entrySet().stream()
          .filter(entry -> !entry.getValue()).map(Entry::getKey).collect(Collectors.toList());
      logger.warn("Failed to kill Yarn applications: " + String.join(",", failed));
      containerizationMetrics.markYarnApplicationKillFail(failed.size());
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
   *
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
    if (this.yarnAppCleanupIntervalMin <= 0) {
      logger.warn("Yarn application cleanup schedule not started: "
          + "invalid time interval " + this.yarnAppCleanupIntervalMin + ", please correct value "
          + ContainerizedDispatchManagerProperties.CONTAINERIZED_YARN_APPLICATION_CLEANUP_INTERVAL_MIN);
    } else {
      this.cleanupService.scheduleAtFixedRate(this::cleanUpDanglingYarnApplications, 0L,
          this.yarnAppCleanupIntervalMin, TimeUnit.MINUTES);
    }
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
