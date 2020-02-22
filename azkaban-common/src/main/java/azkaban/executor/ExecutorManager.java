/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.executor;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.FlowContainerization;
import azkaban.event.EventHandler;
import azkaban.executor.selector.ExecutorComparator;
import azkaban.executor.selector.ExecutorFilter;
import azkaban.executor.selector.ExecutorSelector;
import azkaban.flow.FlowUtils;
import azkaban.metrics.CommonMetrics;
import azkaban.project.Project;
import azkaban.project.ProjectWhitelist;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.linkedin.tony.TonyClient;
import com.linkedin.tony.cli.ClusterSubmitter;
import java.io.File;
import java.io.IOException;
import java.lang.Thread.State;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * Executor manager used to manage the client side job.
 *
 * @deprecated replaced by {@link ExecutionController}
 */
@Singleton
@Deprecated
public class ExecutorManager extends EventHandler implements
    ExecutorManagerAdapter {

  // 12 weeks
  private static final long DEFAULT_EXECUTION_LOGS_RETENTION_MS = 3 * 4 * 7
      * 24 * 60 * 60 * 1000L;
  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(10);
  private static final Logger logger = Logger.getLogger(ExecutorManager.class);
  private final RunningExecutions runningExecutions;
  private final Props azkProps;
  private final CommonMetrics commonMetrics;
  private final ExecutorLoader executorLoader;
  private final CleanerThread cleanerThread;
  private final RunningExecutionsUpdaterThread updaterThread;
  private final ExecutorApiGateway apiGateway;
  private final int maxConcurrentRunsOneFlow;
  private final Map<Pair<String, String>, Integer> maxConcurrentRunsPerFlowMap;
  private final ExecutorManagerUpdaterStage updaterStage;
  private final ExecutionFinalizer executionFinalizer;
  private final ActiveExecutors activeExecutors;
  private final ExecutorService executorInfoRefresherService;
  QueuedExecutions queuedFlows;
  File cacheDir;
  private QueueProcessorThread queueProcessor;
  private volatile Pair<ExecutionReference, ExecutableFlow> runningCandidate = null;
  private List<String> filterList;
  private Map<String, Integer> comparatorWeightsMap;
  private long lastSuccessfulExecutorInfoRefresh;
  private Duration sleepAfterDispatchFailure = Duration.ofSeconds(1L);
  private boolean initialized = false;

  @Inject
  public ExecutorManager(final Props azkProps, final ExecutorLoader executorLoader,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway,
      final RunningExecutions runningExecutions,
      final ActiveExecutors activeExecutors,
      final ExecutorManagerUpdaterStage updaterStage,
      final ExecutionFinalizer executionFinalizer,
      final RunningExecutionsUpdaterThread updaterThread) throws ExecutorManagerException {
    this.azkProps = azkProps;
    this.commonMetrics = commonMetrics;
    this.executorLoader = executorLoader;
    this.apiGateway = apiGateway;
    this.runningExecutions = runningExecutions;
    this.activeExecutors = activeExecutors;
    this.updaterStage = updaterStage;
    this.executionFinalizer = executionFinalizer;
    this.updaterThread = updaterThread;
    this.maxConcurrentRunsOneFlow = ExecutorUtils.getMaxConcurrentRunsOneFlow(azkProps);
    this.maxConcurrentRunsPerFlowMap = ExecutorUtils.getMaxConcurentRunsPerFlowMap(azkProps);
    this.cleanerThread = createCleanerThread();
    this.executorInfoRefresherService = createExecutorInfoRefresherService();
  }

  private CleanerThread createCleanerThread() {
    final long executionLogsRetentionMs = this.azkProps.getLong("execution.logs.retention.ms",
        DEFAULT_EXECUTION_LOGS_RETENTION_MS);
    return new CleanerThread(executionLogsRetentionMs);
  }

  void initialize() throws ExecutorManagerException {
    if (this.initialized) {
      return;
    }
    this.initialized = true;
    this.setupExecutors();
    this.loadRunningExecutions();
    this.queuedFlows = new QueuedExecutions(
        this.azkProps.getLong(ConfigurationKeys.WEBSERVER_QUEUE_SIZE, 100000));
    this.loadQueuedFlows();
    this.cacheDir = new File(this.azkProps.getString("cache.directory", "cache"));
    // TODO extract QueueProcessor as a separate class, move all of this into it
    setupExecutotrComparatorWeightsMap();
    setupExecutorFilterList();
    this.queueProcessor = setupQueueProcessor();
  }

  @Override
  public void start() throws ExecutorManagerException {
    initialize();
    this.updaterThread.start();
    this.cleanerThread.start();
    this.queueProcessor.start();
  }

  private QueueProcessorThread setupQueueProcessor() {
    return new QueueProcessorThread(
        this.azkProps.getBoolean(Constants.ConfigurationKeys.QUEUEPROCESSING_ENABLED, true),
        this.azkProps.getLong(Constants.ConfigurationKeys.ACTIVE_EXECUTOR_REFRESH_IN_MS, 50000),
        this.azkProps.getInt(
            Constants.ConfigurationKeys.ACTIVE_EXECUTOR_REFRESH_IN_NUM_FLOW, 5),
        this.azkProps.getInt(
            Constants.ConfigurationKeys.MAX_DISPATCHING_ERRORS_PERMITTED,
            this.activeExecutors.getAll().size()),
        this.sleepAfterDispatchFailure);
  }

  private void setupExecutotrComparatorWeightsMap() {
    // initialize comparator feature weights for executor selector from azkaban.properties
    final Map<String, String> compListStrings = this.azkProps
        .getMapByPrefix(ConfigurationKeys.EXECUTOR_SELECTOR_COMPARATOR_PREFIX);
    if (compListStrings != null) {
      this.comparatorWeightsMap = new TreeMap<>();
      for (final Map.Entry<String, String> entry : compListStrings.entrySet()) {
        this.comparatorWeightsMap.put(entry.getKey(), Integer.valueOf(entry.getValue()));
      }
    }
  }

  private void setupExecutorFilterList() {
    // initialize hard filters for executor selector from azkaban.properties
    final String filters = this.azkProps
        .getString(ConfigurationKeys.EXECUTOR_SELECTOR_FILTERS, "");
    if (filters != null) {
      this.filterList = Arrays.asList(StringUtils.split(filters, ","));
    }
  }

  private ExecutorService createExecutorInfoRefresherService() {
    return Executors.newFixedThreadPool(this.azkProps.getInt(
        ConfigurationKeys.EXECUTORINFO_REFRESH_MAX_THREADS, 5));
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#setupExecutors()
   */
  @Override
  public void setupExecutors() throws ExecutorManagerException {
    checkMultiExecutorMode();
    this.activeExecutors.setupExecutors();
  }

  // TODO Enforced for now to ensure that users migrate to multi-executor mode acknowledgingly.
  // TODO Remove this once confident enough that all active users have already updated to some
  // version new enough to have this change - for example after 1 year has passed.
  // TODO Then also delete ConfigurationKeys.USE_MULTIPLE_EXECUTORS.
  @Deprecated
  private void checkMultiExecutorMode() {
    if (!this.azkProps.getBoolean(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, false)) {
      throw new IllegalArgumentException(
          Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS +
              " must be true. Single executor mode is not supported any more.");
    }
  }

  /**
   * Refresh Executor stats for all the actie executors in this executorManager
   */
  private void refreshExecutors() {

    final List<Pair<Executor, Future<ExecutorInfo>>> futures =
        new ArrayList<>();
    for (final Executor executor : this.activeExecutors.getAll()) {
      // execute each executorInfo refresh task to fetch
      final Future<ExecutorInfo> fetchExecutionInfo =
          this.executorInfoRefresherService.submit(
              () -> this.apiGateway.callForJsonType(executor.getHost(),
                  executor.getPort(), "/serverStatistics", null, ExecutorInfo.class));
      futures.add(new Pair<>(executor,
          fetchExecutionInfo));
    }

    boolean wasSuccess = true;
    for (final Pair<Executor, Future<ExecutorInfo>> refreshPair : futures) {
      final Executor executor = refreshPair.getFirst();
      executor.setExecutorInfo(null); // invalidate cached ExecutorInfo
      try {
        // max 5 secs
        final ExecutorInfo executorInfo = refreshPair.getSecond().get(5, TimeUnit.SECONDS);
        // executorInfo is null if the response was empty
        executor.setExecutorInfo(executorInfo);
        logger.info(String.format(
            "Successfully refreshed executor: %s with executor info : %s",
            executor, executorInfo));
      } catch (final TimeoutException e) {
        wasSuccess = false;
        logger.error("Timed out while waiting for ExecutorInfo refresh"
            + executor, e);
      } catch (final Exception e) {
        wasSuccess = false;
        logger.error("Failed to update ExecutorInfo for executor : "
            + executor, e);
      }

      // update is successful for all executors
      if (wasSuccess) {
        this.lastSuccessfulExecutorInfoRefresh = System.currentTimeMillis();
      }
    }
  }

  /**
   * @see azkaban.executor.ExecutorManagerAdapter#disableQueueProcessorThread()
   */
  @Override
  public void disableQueueProcessorThread() {
    this.queueProcessor.setActive(false);
  }

  /**
   * @see azkaban.executor.ExecutorManagerAdapter#enableQueueProcessorThread()
   */
  @Override
  public void enableQueueProcessorThread() {
    this.queueProcessor.setActive(true);
  }

  public State getQueueProcessorThreadState() {
    return this.queueProcessor.getState();
  }

  /**
   * Returns state of QueueProcessor False, no flow is being dispatched True , flows are being
   * dispatched as expected
   */
  public boolean isQueueProcessorThreadActive() {
    return this.queueProcessor.isActive();
  }

  /**
   * Return last Successful ExecutorInfo Refresh for all active executors
   */
  public long getLastSuccessfulExecutorInfoRefresh() {
    return this.lastSuccessfulExecutorInfoRefresh;
  }

  /**
   * Get currently supported Comparators available to use via azkaban.properties
   */
  public Set<String> getAvailableExecutorComparatorNames() {
    return ExecutorComparator.getAvailableComparatorNames();

  }

  /**
   * Get currently supported filters available to use via azkaban.properties
   */
  public Set<String> getAvailableExecutorFilterNames() {
    return ExecutorFilter.getAvailableFilterNames();
  }

  @Override
  public State getExecutorManagerThreadState() {
    return this.updaterThread.getState();
  }

  public String getExecutorThreadStage() {
    return this.updaterStage.get();
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    return this.updaterThread.isAlive();
  }

  @Override
  public long getLastExecutorManagerThreadCheckTime() {
    return this.updaterThread.getLastThreadCheckTime();
  }

  @Override
  public Collection<Executor> getAllActiveExecutors() {
    return Collections.unmodifiableCollection(this.activeExecutors.getAll());
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#fetchExecutor(int)
   */
  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    for (final Executor executor : this.activeExecutors.getAll()) {
      if (executor.getId() == executorId) {
        return executor;
      }
    }
    return this.executorLoader.fetchExecutor(executorId);
  }

  @Override
  public Set<String> getPrimaryServerHosts() {
    // Only one for now. More probably later.
    final HashSet<String> ports = new HashSet<>();
    for (final Executor executor : this.activeExecutors.getAll()) {
      ports.add(executor.getHost() + ":" + executor.getPort());
    }
    return ports;
  }

  @Override
  public Set<String> getAllActiveExecutorServerHosts() {
    // Includes non primary server/hosts
    final HashSet<String> ports = new HashSet<>();
    for (final Executor executor : this.activeExecutors.getAll()) {
      ports.add(executor.getHost() + ":" + executor.getPort());
    }
    // include executor which were initially active and still has flows running
    for (final Pair<ExecutionReference, ExecutableFlow> running : this.runningExecutions.get()
        .values()) {
      final ExecutionReference ref = running.getFirst();
      if (ref.getExecutor().isPresent()) {
        final Executor executor = ref.getExecutor().get();
        ports.add(executor.getHost() + ":" + executor.getPort());
      }
    }
    return ports;
  }

  private void loadRunningExecutions() throws ExecutorManagerException {
    logger.info("Loading running flows from database..");
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = this.executorLoader
        .fetchActiveFlows();
    logger.info("Loaded " + activeFlows.size() + " running flows");
    this.runningExecutions.get().putAll(activeFlows);
  }

  /*
   * load queued flows i.e with active_execution_reference and not assigned to
   * any executor
   */
  private void loadQueuedFlows() throws ExecutorManagerException {
    final List<Pair<ExecutionReference, ExecutableFlow>> retrievedExecutions =
        this.executorLoader.fetchQueuedFlows();
    if (retrievedExecutions != null) {
      for (final Pair<ExecutionReference, ExecutableFlow> pair : retrievedExecutions) {
        this.queuedFlows.enqueue(pair.getSecond(), pair.getFirst());
      }
    }
  }

  /**
   * Gets a list of all the active (running flows and non-dispatched flows) executions for a given
   * project and flow {@inheritDoc}. Results should be sorted as we assume this while setting up
   * pipelined execution Id.
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows(int, java.lang.String)
   */
  @Override
  public List<Integer> getRunningFlows(final int projectId, final String flowId) {
    final List<Integer> executionIds = new ArrayList<>();
    executionIds.addAll(getRunningFlowsHelper(projectId, flowId,
        this.queuedFlows.getAllEntries()));
    // it's possible an execution is runningCandidate, meaning it's in dispatching state neither in queuedFlows nor runningFlows,
    // so checks the runningCandidate as well.
    if (this.runningCandidate != null) {
      executionIds
          .addAll(
              getRunningFlowsHelper(projectId, flowId, Lists.newArrayList(this.runningCandidate)));
    }
    executionIds.addAll(getRunningFlowsHelper(projectId, flowId,
        this.runningExecutions.get().values()));
    Collections.sort(executionIds);
    return executionIds;
  }

  /* Helper method for getRunningFlows */
  private List<Integer> getRunningFlowsHelper(final int projectId, final String flowId,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    final List<Integer> executionIds = new ArrayList<>();
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      if (ref.getSecond().getFlowId().equals(flowId)
          && ref.getSecond().getProjectId() == projectId) {
        executionIds.add(ref.getFirst().getExecId());
      }
    }
    return executionIds;
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getActiveFlowsWithExecutor()
   */
  @Override
  public List<Pair<ExecutableFlow, Optional<Executor>>> getActiveFlowsWithExecutor()
      throws IOException {
    final List<Pair<ExecutableFlow, Optional<Executor>>> flows =
        new ArrayList<>();
    getActiveFlowsWithExecutorHelper(flows, this.queuedFlows.getAllEntries());
    getActiveFlowsWithExecutorHelper(flows, this.runningExecutions.get().values());
    return flows;
  }

  /* Helper method for getActiveFlowsWithExecutor */
  private void getActiveFlowsWithExecutorHelper(
      final List<Pair<ExecutableFlow, Optional<Executor>>> flows,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      flows.add(new Pair<>(ref.getSecond(), ref
          .getFirst().getExecutor()));
    }
  }

  /**
   * Checks whether the given flow has an active (running, non-dispatched) executions {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#isFlowRunning(int, java.lang.String)
   */
  @Override
  public boolean isFlowRunning(final int projectId, final String flowId) {
    boolean isRunning = false;
    isRunning =
        isRunning
            || isFlowRunningHelper(projectId, flowId, this.queuedFlows.getAllEntries());
    isRunning =
        isRunning
            || isFlowRunningHelper(projectId, flowId, this.runningExecutions.get().values());
    return isRunning;
  }

  /* Search a running flow in a collection */
  private boolean isFlowRunningHelper(final int projectId, final String flowId,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      if (ref.getSecond().getProjectId() == projectId
          && ref.getSecond().getFlowId().equals(flowId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Fetch ExecutableFlow from database {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getExecutableFlow(int)
   */
  @Override
  public ExecutableFlow getExecutableFlow(final int execId)
      throws ExecutorManagerException {
    return this.executorLoader.fetchExecutableFlow(execId);
  }

  /**
   * Get all active (running, non-dispatched) flows
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows()
   */
  @Override
  public List<ExecutableFlow> getRunningFlows() {
    final ArrayList<ExecutableFlow> flows = new ArrayList<>();
    getActiveFlowHelper(flows, this.queuedFlows.getAllEntries());
    getActiveFlowHelper(flows, this.runningExecutions.get().values());
    return flows;
  }

  /*
   * Helper method to get all running flows from a Pair<ExecutionReference,
   * ExecutableFlow collection
   */
  private void getActiveFlowHelper(final ArrayList<ExecutableFlow> flows,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      flows.add(ref.getSecond());
    }
  }

  /**
   * Get execution Ids of all running (unfinished) flows
   */
  public String getRunningFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    getRunningFlowsIdsHelper(allIds, this.queuedFlows.getAllEntries());
    getRunningFlowsIdsHelper(allIds, this.runningExecutions.get().values());
    Collections.sort(allIds);
    return allIds.toString();
  }

  /**
   * Get execution Ids of all non-dispatched flows
   */
  public String getQueuedFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    getRunningFlowsIdsHelper(allIds, this.queuedFlows.getAllEntries());
    Collections.sort(allIds);
    return allIds.toString();
  }

  /**
   * Get the number of non-dispatched flows. {@inheritDoc}
   */
  @Override
  public long getQueuedFlowSize() {
    return this.queuedFlows.size();
  }

  /* Helper method to flow ids of all running flows */
  private void getRunningFlowsIdsHelper(final List<Integer> allIds,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      allIds.add(ref.getSecond().getExecutionId());
    }
  }

  @Override
  public List<ExecutableFlow> getRecentlyFinishedFlows() {
    List<ExecutableFlow> flows = new ArrayList<>();
    try {
      flows = this.executorLoader.fetchRecentlyFinishedFlows(
          RECENTLY_FINISHED_LIFETIME);
    } catch (final ExecutorManagerException e) {
      //Todo jamiesjc: fix error handling.
      logger.error("Failed to fetch recently finished flows.", e);
    }
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final int skip, final int size)
      throws ExecutorManagerException {
    final List<ExecutableFlow> flows = this.executorLoader.fetchFlowHistory(skip, size);
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final String flowIdContains,
      final int skip, final int size) throws ExecutorManagerException {
    final List<ExecutableFlow> flows =
        this.executorLoader.fetchFlowHistory(null, '%' + flowIdContains + '%', null,
            0, -1, -1, skip, size);
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final String projContain,
      final String flowContain, final String userContain, final int status, final long begin,
      final long end,
      final int skip, final int size) throws ExecutorManagerException {
    final List<ExecutableFlow> flows =
        this.executorLoader.fetchFlowHistory(projContain, flowContain, userContain,
            status, begin, end, skip, size);
    return flows;
  }

  @Override
  public List<ExecutableJobInfo> getExecutableJobs(final Project project,
      final String jobId, final int skip, final int size) throws ExecutorManagerException {
    final List<ExecutableJobInfo> nodes =
        this.executorLoader.fetchJobHistory(project.getId(), jobId, skip, size);
    return nodes;
  }

  @Override
  public int getNumberOfJobExecutions(final Project project, final String jobId)
      throws ExecutorManagerException {
    return this.executorLoader.fetchNumExecutableNodes(project.getId(), jobId);
  }

  @Override
  public LogData getExecutableFlowLog(final ExecutableFlow exFlow, final int offset,
      final int length) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningExecutions.get().get(exFlow.getExecutionId());
    if (pair != null) {
      final Pair<String, String> typeParam = new Pair<>("type", "flow");
      final Pair<String, String> offsetParam =
          new Pair<>("offset", String.valueOf(offset));
      final Pair<String, String> lengthParam =
          new Pair<>("length", String.valueOf(length));

      @SuppressWarnings("unchecked") final Map<String, Object> result =
          this.apiGateway.callWithReference(pair.getFirst(), ConnectorParams.LOG_ACTION,
              typeParam, offsetParam, lengthParam);
      return LogData.createLogDataFromObject(result);
    } else {
      final LogData value =
          this.executorLoader.fetchLogs(exFlow.getExecutionId(), "", 0, offset,
              length);
      return value;
    }
  }

  @Override
  public LogData getExecutionJobLog(final ExecutableFlow exFlow, final String jobId,
      final int offset, final int length, final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningExecutions.get().get(exFlow.getExecutionId());
    if (pair != null) {
      final Pair<String, String> typeParam = new Pair<>("type", "job");
      final Pair<String, String> jobIdParam =
          new Pair<>("jobId", jobId);
      final Pair<String, String> offsetParam =
          new Pair<>("offset", String.valueOf(offset));
      final Pair<String, String> lengthParam =
          new Pair<>("length", String.valueOf(length));
      final Pair<String, String> attemptParam =
          new Pair<>("attempt", String.valueOf(attempt));

      @SuppressWarnings("unchecked") final Map<String, Object> result =
          this.apiGateway.callWithReference(pair.getFirst(), ConnectorParams.LOG_ACTION,
              typeParam, jobIdParam, offsetParam, lengthParam, attemptParam);
      return LogData.createLogDataFromObject(result);
    } else {
      final LogData value =
          this.executorLoader.fetchLogs(exFlow.getExecutionId(), jobId, attempt,
              offset, length);
      return value;
    }
  }

  @Override
  public List<Object> getExecutionJobStats(final ExecutableFlow exFlow, final String jobId,
      final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningExecutions.get().get(exFlow.getExecutionId());
    if (pair == null) {
      return this.executorLoader.fetchAttachments(exFlow.getExecutionId(), jobId,
          attempt);
    }

    final Pair<String, String> jobIdParam = new Pair<>("jobId", jobId);
    final Pair<String, String> attemptParam =
        new Pair<>("attempt", String.valueOf(attempt));

    @SuppressWarnings("unchecked") final Map<String, Object> result =
        this.apiGateway.callWithReference(pair.getFirst(), ConnectorParams.ATTACHMENTS_ACTION,
            jobIdParam, attemptParam);

    @SuppressWarnings("unchecked") final List<Object> jobStats = (List<Object>) result
        .get("attachments");

    return jobStats;
  }

  /**
   * If the Resource Manager and Job History server urls are configured, find all the
   * Hadoop/Spark application ids present in the Azkaban job's log and then construct the url to
   * job logs in the Hadoop/Spark server for each application id found. Application ids are
   * returned in the order they appear in the Azkaban job log.
   *
   * @param exFlow The executable flow.
   * @param jobId The job id.
   * @param attempt The job execution attempt.
   * @return The map of (application id, job log url)
   */
  @Override
  public Map<String, String> getExternalJobLogUrls(final ExecutableFlow exFlow, final String jobId,
      final int attempt) {

    final Map<String, String> jobLogUrlsByAppId = new LinkedHashMap<>();
    if (!this.azkProps.containsKey(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL) ||
        !this.azkProps.containsKey(ConfigurationKeys.HISTORY_SERVER_JOB_URL) ||
        !this.azkProps.containsKey(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL)) {
      return jobLogUrlsByAppId;
    }
    final Set<String> applicationIds = getApplicationIds(exFlow, jobId, attempt);
    for (final String applicationId : applicationIds) {
      final String jobLogUrl = ExecutionControllerUtils
          .createJobLinkUrl(exFlow, jobId, applicationId, this.azkProps);
      if (jobLogUrl != null) {
        jobLogUrlsByAppId.put(applicationId, jobLogUrl);
      }
    }

    return jobLogUrlsByAppId;
  }

  /**
   * Find all the Hadoop/Spark application ids present in the Azkaban job log. When iterating
   * over the set returned by this method the application ids are in the same order they appear
   * in the log.
   *
   * @param exFlow The executable flow.
   * @param jobId The job id.
   * @param attempt The job execution attempt.
   * @return The application ids found.
   */
  Set<String> getApplicationIds(final ExecutableFlow exFlow, final String jobId,
      final int attempt) {
    final Set<String> applicationIds = new LinkedHashSet<>();
    int offset = 0;
    try {
      LogData data = getExecutionJobLog(exFlow, jobId, offset, 50000, attempt);
      while (data != null && data.getLength() > 0) {
        this.logger.info("Get application ID for execution " + exFlow.getExecutionId() + ", job"
            + " " + jobId + ", attempt " + attempt + ", data offset " + offset);
        String logData = data.getData();
        final int indexOfLastSpace = logData.lastIndexOf(' ');
        final int indexOfLastTab = logData.lastIndexOf('\t');
        final int indexOfLastEoL = logData.lastIndexOf('\n');
        final int indexOfLastDelim = Math
            .max(indexOfLastEoL, Math.max(indexOfLastSpace, indexOfLastTab));
        if (indexOfLastDelim > -1) {
          // index + 1 to avoid looping forever if indexOfLastDelim is zero
          logData = logData.substring(0, indexOfLastDelim + 1);
        }
        applicationIds.addAll(ExecutionControllerUtils.findApplicationIdsFromLog(logData));
        offset = data.getOffset() + logData.length();
        data = getExecutionJobLog(exFlow, jobId, offset, 50000, attempt);
      }
    } catch (final ExecutorManagerException e) {
      this.logger.error("Failed to get application ID for execution " + exFlow.getExecutionId() +
          ", job " + jobId + ", attempt " + attempt + ", data offset " + offset, e);
    }
    return applicationIds;
  }

  /**
   * if flows was dispatched to an executor, cancel by calling Executor else if flow is still in
   * queue, remove from queue and finalize {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#cancelFlow(azkaban.executor.ExecutableFlow,
   * java.lang.String)
   */
  @Override
  public void cancelFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      if (this.runningExecutions.get().containsKey(exFlow.getExecutionId())) {
        final Pair<ExecutionReference, ExecutableFlow> pair =
            this.runningExecutions.get().get(exFlow.getExecutionId());
        this.apiGateway.callWithReferenceByUser(pair.getFirst(), ConnectorParams.CANCEL_ACTION,
            userId);
      } else if (this.queuedFlows.hasExecution(exFlow.getExecutionId())) {
        this.queuedFlows.dequeue(exFlow.getExecutionId());
        this.executionFinalizer
            .finalizeFlow(exFlow, "Cancelled before dispatching to executor", null);
      } else {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
    }
  }

  @Override
  public void resumeFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.runningExecutions.get().get(exFlow.getExecutionId());
      if (pair == null) {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
      this.apiGateway
          .callWithReferenceByUser(pair.getFirst(), ConnectorParams.RESUME_ACTION, userId);
    }
  }

  @Override
  public void pauseFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.runningExecutions.get().get(exFlow.getExecutionId());
      if (pair == null) {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
      this.apiGateway
          .callWithReferenceByUser(pair.getFirst(), ConnectorParams.PAUSE_ACTION, userId);
    }
  }

  @Override
  public void retryFailures(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_FAILURES, userId);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> modifyExecutingJobs(final ExecutableFlow exFlow,
      final String command, final String userId, final String... jobIds)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.runningExecutions.get().get(exFlow.getExecutionId());
      if (pair == null) {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }

      final Map<String, Object> response;
      if (jobIds != null && jobIds.length > 0) {
        for (final String jobId : jobIds) {
          if (!jobId.isEmpty()) {
            final ExecutableNode node = exFlow.getExecutableNode(jobId);
            if (node == null) {
              throw new ExecutorManagerException("Job " + jobId
                  + " doesn't exist in execution " + exFlow.getExecutionId()
                  + ".");
            }
          }
        }
        final String ids = StringUtils.join(jobIds, ',');
        response =
            this.apiGateway.callWithReferenceByUser(pair.getFirst(),
                ConnectorParams.MODIFY_EXECUTION_ACTION, userId,
                new Pair<>(
                    ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE, command),
                new Pair<>(ConnectorParams.MODIFY_JOBS_LIST, ids));
      } else {
        response =
            this.apiGateway.callWithReferenceByUser(pair.getFirst(),
                ConnectorParams.MODIFY_EXECUTION_ACTION, userId,
                new Pair<>(
                    ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE, command));
      }

      return response;
    }
  }

  @Override
  public String submitExecutableFlow(final ExecutableFlow exflow, final String userId)
      throws ExecutorManagerException {
    if (exflow.isLocked()) {
      // Skip execution for locked flows.
      final String message = String.format("Flow %s for project %s is locked.", exflow.getId(),
          exflow.getProjectName());
      logger.info(message);
      return message;
    }

    final String exFlowKey = exflow.getProjectName() + "." + exflow.getId() + ".submitFlow";
    // using project and flow name to prevent race condition when same flow is submitted by API and schedule at the same time
    // causing two same flow submission entering this piece.
    synchronized (exFlowKey.intern()) {
      final String flowId = exflow.getFlowId();

      logger.info("Submitting execution flow " + flowId + " by " + userId);

      String message = "";
      //TODO: JANKI: Remove queuing for execution containerization
      if (this.queuedFlows.isFull()) {
        message =
            String
                .format(
                    "Failed to submit %s for project %s. Azkaban has overrun its webserver queue capacity",
                    flowId, exflow.getProjectName());
        logger.error(message);
        this.commonMetrics.markSubmitFlowFail();
      } else {
        final int projectId = exflow.getProjectId();
        exflow.setSubmitUser(userId);
        exflow.setStatus(Status.PREPARING);
        exflow.setSubmitTime(System.currentTimeMillis());

        // Get collection of running flows given a project and a specific flow name
        final List<Integer> running = getRunningFlows(projectId, flowId);

        ExecutionOptions options = exflow.getExecutionOptions();
        if (options == null) {
          options = new ExecutionOptions();
        }

        if (options.getDisabledJobs() != null) {
          FlowUtils.applyDisabledJobs(options.getDisabledJobs(), exflow);
        }
        //TODO: JANKI: To reduce the possible attack on yarn, it will be good to keep max concurrent runs for a flow check
        if (!running.isEmpty()) {
          final int maxConcurrentRuns = ExecutorUtils.getMaxConcurrentRunsForFlow(
              exflow.getProjectName(), flowId, this.maxConcurrentRunsOneFlow,
              this.maxConcurrentRunsPerFlowMap);
          if (running.size() > maxConcurrentRuns) {
            this.commonMetrics.markSubmitFlowSkip();
            throw new ExecutorManagerException("Flow " + flowId
                + " has more than " + maxConcurrentRuns + " concurrent runs. Skipping",
                ExecutorManagerException.Reason.SkippedExecution);
          } else if (options.getConcurrentOption().equals(
              ExecutionOptions.CONCURRENT_OPTION_PIPELINE)) {
            Collections.sort(running);
            final Integer runningExecId = running.get(running.size() - 1);

            options.setPipelineExecutionId(runningExecId);
            message =
                "Flow " + flowId + " is already running with exec id "
                    + runningExecId + ". Pipelining level "
                    + options.getPipelineLevel() + ". \n";
          } else if (options.getConcurrentOption().equals(
              ExecutionOptions.CONCURRENT_OPTION_SKIP)) {
            this.commonMetrics.markSubmitFlowSkip();
            throw new ExecutorManagerException("Flow " + flowId
                + " is already running. Skipping execution.",
                ExecutorManagerException.Reason.SkippedExecution);
          } else {
            // The settings is to run anyways.
            message =
                "Flow " + flowId + " is already running with exec id "
                    + StringUtils.join(running, ",")
                    + ". Will execute concurrently. \n";
          }
        }

        final boolean memoryCheck =
            !ProjectWhitelist.isProjectWhitelisted(exflow.getProjectId(),
                ProjectWhitelist.WhitelistType.MemoryCheck);
        options.setMemoryCheck(memoryCheck);

        // The exflow id is set by the loader. So it's unavailable until after
        // this call.
        this.executorLoader.uploadExecutableFlow(exflow);

        // We create an active flow reference in the datastore. If the upload
        // fails, we remove the reference.
        final ExecutionReference reference =
            new ExecutionReference(exflow.getExecutionId());

        this.executorLoader.addActiveExecutableReference(reference);
        this.queuedFlows.enqueue(exflow, reference);
        message += "Execution queued successfully with exec id " + exflow.getExecutionId();
        this.commonMetrics.markSubmitFlowSuccess();
      }
      return message;
    }
  }

  @Override
  public Map<String, String> doRampActions(List<Map<String, Object>> rampActions)
      throws ExecutorManagerException {
    return this.executorLoader.doRampActions(rampActions);
  }

  private void cleanOldExecutionLogs(final long millis) {
    final long beforeDeleteLogsTimestamp = System.currentTimeMillis();
    try {
      final int count = this.executorLoader.removeExecutionLogsByTime(millis);
      logger.info("Cleaned up " + count + " log entries.");
    } catch (final ExecutorManagerException e) {
      logger.error("log clean up failed. ", e);
    }
    logger.info(
        "log clean up time: " + (System.currentTimeMillis() - beforeDeleteLogsTimestamp) / 1000
            + " seconds.");
  }

  /**
   * Manage servlet call for stats servlet in Azkaban execution server {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#callExecutorStats(int, java.lang.String,
   * azkaban.utils.Pair[])
   */
  @Override
  public Map<String, Object> callExecutorStats(final int executorId, final String action,
      final Pair<String, String>... params) throws IOException, ExecutorManagerException {
    final Executor executor = fetchExecutor(executorId);

    final List<Pair<String, String>> paramList =
        new ArrayList<>();

    // if params = null
    if (params != null) {
      paramList.addAll(Arrays.asList(params));
    }

    paramList
        .add(new Pair<>(ConnectorParams.ACTION_PARAM, action));

    return this.apiGateway.callForJsonObjectMap(executor.getHost(), executor.getPort(),
        "/stats", paramList);
  }

  @Override
  public Map<String, Object> callExecutorJMX(final String hostPort, final String action,
      final String mBean) throws IOException {
    final List<Pair<String, String>> paramList =
        new ArrayList<>();

    paramList.add(new Pair<>(action, ""));
    if (mBean != null) {
      paramList.add(new Pair<>(ConnectorParams.JMX_MBEAN, mBean));
    }

    final String[] hostPortSplit = hostPort.split(":");
    return this.apiGateway.callForJsonObjectMap(hostPortSplit[0],
        Integer.valueOf(hostPortSplit[1]), "/jmx", paramList);
  }

  @Override
  public void shutdown() {
    if(null != this.queueProcessor) {
      this.queueProcessor.shutdown();
    }
    if(null != this.updaterThread) {
      this.updaterThread.shutdown();
    }
  }

  @Override
  public int getExecutableFlows(final int projectId, final String flowId, final int from,
      final int length, final List<ExecutableFlow> outputList)
      throws ExecutorManagerException {
    final List<ExecutableFlow> flows =
        this.executorLoader.fetchFlowHistory(projectId, flowId, from, length);
    outputList.addAll(flows);
    return this.executorLoader.fetchNumExecutableFlows(projectId, flowId);
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final int projectId, final String flowId,
      final int from, final int length, final Status status) throws ExecutorManagerException {
    return this.executorLoader.fetchFlowHistory(projectId, flowId, from, length,
        status);
  }

  /**
   * Dispatch the flow in a container and update in-memory state of executableFlow.
   */
  private void dispatchContainer(final ExecutionReference reference, final ExecutableFlow exflow) throws ExecutorManagerException {
    exflow.setUpdateTime(System.currentTimeMillis());
    logger.info("Creating Tonyclient");
    Configuration conf = new Configuration();
    //conf.addResource("/export/apps/azkaban/azkaban-web-server/current/conf/tony-default.xml");
    TonyClient client = new TonyClient(conf);
    try {
      ClusterSubmitter submitter = new ClusterSubmitter(client);
      int tonyPSInstances = this.azkProps.getInt(FlowContainerization.TONY_PS_INSTANCES, 0);
      String tonyWorkerMemory = this.azkProps.getString(FlowContainerization.TONY_WORKER_MEMORY, "20G");
      int tonyPSWorker = this.azkProps.getInt(FlowContainerization.TONY_PS_WORKER, 1);
      int tonyWorkerInstances = this.azkProps.getInt(FlowContainerization.TONY_WORKER_INSTANCES, 1);
      String tonyWorkerResources = this.azkProps.getString(FlowContainerization.TONY_WORKER_RESOURCES, "/export/home/azkaban/test-dispatch/basic_flow.zip,/export/home/azkaban/test-dispatch/jobtypes.zip,/export/home/azkaban/test-dispatch/lib.zip#archive,/export/home/azkaban/test-dispatch/hive.zip#archive,/export/home/azkaban/test-dispatch/conf.zip#archive");
      String tonyContainerResources = this.azkProps.getString(FlowContainerization.TONY_CONTAINERS_RESOURCES, "/export/home/azkaban/test-dispatch/tony-core-0.3.23.jar,/export/home/azkaban/test-dispatch/tony-cli-0.3.23.jar,/export/home/azkaban/test-dispatch/tony-mini-0.3.23.jar,/export/home/azkaban/test-dispatch/tony-proxy-0.3.23.jar,/export/home/azkaban/test-dispatch/zip4j-1.3.2.jar,/export/home/azkaban/test-dispatch/jackson-core-2.8.11.jar,/export/home/azkaban/test-dispatch/jackson-databind-2.8.11.jar,/export/home/azkaban/test-dispatch/jackson-annotations-2.8.5.jar,/export/home/azkaban/test-dispatch/lib.zip#archive");
      int exitCode = submitter.submit(new String[] {FlowContainerization.TONY_CONF,
          String.join("=", FlowContainerization.TONY_WORKER_RESOURCES, tonyWorkerResources),
          FlowContainerization.TONY_CONF, String.join("=",FlowContainerization.TONY_CONTAINERS_RESOURCES, tonyContainerResources),
          FlowContainerization.TONY_EXECUTES, "java -Dlog4j.configuration=file:./conf.zip/log4j.properties -cp ./lib.zip/*:/export/apps/hadoop/site/etc/hadoop:/export/apps/hadoop/latest/share/hadoop/common/lib/*:/export/apps/hadoop/latest/share/hadoop/common/*:/export/apps/hadoop/latest/share/hadoop/hdfs:/export/apps/hadoop/latest/share/hadoop/hdfs/lib/*:/export/apps/hadoop/latest/share/hadoop/hdfs/*:/export/apps/hadoop/latest/share/hadoop/yarn/lib/*:/export/apps/hadoop/latest/share/hadoop/yarn/*:/export/apps/hadoop/latest/share/hadoop/mapreduce/lib/*:/export/apps/hadoop/latest/share/hadoop/mapreduce/*:/export/apps/hadoop/site/lib/amf-sink-1.2.5-all.jar:/export/apps/hadoop/site/lib/grid-topology-1.0.jar:/export/apps/hadoop/site/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar:./hive.zip/conf:./hive.zip/lib/* azkaban.container.FlowContainer basic_flow.zip basic_flow jobTypeDir=jobtypes.zip lib.zip",
          FlowContainerization.TONY_SHELL_ENV, "CLASSPATH=$(${HADOOP_HDFS_HOME}/bin/hadoop classpath --glob)",
          FlowContainerization.TONY_SHELL_ENV, "LIBHDFS_OPTS=‘-Xmx 2g’",
          FlowContainerization.TONY_CONF, String.join("=",FlowContainerization.TONY_PS_INSTANCES, String.valueOf(tonyPSInstances)),
          FlowContainerization.TONY_CONF, String.join("=", FlowContainerization.TONY_WORKER_MEMORY, tonyWorkerMemory),
          FlowContainerization.TONY_CONF, String.join("=", FlowContainerization.TONY_PS_WORKER, String.valueOf(tonyPSWorker)),
          FlowContainerization.TONY_CONF, String.join("=", FlowContainerization.TONY_WORKER_INSTANCES, String.valueOf(tonyWorkerInstances))});

      logger.info("Tonyclient exitcode: " + exitCode);
      if (exitCode != 0) {
        logger.error("Could not dispatch the container for execution id:" + exflow.getExecutionId() + " , exit code: "
            + exitCode);
        throw new ExecutorManagerException("Could not launch the container. Exit code" + exitCode);
      }
    } catch (Exception ex) {
      logger.error("Could not dispatch the container for execution id:" + exflow.getExecutionId(), ex);
      throw new ExecutorManagerException("Could not launch the container. ", ex);
    }
    // move from flow to running flows
    this.runningExecutions.get().put(exflow.getExecutionId(), new Pair<>(reference, exflow));
    synchronized (this.runningExecutions.get()) {
      // Wake up RunningExecutionsUpdaterThread from wait() so that it will immediately check status
      // from executor(s). Normally flows will run at least some time and can't be cleaned up
      // immediately, so there will be another wait round (or many, actually), but for unit tests
      // this is significant to let them run quickly.
      this.runningExecutions.get().notifyAll();
    }
    synchronized (this) {
      // wake up all internal waiting threads, too
      this.notifyAll();
    }

    logger.info(String.format("Successfully dispatched exec %d with error count %d", exflow.getExecutionId(),
        reference.getNumErrors()));
  }
  /**
   * Calls executor to dispatch the flow, update db to assign the executor and in-memory state of
   * executableFlow.
   */
  private void dispatch(final ExecutionReference reference, final ExecutableFlow exflow,
      final Executor choosenExecutor) throws ExecutorManagerException {
    exflow.setUpdateTime(System.currentTimeMillis());

    this.executorLoader.assignExecutor(choosenExecutor.getId(),
        exflow.getExecutionId());
    try {
      this.apiGateway.callWithExecutable(exflow, choosenExecutor,
          ConnectorParams.EXECUTE_ACTION);
    } catch (final ExecutorManagerException ex) {
      logger.error("Rolling back executor assignment for execution id:"
          + exflow.getExecutionId(), ex);
      this.executorLoader.unassignExecutor(exflow.getExecutionId());
      throw new ExecutorManagerException(ex);
    }
    reference.setExecutor(choosenExecutor);

    // move from flow to running flows
    this.runningExecutions.get().put(exflow.getExecutionId(), new Pair<>(reference, exflow));
    synchronized (this.runningExecutions.get()) {
      // Wake up RunningExecutionsUpdaterThread from wait() so that it will immediately check status
      // from executor(s). Normally flows will run at least some time and can't be cleaned up
      // immediately, so there will be another wait round (or many, actually), but for unit tests
      // this is significant to let them run quickly.
      this.runningExecutions.get().notifyAll();
    }
    synchronized (this) {
      // wake up all internal waiting threads, too
      this.notifyAll();
    }

    logger.info(String.format(
        "Successfully dispatched exec %d with error count %d",
        exflow.getExecutionId(), reference.getNumErrors()));
  }

  @VisibleForTesting
  void setSleepAfterDispatchFailure(final Duration sleepAfterDispatchFailure) {
    this.sleepAfterDispatchFailure = sleepAfterDispatchFailure;
  }

  /*
   * cleaner thread to clean up execution_logs, etc in DB. Runs every hour.
   */
  private class CleanerThread extends Thread {
    // log file retention is 1 month.

    // check every hour
    private static final long CLEANER_THREAD_WAIT_INTERVAL_MS = 60 * 60 * 1000;

    private final long executionLogsRetentionMs;

    private boolean shutdown = false;
    private long lastLogCleanTime = -1;

    public CleanerThread(final long executionLogsRetentionMs) {
      this.executionLogsRetentionMs = executionLogsRetentionMs;
      this.setName("AzkabanWebServer-Cleaner-Thread");
    }

    @SuppressWarnings("unused")
    public void shutdown() {
      this.shutdown = true;
      this.interrupt();
    }

    @Override
    public void run() {
      while (!this.shutdown) {
        synchronized (this) {
          try {
            // Cleanup old stuff.
            final long currentTime = System.currentTimeMillis();
            if (currentTime - CLEANER_THREAD_WAIT_INTERVAL_MS > this.lastLogCleanTime) {
              cleanExecutionLogs();
              this.lastLogCleanTime = currentTime;
            }

            wait(CLEANER_THREAD_WAIT_INTERVAL_MS);
          } catch (final InterruptedException e) {
            ExecutorManager.logger.info("Interrupted. Probably to shut down.");
          }
        }
      }
    }

    private void cleanExecutionLogs() {
      ExecutorManager.logger.info("Cleaning old logs from execution_logs");
      final long cutoff = System.currentTimeMillis() - this.executionLogsRetentionMs;
      ExecutorManager.logger.info("Cleaning old log files before "
          + new DateTime(cutoff).toString());
      cleanOldExecutionLogs(System.currentTimeMillis()
          - this.executionLogsRetentionMs);
    }
  }

  /*
   * This thread is responsible for processing queued flows using dispatcher and
   * making rest api calls to executor server
   */
  private class QueueProcessorThread extends Thread {

    private static final long QUEUE_PROCESSOR_WAIT_IN_MS = 1000;
    private final int maxDispatchingErrors;
    private final long activeExecutorRefreshWindowInMillisec;
    private final int activeExecutorRefreshWindowInFlows;
    private final Duration sleepAfterDispatchFailure;

    private volatile boolean shutdown = false;
    private volatile boolean isActive = true;

    public QueueProcessorThread(final boolean isActive,
        final long activeExecutorRefreshWindowInTime,
        final int activeExecutorRefreshWindowInFlows,
        final int maxDispatchingErrors,
        final Duration sleepAfterDispatchFailure) {
      setActive(isActive);
      this.maxDispatchingErrors = maxDispatchingErrors;
      this.activeExecutorRefreshWindowInFlows =
          activeExecutorRefreshWindowInFlows;
      this.activeExecutorRefreshWindowInMillisec =
          activeExecutorRefreshWindowInTime;
      this.sleepAfterDispatchFailure = sleepAfterDispatchFailure;
      this.setName("AzkabanWebServer-QueueProcessor-Thread");
    }

    public boolean isActive() {
      return this.isActive;
    }

    public void setActive(final boolean isActive) {
      this.isActive = isActive;
      ExecutorManager.logger.info("QueueProcessorThread active turned " + this.isActive);
    }

    public void shutdown() {
      this.shutdown = true;
      this.interrupt();
    }

    @Override
    public void run() {
      // Loops till QueueProcessorThread is shutdown
      while (!this.shutdown) {
        synchronized (this) {
          try {
            // start processing queue if active, other wait for sometime
            if (this.isActive) {
              processQueuedFlows(this.activeExecutorRefreshWindowInMillisec,
                  this.activeExecutorRefreshWindowInFlows);
            }
            wait(QUEUE_PROCESSOR_WAIT_IN_MS);
          } catch (final Exception e) {
            ExecutorManager.logger.error(
                "QueueProcessorThread Interrupted. Probably to shut down.", e);
          }
        }
      }
    }

    /* Method responsible for processing the non-dispatched flows */
    private void processQueuedFlows(final long activeExecutorsRefreshWindow,
        final int maxContinuousFlowProcessed) throws InterruptedException,
        ExecutorManagerException {
      long lastExecutorRefreshTime = 0;
      int currentContinuousFlowProcessed = 0;

      while (isActive() && (ExecutorManager.this.runningCandidate = ExecutorManager.this.queuedFlows
          .fetchHead()) != null) {
        final ExecutionReference reference = ExecutorManager.this.runningCandidate.getFirst();
        final ExecutableFlow exflow = ExecutorManager.this.runningCandidate.getSecond();
        final long currentTime = System.currentTimeMillis();

        // Launch a container for flow execution
        //TODO: Add flow in a queue in case of failure
        exflow.setUpdateTime(currentTime);
        dispatchFlowInContainer(reference, exflow);
        ExecutorManager.this.runningCandidate = null;
      }
    }

    private void dispatchFlowInContainer(final ExecutionReference reference, final ExecutableFlow exflow) {
      Throwable lastError;
      synchronized (exflow) {
        do {
          try {
            dispatchContainer(reference, exflow);
            ExecutorManager.this.commonMetrics.markDispatchSuccess();
            // SUCCESS - exit
            return;
          } catch (final ExecutorManagerException e) {
            lastError = e;
            //logFailedDispatchAttempt(reference, exflow, selectedExecutor, e);
            ExecutorManager.this.commonMetrics.markDispatchFail();
            reference.setNumErrors(reference.getNumErrors() + 1);
          }
        } while (reference.getNumErrors() < this.maxDispatchingErrors);
        // GAVE UP DISPATCHING
        final String message = "Failed to dispatch queued execution " + exflow.getId() + " because "
            + "reached " + ConfigurationKeys.MAX_DISPATCHING_ERRORS_PERMITTED;
        ExecutorManager.logger.error(message);
        ExecutorManager.this.executionFinalizer.finalizeFlow(exflow, message, lastError);
      }
    }

    /* process flow with a snapshot of available Executors */
    private void selectExecutorAndDispatchFlow(final ExecutionReference reference,
        final ExecutableFlow exflow)
        throws ExecutorManagerException {
      final Set<Executor> remainingExecutors = new HashSet<>(
          ExecutorManager.this.activeExecutors.getAll());
      Throwable lastError;
      synchronized (exflow) {
        do {
          final Executor selectedExecutor = selectExecutor(exflow, remainingExecutors);
          if (selectedExecutor == null) {
            ExecutorManager.this.commonMetrics.markDispatchFail();
            handleNoExecutorSelectedCase(reference, exflow);
            // RE-QUEUED - exit
            return;
          } else {
            try {
              dispatch(reference, exflow, selectedExecutor);
              ExecutorManager.this.commonMetrics.markDispatchSuccess();
              // SUCCESS - exit
              return;
            } catch (final ExecutorManagerException e) {
              lastError = e;
              logFailedDispatchAttempt(reference, exflow, selectedExecutor, e);
              ExecutorManager.this.commonMetrics.markDispatchFail();
              reference.setNumErrors(reference.getNumErrors() + 1);
              // FAILED ATTEMPT - try other executors except selectedExecutor
              updateRemainingExecutorsAndSleep(remainingExecutors, selectedExecutor);
            }
          }
        } while (reference.getNumErrors() < this.maxDispatchingErrors);
        // GAVE UP DISPATCHING
        final String message = "Failed to dispatch queued execution " + exflow.getId() + " because "
            + "reached " + ConfigurationKeys.MAX_DISPATCHING_ERRORS_PERMITTED
            + " (tried " + reference.getNumErrors() + " executors)";
        ExecutorManager.logger.error(message);
        ExecutorManager.this.executionFinalizer.finalizeFlow(exflow, message, lastError);
      }
    }

    private void updateRemainingExecutorsAndSleep(final Set<Executor> remainingExecutors,
        final Executor selectedExecutor) {
      remainingExecutors.remove(selectedExecutor);
      if (remainingExecutors.isEmpty()) {
        remainingExecutors.addAll(ExecutorManager.this.activeExecutors.getAll());
        sleepAfterDispatchFailure();
      }
    }

    private void sleepAfterDispatchFailure() {
      try {
        Thread.sleep(this.sleepAfterDispatchFailure.toMillis());
      } catch (final InterruptedException e1) {
        ExecutorManager.logger.warn("Sleep after dispatch failure was interrupted - ignoring");
      }
    }

    private void logFailedDispatchAttempt(final ExecutionReference reference,
        final ExecutableFlow exflow,
        final Executor selectedExecutor, final ExecutorManagerException e) {
      ExecutorManager.logger.warn(String.format(
          "Executor %s responded with exception for exec: %d",
          selectedExecutor, exflow.getExecutionId()), e);
      ExecutorManager.logger.info(String.format(
          "Failed dispatch attempt for exec %d with error count %d",
          exflow.getExecutionId(), reference.getNumErrors()));
    }

    /* Helper method to fetch  overriding Executor, if a valid user has specifed otherwise return null */
    private Executor getUserSpecifiedExecutor(final ExecutionOptions options,
        final int executionId) {
      Executor executor = null;
      if (options != null
          && options.getFlowParameters() != null
          && options.getFlowParameters().containsKey(
          ExecutionOptions.USE_EXECUTOR)) {
        try {
          final int executorId =
              Integer.valueOf(options.getFlowParameters().get(
                  ExecutionOptions.USE_EXECUTOR));
          executor = fetchExecutor(executorId);

          if (executor == null) {
            ExecutorManager.logger
                .warn(String
                    .format(
                        "User specified executor id: %d for execution id: %d is not active, Looking up db.",
                        executorId, executionId));
            executor = ExecutorManager.this.executorLoader.fetchExecutor(executorId);
            if (executor == null) {
              ExecutorManager.logger
                  .warn(String
                      .format(
                          "User specified executor id: %d for execution id: %d is missing from db. Defaulting to availableExecutors",
                          executorId, executionId));
            }
          }
        } catch (final ExecutorManagerException ex) {
          ExecutorManager.logger.error("Failed to fetch user specified executor for exec_id = "
              + executionId, ex);
        }
      }
      return executor;
    }

    /* Choose Executor for exflow among the available executors */
    private Executor selectExecutor(final ExecutableFlow exflow,
        final Set<Executor> availableExecutors) {
      Executor choosenExecutor =
          getUserSpecifiedExecutor(exflow.getExecutionOptions(),
              exflow.getExecutionId());

      // If no executor was specified by admin
      if (choosenExecutor == null) {
        ExecutorManager.logger.info("Using dispatcher for execution id :"
            + exflow.getExecutionId());
        final ExecutorSelector selector = new ExecutorSelector(ExecutorManager.this.filterList,
            ExecutorManager.this.comparatorWeightsMap);
        choosenExecutor = selector.getBest(availableExecutors, exflow);
      }
      return choosenExecutor;
    }

    private void handleNoExecutorSelectedCase(final ExecutionReference reference,
        final ExecutableFlow exflow) throws ExecutorManagerException {
      ExecutorManager.logger
          .info(String
              .format(
                  "Reached handleNoExecutorSelectedCase stage for exec %d with error count %d",
                  exflow.getExecutionId(), reference.getNumErrors()));
      // TODO: handle scenario where a high priority flow failing to get
      // schedule can starve all others
      ExecutorManager.this.queuedFlows.enqueue(exflow, reference);
    }
  }
}
