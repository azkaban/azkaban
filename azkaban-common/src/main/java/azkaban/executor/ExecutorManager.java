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

import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.alert.Alerter;
import azkaban.event.EventHandler;
import azkaban.executor.selector.ExecutorComparator;
import azkaban.executor.selector.ExecutorFilter;
import azkaban.executor.selector.ExecutorSelector;
import azkaban.flow.FlowUtils;
import azkaban.metrics.CommonMetrics;
import azkaban.project.Project;
import azkaban.project.ProjectWhitelist;
import azkaban.utils.AuthenticationUtils;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Thread.State;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * Executor manager used to manage the client side job.
 */
@Singleton
public class ExecutorManager extends EventHandler implements
    ExecutorManagerAdapter {

  private static final String SPARK_JOB_TYPE = "spark";
  private static final String APPLICATION_ID = "${application.id}";
  // The regex to look for while fetching application ID from the Hadoop/Spark job log
  private static final Pattern APPLICATION_ID_PATTERN = Pattern
      .compile("application_\\d+_\\d+");
  // The regex to look for while validating the content from RM job link
  private static final Pattern FAILED_TO_READ_APPLICATION_PATTERN = Pattern
      .compile("Failed to read the application");
  private static final Pattern INVALID_APPLICATION_ID_PATTERN = Pattern
      .compile("Invalid Application ID");
  private static final int DEFAULT_MAX_ONCURRENT_RUNS_ONEFLOW = 30;
  // 12 weeks
  private static final long DEFAULT_EXECUTION_LOGS_RETENTION_MS = 3 * 4 * 7
      * 24 * 60 * 60 * 1000L;
  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(10);
  private static final Logger logger = Logger.getLogger(ExecutorManager.class);
  final ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> runningFlows =
      new ConcurrentHashMap<>();
  private final AlerterHolder alerterHolder;
  private final Props azkProps;
  private final CommonMetrics commonMetrics;
  private final ExecutorLoader executorLoader;
  private final CleanerThread cleanerThread;
  private final ExecutingManagerUpdaterThread executingManager;
  private final ExecutorApiGateway apiGateway;
  private final int maxConcurrentRunsOneFlow;
  private final ExecutorManagerUpdaterStage updaterStage = new ExecutorManagerUpdaterStage();
  QueuedExecutions queuedFlows;
  File cacheDir;
  //make it immutable to ensure threadsafety
  private volatile ImmutableSet<Executor> activeExecutors = null;
  private QueueProcessorThread queueProcessor;
  private volatile Pair<ExecutionReference, ExecutableFlow> runningCandidate = null;
  private List<String> filterList;
  private Map<String, Integer> comparatorWeightsMap;
  private long lastSuccessfulExecutorInfoRefresh;
  private ExecutorService executorInforRefresherService;

  @Inject
  public ExecutorManager(final Props azkProps, final ExecutorLoader loader,
      final AlerterHolder alerterHolder,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway) throws ExecutorManagerException {
    this.alerterHolder = alerterHolder;
    this.azkProps = azkProps;
    this.commonMetrics = commonMetrics;
    this.executorLoader = loader;
    this.apiGateway = apiGateway;
    this.setupExecutors();
    this.loadRunningFlows();

    this.queuedFlows = new QueuedExecutions(
        azkProps.getLong(Constants.ConfigurationKeys.WEBSERVER_QUEUE_SIZE, 100000));

    // The default threshold is set to 30 for now, in case some users are affected. We may
    // decrease this number in future, to better prevent DDos attacks.
    this.maxConcurrentRunsOneFlow = azkProps
        .getInt(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW,
            DEFAULT_MAX_ONCURRENT_RUNS_ONEFLOW);
    this.loadQueuedFlows();

    this.cacheDir = new File(azkProps.getString("cache.directory", "cache"));

    this.executingManager = new ExecutingManagerUpdaterThread(
        this.updaterStage, alerterHolder, commonMetrics, apiGateway, this);

    if (isMultiExecutorMode()) {
      setupMultiExecutorMode();
    }

    final long executionLogsRetentionMs =
        azkProps.getLong("execution.logs.retention.ms",
            DEFAULT_EXECUTION_LOGS_RETENTION_MS);

    this.cleanerThread = new CleanerThread(executionLogsRetentionMs);
  }

  // TODO move to some common place
  static boolean isFinished(final ExecutableFlow flow) {
    switch (flow.getStatus()) {
      case SUCCEEDED:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
    }
  }

  public void start() {
    this.executingManager.start();
    this.cleanerThread.start();
    if (isMultiExecutorMode()) {
      this.queueProcessor.start();
    }
  }

  private String findApplicationIdFromLog(final String logData) {
    final Matcher matcher = APPLICATION_ID_PATTERN.matcher(logData);
    String appId = null;
    if (matcher.find()) {
      appId = matcher.group().substring(12);
    }
    this.logger.info("Application ID is " + appId);
    return appId;
  }

  private void setupMultiExecutorMode() {
    // initialize hard filters for executor selector from azkaban.properties
    final String filters = this.azkProps
        .getString(Constants.ConfigurationKeys.EXECUTOR_SELECTOR_FILTERS, "");
    if (filters != null) {
      this.filterList = Arrays.asList(StringUtils.split(filters, ","));
    }

    // initialize comparator feature weights for executor selector from azkaban.properties
    final Map<String, String> compListStrings = this.azkProps
        .getMapByPrefix(Constants.ConfigurationKeys.EXECUTOR_SELECTOR_COMPARATOR_PREFIX);
    if (compListStrings != null) {
      this.comparatorWeightsMap = new TreeMap<>();
      for (final Map.Entry<String, String> entry : compListStrings.entrySet()) {
        this.comparatorWeightsMap.put(entry.getKey(), Integer.valueOf(entry.getValue()));
      }
    }

    this.executorInforRefresherService =
        Executors.newFixedThreadPool(this.azkProps.getInt(
            Constants.ConfigurationKeys.EXECUTORINFO_REFRESH_MAX_THREADS, 5));

    // configure queue processor
    this.queueProcessor =
        new QueueProcessorThread(
            this.azkProps.getBoolean(Constants.ConfigurationKeys.QUEUEPROCESSING_ENABLED, true),
            this.azkProps.getLong(Constants.ConfigurationKeys.ACTIVE_EXECUTOR_REFRESH_IN_MS, 50000),
            this.azkProps.getInt(
                Constants.ConfigurationKeys.ACTIVE_EXECUTOR_REFRESH_IN_NUM_FLOW, 5),
            this.azkProps.getInt(
                Constants.ConfigurationKeys.MAX_DISPATCHING_ERRORS_PERMITTED,
                this.activeExecutors.size()));
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#setupExecutors()
   */
  @Override
  public void setupExecutors() throws ExecutorManagerException {
    final Set<Executor> newExecutors = new HashSet<>();

    if (isMultiExecutorMode()) {
      logger.info("Initializing multi executors from database.");
      newExecutors.addAll(this.executorLoader.fetchActiveExecutors());
    } else if (this.azkProps.containsKey(ConfigurationKeys.EXECUTOR_PORT)) {
      // add local executor, if specified as per properties
      final String executorHost = this.azkProps
          .getString(Constants.ConfigurationKeys.EXECUTOR_HOST, "localhost");
      final int executorPort = this.azkProps.getInt(ConfigurationKeys.EXECUTOR_PORT);
      logger.info(String.format("Initializing local executor %s:%d",
          executorHost, executorPort));
      Executor executor =
          this.executorLoader.fetchExecutor(executorHost, executorPort);
      if (executor == null) {
        executor = this.executorLoader.addExecutor(executorHost, executorPort);
      } else if (!executor.isActive()) {
        executor.setActive(true);
        this.executorLoader.updateExecutor(executor);
      }
      newExecutors.add(new Executor(executor.getId(), executorHost,
          executorPort, true));
    } else {
      // throw exception when in single executor mode and no executor port specified in azkaban
      // properties
      //todo chengren311: convert to slf4j and parameterized logging
      final String error = "Missing" + ConfigurationKeys.EXECUTOR_PORT + " in azkaban properties.";
      logger.error(error);
      throw new ExecutorManagerException(error);
    }

    if (newExecutors.isEmpty()) {
      final String error = "No active executor found";
      logger.error(error);
      throw new ExecutorManagerException(error);
    } else {
      this.activeExecutors = ImmutableSet.copyOf(newExecutors);
    }
  }

  private boolean isMultiExecutorMode() {
    return this.azkProps.getBoolean(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, false);
  }

  /**
   * Refresh Executor stats for all the actie executors in this executorManager
   */
  private void refreshExecutors() {

    final List<Pair<Executor, Future<ExecutorInfo>>> futures =
        new ArrayList<>();
    for (final Executor executor : this.activeExecutors) {
      // execute each executorInfo refresh task to fetch
      final Future<ExecutorInfo> fetchExecutionInfo =
          this.executorInforRefresherService.submit(
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
   * Throws exception if running in local mode {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#disableQueueProcessorThread()
   */
  @Override
  public void disableQueueProcessorThread() throws ExecutorManagerException {
    if (isMultiExecutorMode()) {
      this.queueProcessor.setActive(false);
    } else {
      throw new ExecutorManagerException(
          "Cannot disable QueueProcessor in local mode");
    }
  }

  /**
   * Throws exception if running in local mode {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#enableQueueProcessorThread()
   */
  @Override
  public void enableQueueProcessorThread() throws ExecutorManagerException {
    if (isMultiExecutorMode()) {
      this.queueProcessor.setActive(true);
    } else {
      throw new ExecutorManagerException(
          "Cannot enable QueueProcessor in local mode");
    }
  }

  public State getQueueProcessorThreadState() {
    if (isMultiExecutorMode()) {
      return this.queueProcessor.getState();
    } else {
      return State.NEW; // not started in local mode
    }
  }

  /**
   * Returns state of QueueProcessor False, no flow is being dispatched True , flows are being
   * dispatched as expected
   */
  public boolean isQueueProcessorThreadActive() {
    if (isMultiExecutorMode()) {
      return this.queueProcessor.isActive();
    } else {
      return false;
    }
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
    return this.executingManager.getState();
  }

  public String getExecutorThreadStage() {
    return this.updaterStage.get();
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    return this.executingManager.isAlive();
  }

  @Override
  public long getLastExecutorManagerThreadCheckTime() {
    return this.executingManager.getLastThreadCheckTime();
  }

  @Override
  public Collection<Executor> getAllActiveExecutors() {
    return Collections.unmodifiableCollection(this.activeExecutors);
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#fetchExecutor(int)
   */
  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    for (final Executor executor : this.activeExecutors) {
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
    for (final Executor executor : this.activeExecutors) {
      ports.add(executor.getHost() + ":" + executor.getPort());
    }
    return ports;
  }

  @Override
  public Set<String> getAllActiveExecutorServerHosts() {
    // Includes non primary server/hosts
    final HashSet<String> ports = new HashSet<>();
    for (final Executor executor : this.activeExecutors) {
      ports.add(executor.getHost() + ":" + executor.getPort());
    }
    // include executor which were initially active and still has flows running
    for (final Pair<ExecutionReference, ExecutableFlow> running : this.runningFlows
        .values()) {
      final ExecutionReference ref = running.getFirst();
      if (ref.getExecutor().isPresent()) {
        final Executor executor = ref.getExecutor().get();
        ports.add(executor.getHost() + ":" + executor.getPort());
      }
    }
    return ports;
  }

  private void loadRunningFlows() throws ExecutorManagerException {
    logger.info("Loading running flows from database..");
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = this.executorLoader
        .fetchActiveFlows();
    logger.info("Loaded " + activeFlows.size() + " running flows");
    this.runningFlows.putAll(activeFlows);
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
        this.runningFlows.values()));
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
    getActiveFlowsWithExecutorHelper(flows, this.runningFlows.values());
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
            || isFlowRunningHelper(projectId, flowId, this.runningFlows.values());
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
    getActiveFlowHelper(flows, this.runningFlows.values());
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
   * Get execution Ids of all active (running, non-dispatched) flows
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows()
   */
  public String getRunningFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    getRunningFlowsIdsHelper(allIds, this.queuedFlows.getAllEntries());
    getRunningFlowsIdsHelper(allIds, this.runningFlows.values());
    Collections.sort(allIds);
    return allIds.toString();
  }

  /**
   * Get execution Ids of all non-dispatched flows
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows()
   */
  public String getQueuedFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    getRunningFlowsIdsHelper(allIds, this.queuedFlows.getAllEntries());
    Collections.sort(allIds);
    return allIds.toString();
  }

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
  public List<ExecutableFlow> getExecutableFlows(final Project project,
      final String flowId, final int skip, final int size) throws ExecutorManagerException {
    final List<ExecutableFlow> flows =
        this.executorLoader.fetchFlowHistory(project.getId(), flowId, skip, size);
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
  public int getNumberOfExecutions(final Project project, final String flowId)
      throws ExecutorManagerException {
    return this.executorLoader.fetchNumExecutableFlows(project.getId(), flowId);
  }

  @Override
  public LogData getExecutableFlowLog(final ExecutableFlow exFlow, final int offset,
      final int length) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningFlows.get(exFlow.getExecutionId());
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
        this.runningFlows.get(exFlow.getExecutionId());
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
        this.runningFlows.get(exFlow.getExecutionId());
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

  @Override
  public String getJobLinkUrl(final ExecutableFlow exFlow, final String jobId, final int attempt) {
    if (!this.azkProps.containsKey(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL) || !this.azkProps
        .containsKey(ConfigurationKeys.HISTORY_SERVER_JOB_URL) || !this.azkProps
        .containsKey(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL)) {
      return null;
    }

    final String applicationId = getApplicationId(exFlow, jobId, attempt);
    if (applicationId == null) {
      return null;
    }

    final URL url;
    final String jobLinkUrl;
    boolean isRMJobLinkValid = true;

    try {
      url = new URL(this.azkProps.getString(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL)
          .replace(APPLICATION_ID, applicationId));
      final String keytabPrincipal = requireNonNull(
          this.azkProps.getString(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL));
      final String keytabPath = requireNonNull(this.azkProps.getString(ConfigurationKeys
          .AZKABAN_KEYTAB_PATH));
      final HttpURLConnection connection = AuthenticationUtils.loginAuthenticatedURL(url,
          keytabPrincipal, keytabPath);

      try (final BufferedReader in = new BufferedReader(
          new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          if (FAILED_TO_READ_APPLICATION_PATTERN.matcher(inputLine).find()
              || INVALID_APPLICATION_ID_PATTERN.matcher(inputLine).find()) {
            this.logger.info(
                "RM job link is invalid or has expired for application_" + applicationId);
            isRMJobLinkValid = false;
            break;
          }
        }
      }
    } catch (final Exception e) {
      this.logger.error("Failed to get job link for application_" + applicationId, e);
      return null;
    }

    if (isRMJobLinkValid) {
      jobLinkUrl = url.toString();
    } else {
      // If RM job link is invalid or has expired, fetch the job link from JHS or SHS.
      if (exFlow.getExecutableNode(jobId).getType().equals(SPARK_JOB_TYPE)) {
        jobLinkUrl =
            this.azkProps.get(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL).replace
                (APPLICATION_ID, applicationId);
      } else {
        jobLinkUrl =
            this.azkProps.get(ConfigurationKeys.HISTORY_SERVER_JOB_URL).replace(APPLICATION_ID,
                applicationId);
      }
    }

    this.logger.info(
        "Job link url is " + jobLinkUrl + " for execution " + exFlow.getExecutionId() + ", job "
            + jobId);
    return jobLinkUrl;
  }

  private String getApplicationId(final ExecutableFlow exFlow, final String jobId,
      final int attempt) {
    String applicationId;
    boolean finished = false;
    int offset = 0;
    try {
      while (!finished) {
        final LogData data = getExecutionJobLog(exFlow, jobId, offset, 50000, attempt);
        if (data != null) {
          applicationId = findApplicationIdFromLog(data.getData());
          if (applicationId != null) {
            return applicationId;
          }
          offset = data.getOffset() + data.getLength();
          this.logger.info("Get application ID for execution " + exFlow.getExecutionId() + ", job"
              + " " + jobId + ", attempt " + attempt + ", data offset " + offset);
        } else {
          finished = true;
        }
      }
    } catch (final ExecutorManagerException e) {
      this.logger.error("Failed to get application ID for execution " + exFlow.getExecutionId() +
          ", job " + jobId + ", attempt " + attempt + ", data offset " + offset, e);
    }
    return null;
  }

  @Override
  public JobMetaData getExecutionJobMetaData(final ExecutableFlow exFlow,
      final String jobId, final int offset, final int length, final int attempt)
      throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningFlows.get(exFlow.getExecutionId());
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
          this.apiGateway.callWithReference(pair.getFirst(), ConnectorParams.METADATA_ACTION,
              typeParam, jobIdParam, offsetParam, lengthParam, attemptParam);
      return JobMetaData.createJobMetaDataFromObject(result);
    } else {
      return null;
    }
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
      if (this.runningFlows.containsKey(exFlow.getExecutionId())) {
        final Pair<ExecutionReference, ExecutableFlow> pair =
            this.runningFlows.get(exFlow.getExecutionId());
        this.apiGateway.callWithReferenceByUser(pair.getFirst(), ConnectorParams.CANCEL_ACTION,
            userId);
      } else if (this.queuedFlows.hasExecution(exFlow.getExecutionId())) {
        this.queuedFlows.dequeue(exFlow.getExecutionId());
        finalizeFlows(exFlow, "Cancelled before dispatching to executor", null);
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
          this.runningFlows.get(exFlow.getExecutionId());
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
          this.runningFlows.get(exFlow.getExecutionId());
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
  public void pauseExecutingJobs(final ExecutableFlow exFlow, final String userId,
      final String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_PAUSE_JOBS, userId,
        jobIds);
  }

  @Override
  public void resumeExecutingJobs(final ExecutableFlow exFlow, final String userId,
      final String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RESUME_JOBS, userId,
        jobIds);
  }

  @Override
  public void retryFailures(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_FAILURES, userId);
  }

  @Override
  public void retryExecutingJobs(final ExecutableFlow exFlow, final String userId,
      final String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_JOBS, userId,
        jobIds);
  }

  @Override
  public void disableExecutingJobs(final ExecutableFlow exFlow, final String userId,
      final String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_DISABLE_JOBS, userId,
        jobIds);
  }

  @Override
  public void enableExecutingJobs(final ExecutableFlow exFlow, final String userId,
      final String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_ENABLE_JOBS, userId,
        jobIds);
  }

  @Override
  public void cancelExecutingJobs(final ExecutableFlow exFlow, final String userId,
      final String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_CANCEL_JOBS, userId,
        jobIds);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> modifyExecutingJobs(final ExecutableFlow exFlow,
      final String command, final String userId, final String... jobIds)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.runningFlows.get(exFlow.getExecutionId());
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

    final String exFlowKey = exflow.getProjectName() + "." + exflow.getId() + ".submitFlow";
    // using project and flow name to prevent race condition when same flow is submitted by API and schedule at the same time
    // causing two same flow submission entering this piece.
    synchronized (exFlowKey.intern()) {
      final String flowId = exflow.getFlowId();

      logger.info("Submitting execution flow " + flowId + " by " + userId);

      String message = "";
      if (this.queuedFlows.isFull()) {
        message =
            String
                .format(
                    "Failed to submit %s for project %s. Azkaban has overrun its webserver queue capacity",
                    flowId, exflow.getProjectName());
        logger.error(message);
      } else {
        final int projectId = exflow.getProjectId();
        exflow.setSubmitUser(userId);
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

        if (!running.isEmpty()) {
          if (running.size() > this.maxConcurrentRunsOneFlow) {
            throw new ExecutorManagerException("Flow " + flowId
                + " has more than " + this.maxConcurrentRunsOneFlow + " concurrent runs. Skipping",
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

        if (isMultiExecutorMode()) {
          //Take MultiExecutor route
          this.executorLoader.addActiveExecutableReference(reference);
          this.queuedFlows.enqueue(exflow, reference);
        } else {
          // assign only local executor we have
          final Executor choosenExecutor = this.activeExecutors.iterator().next();
          this.executorLoader.addActiveExecutableReference(reference);
          try {
            dispatch(reference, exflow, choosenExecutor);
            this.commonMetrics.markDispatchSuccess();
          } catch (final ExecutorManagerException e) {
            // When flow dispatch fails, should update the flow status
            // to FAILED in execution_flows DB table as well. Currently
            // this logic is only implemented in multiExecutorMode but
            // missed in single executor case.
            this.commonMetrics.markDispatchFail();
            finalizeFlows(exflow, "Dispatching failed", e);
            throw e;
          }
        }
        message +=
            "Execution submitted successfully with exec id "
                + exflow.getExecutionId();
      }
      return message;
    }
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
    if (isMultiExecutorMode()) {
      this.queueProcessor.shutdown();
    }
    this.executingManager.shutdown();
  }

  void finalizeFlows(final ExecutableFlow flow, final String reason,
      final Throwable originalError) {

    final int execId = flow.getExecutionId();
    boolean alertUser = true;
    final String[] extraReasons = getFinalizeFlowReasons(reason, originalError);
    this.updaterStage.set("finalizing flow " + execId);
    // First we check if the execution in the datastore is complete
    try {
      final ExecutableFlow dsFlow;
      if (isFinished(flow)) {
        dsFlow = flow;
      } else {
        this.updaterStage.set("finalizing flow " + execId + " loading from db");
        dsFlow = this.executorLoader.fetchExecutableFlow(execId);

        // If it's marked finished, we're good. If not, we fail everything and
        // then mark it finished.
        if (!isFinished(dsFlow)) {
          this.updaterStage.set("finalizing flow " + execId + " failing the flow");
          failEverything(dsFlow);
          this.executorLoader.updateExecutableFlow(dsFlow);
        }
      }

      this.updaterStage.set("finalizing flow " + execId + " deleting active reference");

      // Delete the executing reference.
      if (flow.getEndTime() == -1) {
        flow.setEndTime(System.currentTimeMillis());
        this.executorLoader.updateExecutableFlow(dsFlow);
      }
      this.executorLoader.removeActiveExecutableReference(execId);

      this.updaterStage.set("finalizing flow " + execId + " cleaning from memory");
      this.runningFlows.remove(execId);
    } catch (final ExecutorManagerException e) {
      alertUser = false; // failed due to azkaban internal error, not to alert user
      logger.error(e);
    }

    // TODO append to the flow log that we marked this flow as failed + the extraReasons

    this.updaterStage.set("finalizing flow " + execId + " alerting and emailing");
    if (alertUser) {
      final ExecutionOptions options = flow.getExecutionOptions();
      // But we can definitely email them.
      final Alerter mailAlerter = this.alerterHolder.get("email");
      if (flow.getStatus() != Status.SUCCEEDED) {
        if (options.getFailureEmails() != null && !options.getFailureEmails().isEmpty()) {
          try {
            mailAlerter.alertOnError(flow, extraReasons);
          } catch (final Exception e) {
            logger.error(e);
          }
        }
        if (options.getFlowParameters().containsKey("alert.type")) {
          final String alertType = options.getFlowParameters().get("alert.type");
          final Alerter alerter = this.alerterHolder.get(alertType);
          if (alerter != null) {
            try {
              alerter.alertOnError(flow, extraReasons);
            } catch (final Exception e) {
              logger.error("Failed to alert by " + alertType, e);
            }
          } else {
            logger.error("Alerter type " + alertType + " doesn't exist. Failed to alert.");
          }
        }
      } else {
        if (options.getSuccessEmails() != null && !options.getSuccessEmails().isEmpty()) {
          try {

            mailAlerter.alertOnSuccess(flow);
          } catch (final Exception e) {
            logger.error(e);
          }
        }
        if (options.getFlowParameters().containsKey("alert.type")) {
          final String alertType = options.getFlowParameters().get("alert.type");
          final Alerter alerter = this.alerterHolder.get(alertType);
          if (alerter != null) {
            try {
              alerter.alertOnSuccess(flow);
            } catch (final Exception e) {
              logger.error("Failed to alert by " + alertType, e);
            }
          } else {
            logger.error("Alerter type " + alertType + " doesn't exist. Failed to alert.");
          }
        }
      }
    }

  }

  private String[] getFinalizeFlowReasons(final String reason, final Throwable originalError) {
    final List<String> reasons = new LinkedList<>();
    reasons.add(reason);
    if (originalError != null) {
      reasons.add(ExceptionUtils.getStackTrace(originalError));
    }
    return reasons.toArray(new String[reasons.size()]);
  }

  private void failEverything(final ExecutableFlow exFlow) {
    final long time = System.currentTimeMillis();
    for (final ExecutableNode node : exFlow.getExecutableNodes()) {
      switch (node.getStatus()) {
        case SUCCEEDED:
        case FAILED:
        case KILLED:
        case SKIPPED:
        case DISABLED:
          continue;
          // case UNKNOWN:
        case READY:
          node.setStatus(Status.KILLING);
          break;
        default:
          node.setStatus(Status.FAILED);
          break;
      }

      if (node.getStartTime() == -1) {
        node.setStartTime(time);
      }
      if (node.getEndTime() == -1) {
        node.setEndTime(time);
      }
    }

    if (exFlow.getEndTime() == -1) {
      exFlow.setEndTime(time);
    }

    exFlow.setStatus(Status.FAILED);
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
   * Calls executor to dispatch the flow, update db to assign the executor and in-memory state of
   * executableFlow
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
    this.runningFlows.put(exflow.getExecutionId(),
        new Pair<>(reference, exflow));
    synchronized (this) {
      // Wake up ExecutingManagerUpdaterThread from wait() so that it will immediately check status
      // from executor(s). Normally flows will run at least some time and can't be cleaned up
      // immediately, so there will be another wait round (or many, actually), but for unit tests
      // this is significant to let them run quickly.
      this.notifyAll();
    }

    logger.info(String.format(
        "Successfully dispatched exec %d with error count %d",
        exflow.getExecutionId(), reference.getNumErrors()));
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
            logger.info("Interrupted. Probably to shut down.");
          }
        }
      }
    }

    private void cleanExecutionLogs() {
      logger.info("Cleaning old logs from execution_logs");
      final long cutoff = System.currentTimeMillis() - this.executionLogsRetentionMs;
      logger.info("Cleaning old log files before "
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

    private volatile boolean shutdown = false;
    private volatile boolean isActive = true;

    public QueueProcessorThread(final boolean isActive,
        final long activeExecutorRefreshWindowInTime,
        final int activeExecutorRefreshWindowInFlows,
        final int maxDispatchingErrors) {
      setActive(isActive);
      this.maxDispatchingErrors = maxDispatchingErrors;
      this.activeExecutorRefreshWindowInFlows =
          activeExecutorRefreshWindowInFlows;
      this.activeExecutorRefreshWindowInMillisec =
          activeExecutorRefreshWindowInTime;
      this.setName("AzkabanWebServer-QueueProcessor-Thread");
    }

    public boolean isActive() {
      return this.isActive;
    }

    public void setActive(final boolean isActive) {
      this.isActive = isActive;
      logger.info("QueueProcessorThread active turned " + this.isActive);
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
            logger.error(
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

        // if we have dispatched more than maxContinuousFlowProcessed or
        // It has been more then activeExecutorsRefreshWindow millisec since we
        // refreshed

        if (currentTime - lastExecutorRefreshTime > activeExecutorsRefreshWindow
            || currentContinuousFlowProcessed >= maxContinuousFlowProcessed) {
          // Refresh executorInfo for all activeExecutors
          refreshExecutors();
          lastExecutorRefreshTime = currentTime;
          currentContinuousFlowProcessed = 0;
        }

        /**
         * <pre>
         *  TODO: Work around till we improve Filters to have a notion of GlobalSystemState.
         *        Currently we try each queued flow once to infer a global busy state
         * Possible improvements:-
         *   1. Move system level filters in refreshExecutors and sleep if we have all executors busy after refresh
         *   2. Implement GlobalSystemState in selector or in a third place to manage system filters. Basically
         *      taking out all the filters which do not depend on the flow but are still being part of Selector.
         * Assumptions:-
         *   1. no one else except QueueProcessor is updating ExecutableFlow update time
         *   2. re-attempting a flow (which has been tried before) is considered as all executors are busy
         * </pre>
         */
        if (exflow.getUpdateTime() > lastExecutorRefreshTime) {
          // put back in the queue
          ExecutorManager.this.queuedFlows.enqueue(exflow, reference);
          ExecutorManager.this.runningCandidate = null;
          final long sleepInterval =
              activeExecutorsRefreshWindow
                  - (currentTime - lastExecutorRefreshTime);
          // wait till next executor refresh
          sleep(sleepInterval);
        } else {
          exflow.setUpdateTime(currentTime);
          // process flow with current snapshot of activeExecutors
          selectExecutorAndDispatchFlow(reference, exflow);
          ExecutorManager.this.runningCandidate = null;
        }

        // do not count failed flow processsing (flows still in queue)
        if (ExecutorManager.this.queuedFlows.getFlow(exflow.getExecutionId()) == null) {
          currentContinuousFlowProcessed++;
        }
      }
    }

    /* process flow with a snapshot of available Executors */
    private void selectExecutorAndDispatchFlow(final ExecutionReference reference,
        final ExecutableFlow exflow)
        throws ExecutorManagerException {
      final Set<Executor> remainingExecutors = new HashSet<>(ExecutorManager.this.activeExecutors);
      synchronized (exflow) {
        for (int i = 0; i <= this.maxDispatchingErrors; i++) {
          final String giveUpReason = checkGiveUpDispatching(reference, remainingExecutors);
          if (giveUpReason != null) {
            logger.error("Failed to dispatch queued execution " + exflow.getId() + " because "
                + giveUpReason);
            finalizeFlows(exflow, "Failed to dispatch because " + giveUpReason, null);
            // GIVE UP DISPATCHING - exit
            return;
          } else {
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
                logFailedDispatchAttempt(reference, exflow, selectedExecutor, e);
                ExecutorManager.this.commonMetrics.markDispatchFail();
                reference.setNumErrors(reference.getNumErrors() + 1);
                // FAILED ATTEMPT - try other executors except selectedExecutor
                remainingExecutors.remove(selectedExecutor);
              }
            }
          }
        }
        throw new IllegalStateException(
            "Unexpected error in dispatching " + exflow.getExecutionId());
      }
    }

    private String checkGiveUpDispatching(final ExecutionReference reference,
        final Set<Executor> remainingExecutors) {
      if (reference.getNumErrors() >= this.maxDispatchingErrors) {
        return "reached " + ConfigurationKeys.MAX_DISPATCHING_ERRORS_PERMITTED
            + " (tried " + reference.getNumErrors() + " executors)";
      } else if (remainingExecutors.isEmpty()) {
        return "tried calling all executors (total: "
            + ExecutorManager.this.activeExecutors.size() + ") but all failed";
      } else {
        return null;
      }
    }

    private void logFailedDispatchAttempt(final ExecutionReference reference,
        final ExecutableFlow exflow,
        final Executor selectedExecutor, final ExecutorManagerException e) {
      logger.warn(String.format(
          "Executor %s responded with exception for exec: %d",
          selectedExecutor, exflow.getExecutionId()), e);
      logger.info(String.format(
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
            logger
                .warn(String
                    .format(
                        "User specified executor id: %d for execution id: %d is not active, Looking up db.",
                        executorId, executionId));
            executor = ExecutorManager.this.executorLoader.fetchExecutor(executorId);
            if (executor == null) {
              logger
                  .warn(String
                      .format(
                          "User specified executor id: %d for execution id: %d is missing from db. Defaulting to availableExecutors",
                          executorId, executionId));
            }
          }
        } catch (final ExecutorManagerException ex) {
          logger.error("Failed to fetch user specified executor for exec_id = "
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
        logger.info("Using dispatcher for execution id :"
            + exflow.getExecutionId());
        final ExecutorSelector selector = new ExecutorSelector(ExecutorManager.this.filterList,
            ExecutorManager.this.comparatorWeightsMap);
        choosenExecutor = selector.getBest(availableExecutors, exflow);
      }
      return choosenExecutor;
    }

    private void handleNoExecutorSelectedCase(final ExecutionReference reference,
        final ExecutableFlow exflow) throws ExecutorManagerException {
      logger
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
