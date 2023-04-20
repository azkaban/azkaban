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

import static azkaban.Constants.LogConstants.NEARLINE_LOGS;
import static azkaban.Constants.LogConstants.OFFLINE_LOGS;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.executor.selector.ExecutorComparator;
import azkaban.executor.selector.ExecutorFilter;
import azkaban.executor.selector.ExecutorSelector;
import azkaban.logs.ExecutionLogsLoader;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.DummyContainerizationMetricsImpl;
import azkaban.project.ProjectManager;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.lang.Thread.State;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Executor manager used to manage the client side job.
 *
 * @deprecated replaced by {@link ExecutionController}
 */
@Singleton
@Deprecated
public class ExecutorManager extends AbstractExecutorManagerAdapter {

  private static final Logger logger = Logger.getLogger(ExecutorManager.class);
  private final RunningExecutions runningExecutions;
  private final RunningExecutionsUpdaterThread updaterThread;
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
  public ExecutorManager(final Props azkProps,
      final ProjectManager projectManager, final ExecutorLoader executorLoader,
      @Named(NEARLINE_LOGS) final ExecutionLogsLoader nearlineExecutionLogsLoader,
      @Named(OFFLINE_LOGS) @Nullable final ExecutionLogsLoader offlineExecutionLogsLoader,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway,
      final RunningExecutions runningExecutions,
      final ActiveExecutors activeExecutors,
      final ExecutorManagerUpdaterStage updaterStage,
      final ExecutionFinalizer executionFinalizer,
      final RunningExecutionsUpdaterThread updaterThread) {
    super(azkProps, projectManager, executorLoader, nearlineExecutionLogsLoader,
        offlineExecutionLogsLoader, commonMetrics, apiGateway, null, new DummyEventListener(),
        new DummyContainerizationMetricsImpl());
    this.runningExecutions = runningExecutions;
    this.activeExecutors = activeExecutors;
    this.updaterStage = updaterStage;
    this.executionFinalizer = executionFinalizer;
    this.updaterThread = updaterThread;
    this.maxConcurrentRunsOneFlow = ExecutorUtils.getMaxConcurrentRunsOneFlow(azkProps);
    this.maxConcurrentRunsPerFlowMap = ExecutorUtils.getMaxConcurentRunsPerFlowMap(azkProps);
    this.executorInfoRefresherService = createExecutorInfoRefresherService();
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
    setupExecutorComparatorWeightsMap();
    setupExecutorFilterList();
    this.queueProcessor = setupQueueProcessor();
  }

  @Override
  public void start() throws ExecutorManagerException {
    initialize();
    this.updaterThread.start();
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

  private void setupExecutorComparatorWeightsMap() {
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
        ConfigurationKeys.EXECUTORINFO_REFRESH_MAX_THREADS, 5),
        new ThreadFactoryBuilder().setNameFormat("azk-refresher-pool-%d").build());
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
                  executor.getPort(), "/serverStatistics", DispatchMethod.PUSH, Optional.empty(),
                  null,
                  ExecutorInfo.class));
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
        .fetchActiveFlows(DispatchMethod.PUSH);
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
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlowIds(int, java.lang.String)
   */
  @Override
  public List<Integer> getRunningFlowIds(final int projectId, final String flowId) {
    final List<Integer> executionIds = new ArrayList<>();
    executionIds.addAll(ExecutorUtils.getRunningFlowsHelper(projectId, flowId,
        this.queuedFlows.getAllEntries()));
    // it's possible an execution is runningCandidate, meaning it's in dispatching state neither in queuedFlows nor runningFlows,
    // so checks the runningCandidate as well.
    if (this.runningCandidate != null) {
      executionIds
          .addAll(
              ExecutorUtils.getRunningFlowsHelper(projectId, flowId,
                  Lists.newArrayList(this.runningCandidate)));
    }
    executionIds.addAll(ExecutorUtils.getRunningFlowsHelper(projectId, flowId,
        this.runningExecutions.get().values()));
    Collections.sort(executionIds);
    return executionIds;
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getActiveFlowsWithExecutor()
   */
  @Override
  public List<Pair<ExecutableFlow, Optional<Executor>>> getActiveFlowsWithExecutor() {
    final List<Pair<ExecutableFlow, Optional<Executor>>> flows =
        new ArrayList<>();
    getActiveFlowsWithExecutorHelper(flows, this.queuedFlows.getAllEntries());
    getActiveFlowsWithExecutorHelper(flows, this.runningExecutions.get().values());
    return flows;
  }

  @Override
  public List<Integer> getRunningFlowIds() {
    final ArrayList<Integer> flowIDs = new ArrayList<>();
    flowIDs.addAll(this.queuedFlows.getAllEntries().stream().map(entry -> entry.getSecond().getExecutionId()).collect(
        Collectors.toList()));
    flowIDs.addAll(this.runningExecutions.get().values().stream().map(entry -> entry.getSecond().getExecutionId()).collect(
        Collectors.toList()));
    return flowIDs;
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

  @Override
  public long getAgedQueuedFlowSize() {
    // ToDo(anish-mal) Implement this for push based dispatch logic.
    return 0;
  }

  @Override
  public DispatchMethod getDispatchMethod() {
    return DispatchMethod.PUSH;
  }

  /* Helper method to flow ids of all running flows */
  private void getRunningFlowsIdsHelper(final List<Integer> allIds,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      allIds.add(ref.getSecond().getExecutionId());
    }
  }

  @Override
  public LogData getExecutableFlowLog(final ExecutableFlow exFlow, final int offset,
      final int length) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningExecutions.get().get(exFlow.getExecutionId());
    return getFlowLogData(exFlow, offset, length, pair);
  }

  @Override
  public LogData getExecutionJobLog(final ExecutableFlow exFlow, final String jobId,
      final int offset, final int length, final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningExecutions.get().get(exFlow.getExecutionId());
    return getJobLogData(exFlow, jobId, offset, length, attempt, pair, false);
  }

  @Override
  public LogData getExecutionJobLogNearlineOnly(final ExecutableFlow exFlow, final String jobId,
      final int offset, final int length, final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningExecutions.get().get(exFlow.getExecutionId());
    return getJobLogData(exFlow, jobId, offset, length, attempt, pair, true);
  }

  @Override
  public List<Object> getExecutionJobStats(final ExecutableFlow exFlow, final String jobId,
      final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.runningExecutions.get().get(exFlow.getExecutionId());
    return getExecutionJobStats(exFlow, jobId, attempt, pair);
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
      return modifyExecutingJobs(exFlow, command, userId, pair, jobIds);
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
      if (this.queuedFlows.isFull()) {
        message =
            String
                .format(
                    "Failed to submit %s for project %s. Azkaban has overrun its webserver queue capacity",
                    flowId, exflow.getProjectName());
        logger.error(message);
        this.commonMetrics.markSubmitFlowFail();
      } else {
        message = uploadExecutableFlow(exflow, userId, flowId, message);

        // We create an active flow reference in the datastore. If the upload
        // fails, we remove the reference.
        final ExecutionReference reference =
            new ExecutionReference(exflow.getExecutionId(), exflow.getDispatchMethod());

        this.executorLoader.addActiveExecutableReference(reference);
        this.queuedFlows.enqueue(exflow, reference);
        message += "Execution queued successfully with exec id " + exflow.getExecutionId();
        this.commonMetrics.markSubmitFlowSuccess();
      }
      return message;
    }
  }

  @Override
  public void shutdown() {
    if (null != this.queueProcessor) {
      this.queueProcessor.shutdown();
    }
    if (null != this.updaterThread) {
      this.updaterThread.shutdown();
    }
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
          Thread.sleep(sleepInterval);
        } else {
          exflow.setUpdateTime(currentTime);
          // process flow with current snapshot of activeExecutors
          selectExecutorAndDispatchFlow(reference, exflow);
          ExecutorManager.this.runningCandidate = null;
        }

        // do not count failed flow processing (flows still in queue)
        if (ExecutorManager.this.queuedFlows.getFlow(exflow.getExecutionId()) == null) {
          currentContinuousFlowProcessed++;
        }
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
