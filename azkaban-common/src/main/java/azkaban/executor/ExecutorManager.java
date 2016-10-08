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

import java.io.File;
import java.io.IOException;
import java.lang.Thread.State;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.alert.Alerter;
import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.event.EventData;
import azkaban.event.EventHandler;
import azkaban.executor.selector.ExecutorComparator;
import azkaban.executor.selector.ExecutorFilter;
import azkaban.executor.selector.ExecutorSelector;
import azkaban.project.Project;
import azkaban.project.ProjectWhitelist;
import azkaban.scheduler.ScheduleStatisticManager;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;

/**
 * Executor manager used to manage the client side job.
 *
 */
public class ExecutorManager extends EventHandler implements
    ExecutorManagerAdapter {
  static final String AZKABAN_EXECUTOR_SELECTOR_FILTERS =
      "azkaban.executorselector.filters";
  static final String AZKABAN_EXECUTOR_SELECTOR_COMPARATOR_PREFIX =
      "azkaban.executorselector.comparator.";
  static final String AZKABAN_QUEUEPROCESSING_ENABLED =
    "azkaban.queueprocessing.enabled";
  static final String AZKABAN_USE_MULTIPLE_EXECUTORS =
    "azkaban.use.multiple.executors";
  private static final String AZKABAN_WEBSERVER_QUEUE_SIZE =
    "azkaban.webserver.queue.size";
  private static final String AZKABAN_ACTIVE_EXECUTOR_REFRESH_IN_MS =
    "azkaban.activeexecutor.refresh.milisecinterval";
  private static final String AZKABAN_ACTIVE_EXECUTOR_REFRESH_IN_NUM_FLOW =
    "azkaban.activeexecutor.refresh.flowinterval";
  private static final String AZKABAN_EXECUTORINFO_REFRESH_MAX_THREADS =
      "azkaban.executorinfo.refresh.maxThreads";
  private static final String AZKABAN_MAX_DISPATCHING_ERRORS_PERMITTED =
    "azkaban.maxDispatchingErrors";

  private static Logger logger = Logger.getLogger(ExecutorManager.class);
  private ExecutorLoader executorLoader;

  private CleanerThread cleanerThread;

  private ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> runningFlows =
      new ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>>();
  private ConcurrentHashMap<Integer, ExecutableFlow> recentlyFinished =
      new ConcurrentHashMap<Integer, ExecutableFlow>();

  QueuedExecutions queuedFlows;

  final private Set<Executor> activeExecutors = new HashSet<Executor>();
  private QueueProcessorThread queueProcessor;

  private ExecutingManagerUpdaterThread executingManager;
  // 12 weeks
  private static final long DEFAULT_EXECUTION_LOGS_RETENTION_MS = 3 * 4 * 7
      * 24 * 60 * 60 * 1000L;
  private long lastCleanerThreadCheckTime = -1;

  private long lastThreadCheckTime = -1;
  private String updaterStage = "not started";

  private Map<String, Alerter> alerters;

  File cacheDir;

  private final Props azkProps;
  private List<String> filterList;
  private Map<String, Integer> comparatorWeightsMap;
  private long lastSuccessfulExecutorInfoRefresh;
  private ExecutorService executorInforRefresherService;

  public ExecutorManager(Props azkProps, ExecutorLoader loader,
      Map<String, Alerter> alerters) throws ExecutorManagerException {
    this.alerters = alerters;
    this.azkProps = azkProps;
    this.executorLoader = loader;
    this.setupExecutors();
    this.loadRunningFlows();

    queuedFlows =
        new QueuedExecutions(azkProps.getLong(AZKABAN_WEBSERVER_QUEUE_SIZE, 100000));
    this.loadQueuedFlows();

    cacheDir = new File(azkProps.getString("cache.directory", "cache"));

    executingManager = new ExecutingManagerUpdaterThread();
    executingManager.start();

    if(isMultiExecutorMode()) {
      setupMultiExecutorMode();
    }

    long executionLogsRetentionMs =
        azkProps.getLong("execution.logs.retention.ms",
        DEFAULT_EXECUTION_LOGS_RETENTION_MS);

    cleanerThread = new CleanerThread(executionLogsRetentionMs);
    cleanerThread.start();

  }

  private void setupMultiExecutorMode() {
    // initliatize hard filters for executor selector from azkaban.properties
    String filters = azkProps.getString(AZKABAN_EXECUTOR_SELECTOR_FILTERS, "");
    if (filters != null) {
      filterList = Arrays.asList(StringUtils.split(filters, ","));
    }

    // initliatize comparator feature weights for executor selector from
    // azkaban.properties
    Map<String, String> compListStrings =
      azkProps.getMapByPrefix(AZKABAN_EXECUTOR_SELECTOR_COMPARATOR_PREFIX);
    if (compListStrings != null) {
      comparatorWeightsMap = new TreeMap<String, Integer>();
      for (Map.Entry<String, String> entry : compListStrings.entrySet()) {
        comparatorWeightsMap.put(entry.getKey(), Integer.valueOf(entry.getValue()));
      }
    }

    executorInforRefresherService =
        Executors.newFixedThreadPool(azkProps.getInt(
          AZKABAN_EXECUTORINFO_REFRESH_MAX_THREADS, 5));

    // configure queue processor
    queueProcessor =
      new QueueProcessorThread(azkProps.getBoolean(
        AZKABAN_QUEUEPROCESSING_ENABLED, true), azkProps.getLong(
        AZKABAN_ACTIVE_EXECUTOR_REFRESH_IN_MS, 50000), azkProps.getInt(
        AZKABAN_ACTIVE_EXECUTOR_REFRESH_IN_NUM_FLOW, 5), azkProps.getInt(
        AZKABAN_MAX_DISPATCHING_ERRORS_PERMITTED, activeExecutors.size()));

    queueProcessor.start();
  }

  /**
   *
   * {@inheritDoc}
   * @see azkaban.executor.ExecutorManagerAdapter#setupExecutors()
   */
  @Override
  public void setupExecutors() throws ExecutorManagerException {
    Set<Executor> newExecutors = new HashSet<Executor>();

    if (isMultiExecutorMode()) {
      logger.info("Initializing multi executors from database");
      newExecutors.addAll(executorLoader.fetchActiveExecutors());
    } else if (azkProps.containsKey("executor.port")) {
      // Add local executor, if specified as per properties
      String executorHost = azkProps.getString("executor.host", "localhost");
      int executorPort = azkProps.getInt("executor.port");
      logger.info(String.format("Initializing local executor %s:%d",
        executorHost, executorPort));
      Executor executor =
        executorLoader.fetchExecutor(executorHost, executorPort);
      if (executor == null) {
        executor = executorLoader.addExecutor(executorHost, executorPort);
      } else if (!executor.isActive()) {
        executor.setActive(true);
        executorLoader.updateExecutor(executor);
      }
      newExecutors.add(new Executor(executor.getId(), executorHost,
        executorPort, true));
    }

    if (newExecutors.isEmpty()) {
      logger.error("No active executor found");
      throw new ExecutorManagerException("No active executor found");
    } else if(newExecutors.size() > 1 && !isMultiExecutorMode()) {
      logger.error("Multiple local executors specified");
      throw new ExecutorManagerException("Multiple local executors specified");
    } else {
      // clear all active executors, only if we have at least one new active
      // executors
      activeExecutors.clear();
      activeExecutors.addAll(newExecutors);
    }
  }

  private boolean isMultiExecutorMode() {
    return azkProps.getBoolean(AZKABAN_USE_MULTIPLE_EXECUTORS, false);
  }

  /**
   * Refresh Executor stats for all the actie executors in this executorManager
   */
  private void refreshExecutors() {
    synchronized (activeExecutors) {

      List<Pair<Executor, Future<String>>> futures =
        new ArrayList<Pair<Executor, Future<String>>>();
      for (final Executor executor : activeExecutors) {
        // execute each executorInfo refresh task to fetch
        Future<String> fetchExecutionInfo =
          executorInforRefresherService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
              return callExecutorForJsonString(executor.getHost(),
                executor.getPort(), "/serverStatistics", null);
            }
          });
        futures.add(new Pair<Executor, Future<String>>(executor,
          fetchExecutionInfo));
      }

      boolean wasSuccess = true;
      for (Pair<Executor, Future<String>> refreshPair : futures) {
        Executor executor = refreshPair.getFirst();
        executor.setExecutorInfo(null); // invalidate cached EXecutorInfo
        try {
          // max 5 secs
          String jsonString = refreshPair.getSecond().get(5, TimeUnit.SECONDS);
          executor.setExecutorInfo(ExecutorInfo.fromJSONString(jsonString));
          logger.info(String.format(
            "Successfully refreshed executor: %s with executor info : %s",
            executor, jsonString));
        } catch (TimeoutException e) {
          wasSuccess = false;
          logger.error("Timed out while waiting for ExecutorInfo refresh"
            + executor, e);
        } catch (Exception e) {
          wasSuccess = false;
          logger.error("Failed to update ExecutorInfo for executor : "
            + executor, e);
        }
      }

      // update is successful for all executors
      if (wasSuccess) {
        lastSuccessfulExecutorInfoRefresh = System.currentTimeMillis();
      }
    }
  }

  /**
   * Throws exception if running in local mode
   * {@inheritDoc}
   * @see azkaban.executor.ExecutorManagerAdapter#disableQueueProcessorThread()
   */
  @Override
  public void disableQueueProcessorThread() throws ExecutorManagerException {
    if (isMultiExecutorMode()) {
      queueProcessor.setActive(false);
    } else {
      throw new ExecutorManagerException(
        "Cannot disable QueueProcessor in local mode");
    }
  }

  /**
   * Throws exception if running in local mode
   * {@inheritDoc}
   * @see azkaban.executor.ExecutorManagerAdapter#enableQueueProcessorThread()
   */
  @Override
  public void enableQueueProcessorThread() throws ExecutorManagerException {
    if (isMultiExecutorMode()) {
      queueProcessor.setActive(true);
    } else {
      throw new ExecutorManagerException(
        "Cannot enable QueueProcessor in local mode");
    }
  }

  public State getQueueProcessorThreadState() {
    if (isMultiExecutorMode())
      return queueProcessor.getState();
    else
      return State.NEW; // not started in local mode
  }

  /**
   * Returns state of QueueProcessor False, no flow is being dispatched True ,
   * flows are being dispatched as expected
   *
   * @return
   */
  public boolean isQueueProcessorThreadActive() {
    if (isMultiExecutorMode())
      return queueProcessor.isActive();
    else
      return false;
  }

  /**
   * Return last Successful ExecutorInfo Refresh for all active executors
   *
   * @return
   */
  public long getLastSuccessfulExecutorInfoRefresh() {
    return this.lastSuccessfulExecutorInfoRefresh;
  }

  /**
   * Get currently supported Comparators available to use via azkaban.properties
   *
   * @return
   */
  public Set<String> getAvailableExecutorComparatorNames() {
    return ExecutorComparator.getAvailableComparatorNames();

  }

  /**
   * Get currently supported filters available to use via azkaban.properties
   *
   * @return
   */
  public Set<String> getAvailableExecutorFilterNames() {
    return ExecutorFilter.getAvailableFilterNames();
  }

  @Override
  public State getExecutorManagerThreadState() {
    return executingManager.getState();
  }

  public String getExecutorThreadStage() {
    return updaterStage;
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    return executingManager.isAlive();
  }

  @Override
  public long getLastExecutorManagerThreadCheckTime() {
    return lastThreadCheckTime;
  }

  public long getLastCleanerThreadCheckTime() {
    return this.lastCleanerThreadCheckTime;
  }

  @Override
  public Collection<Executor> getAllActiveExecutors() {
    return Collections.unmodifiableCollection(activeExecutors);
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#fetchExecutor(int)
   */
  @Override
  public Executor fetchExecutor(int executorId) throws ExecutorManagerException {
    for (Executor executor : activeExecutors) {
      if (executor.getId() == executorId) {
        return executor;
      }
    }
    return executorLoader.fetchExecutor(executorId);
  }

  @Override
  public Set<String> getPrimaryServerHosts() {
    // Only one for now. More probably later.
    HashSet<String> ports = new HashSet<String>();
    for (Executor executor : activeExecutors) {
      ports.add(executor.getHost() + ":" + executor.getPort());
    }
    return ports;
  }

  @Override
  public Set<String> getAllActiveExecutorServerHosts() {
    // Includes non primary server/hosts
    HashSet<String> ports = new HashSet<String>();
    for (Executor executor : activeExecutors) {
      ports.add(executor.getHost() + ":" + executor.getPort());
    }
    // include executor which were initially active and still has flows running
    for (Pair<ExecutionReference, ExecutableFlow> running : runningFlows
      .values()) {
      ExecutionReference ref = running.getFirst();
      ports.add(ref.getHost() + ":" + ref.getPort());
    }
    return ports;
  }

  private void loadRunningFlows() throws ExecutorManagerException {
    runningFlows.putAll(executorLoader.fetchActiveFlows());
  }

  /*
   * load queued flows i.e with active_execution_reference and not assigned to
   * any executor
   */
  private void loadQueuedFlows() throws ExecutorManagerException {
    List<Pair<ExecutionReference, ExecutableFlow>> retrievedExecutions =
      executorLoader.fetchQueuedFlows();
    if (retrievedExecutions != null) {
      for (Pair<ExecutionReference, ExecutableFlow> pair : retrievedExecutions) {
        queuedFlows.enqueue(pair.getSecond(), pair.getFirst());
      }
    }
  }

  /**
   * Gets a list of all the active (running flows and non-dispatched flows)
   * executions for a given project and flow {@inheritDoc}. Results should
   * be sorted as we assume this while setting up pipelined execution Id.
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows(int,
   *      java.lang.String)
   */
  @Override
  public List<Integer> getRunningFlows(int projectId, String flowId) {
    List<Integer> executionIds = new ArrayList<Integer>();
    executionIds.addAll(getRunningFlowsHelper(projectId, flowId,
      queuedFlows.getAllEntries()));
    executionIds.addAll(getRunningFlowsHelper(projectId, flowId,
      runningFlows.values()));
    Collections.sort(executionIds);
    return executionIds;
  }

  /* Helper method for getRunningFlows */
  private List<Integer> getRunningFlowsHelper(int projectId, String flowId,
    Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    List<Integer> executionIds = new ArrayList<Integer>();
    for (Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      if (ref.getSecond().getFlowId().equals(flowId)
        && ref.getSecond().getProjectId() == projectId) {
        executionIds.add(ref.getFirst().getExecId());
      }
    }
    return executionIds;
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getActiveFlowsWithExecutor()
   */
  @Override
  public List<Pair<ExecutableFlow, Executor>> getActiveFlowsWithExecutor()
    throws IOException {
    List<Pair<ExecutableFlow, Executor>> flows =
      new ArrayList<Pair<ExecutableFlow, Executor>>();
    getActiveFlowsWithExecutorHelper(flows, queuedFlows.getAllEntries());
    getActiveFlowsWithExecutorHelper(flows, runningFlows.values());
    return flows;
  }

  /* Helper method for getActiveFlowsWithExecutor */
  private void getActiveFlowsWithExecutorHelper(
    List<Pair<ExecutableFlow, Executor>> flows,
    Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      flows.add(new Pair<ExecutableFlow, Executor>(ref.getSecond(), ref
        .getFirst().getExecutor()));
    }
  }

  /**
   * Checks whether the given flow has an active (running, non-dispatched)
   * executions {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#isFlowRunning(int,
   *      java.lang.String)
   */
  @Override
  public boolean isFlowRunning(int projectId, String flowId) {
    boolean isRunning = false;
    isRunning =
      isRunning
        || isFlowRunningHelper(projectId, flowId, queuedFlows.getAllEntries());
    isRunning =
      isRunning
        || isFlowRunningHelper(projectId, flowId, runningFlows.values());
    return isRunning;
  }

  /* Search a running flow in a collection */
  private boolean isFlowRunningHelper(int projectId, String flowId,
    Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      if (ref.getSecond().getProjectId() == projectId
        && ref.getSecond().getFlowId().equals(flowId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Fetch ExecutableFlow from an active (running, non-dispatched) or from
   * database {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getExecutableFlow(int)
   */
  @Override
  public ExecutableFlow getExecutableFlow(int execId)
    throws ExecutorManagerException {
    if (runningFlows.containsKey(execId)) {
      return runningFlows.get(execId).getSecond();
    } else if (queuedFlows.hasExecution(execId)) {
      return queuedFlows.getFlow(execId);
    } else {
      return executorLoader.fetchExecutableFlow(execId);
    }
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
    ArrayList<ExecutableFlow> flows = new ArrayList<ExecutableFlow>();
    getActiveFlowHelper(flows, queuedFlows.getAllEntries());
    getActiveFlowHelper(flows, runningFlows.values());
    return flows;
  }

  /*
   * Helper method to get all running flows from a Pair<ExecutionReference,
   * ExecutableFlow collection
   */
  private void getActiveFlowHelper(ArrayList<ExecutableFlow> flows,
    Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (Pair<ExecutionReference, ExecutableFlow> ref : collection) {
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
    List<Integer> allIds = new ArrayList<Integer>();
    getRunningFlowsIdsHelper(allIds, queuedFlows.getAllEntries());
    getRunningFlowsIdsHelper(allIds, runningFlows.values());
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
    List<Integer> allIds = new ArrayList<Integer>();
    getRunningFlowsIdsHelper(allIds, queuedFlows.getAllEntries());
    Collections.sort(allIds);
    return allIds.toString();
  }

  /* Helper method to flow ids of all running flows */
  private void getRunningFlowsIdsHelper(List<Integer> allIds,
    Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      allIds.add(ref.getSecond().getExecutionId());
    }
  }

  public List<ExecutableFlow> getRecentlyFinishedFlows() {
    return new ArrayList<ExecutableFlow>(recentlyFinished.values());
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(Project project,
      String flowId, int skip, int size) throws ExecutorManagerException {
    List<ExecutableFlow> flows =
        executorLoader.fetchFlowHistory(project.getId(), flowId, skip, size);
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(int skip, int size)
      throws ExecutorManagerException {
    List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(skip, size);
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(String flowIdContains,
      int skip, int size) throws ExecutorManagerException {
    List<ExecutableFlow> flows =
        executorLoader.fetchFlowHistory(null, '%' + flowIdContains + '%', null,
            0, -1, -1, skip, size);
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(String projContain,
      String flowContain, String userContain, int status, long begin, long end,
      int skip, int size) throws ExecutorManagerException {
    List<ExecutableFlow> flows =
        executorLoader.fetchFlowHistory(projContain, flowContain, userContain,
            status, begin, end, skip, size);
    return flows;
  }

  @Override
  public List<ExecutableJobInfo> getExecutableJobs(Project project,
      String jobId, int skip, int size) throws ExecutorManagerException {
    List<ExecutableJobInfo> nodes =
        executorLoader.fetchJobHistory(project.getId(), jobId, skip, size);
    return nodes;
  }

  @Override
  public int getNumberOfJobExecutions(Project project, String jobId)
      throws ExecutorManagerException {
    return executorLoader.fetchNumExecutableNodes(project.getId(), jobId);
  }

  @Override
  public int getNumberOfExecutions(Project project, String flowId)
      throws ExecutorManagerException {
    return executorLoader.fetchNumExecutableFlows(project.getId(), flowId);
  }

  @Override
  public LogData getExecutableFlowLog(ExecutableFlow exFlow, int offset,
      int length) throws ExecutorManagerException {
    Pair<ExecutionReference, ExecutableFlow> pair =
        runningFlows.get(exFlow.getExecutionId());
    if (pair != null) {
      Pair<String, String> typeParam = new Pair<String, String>("type", "flow");
      Pair<String, String> offsetParam =
          new Pair<String, String>("offset", String.valueOf(offset));
      Pair<String, String> lengthParam =
          new Pair<String, String>("length", String.valueOf(length));

      @SuppressWarnings("unchecked")
      Map<String, Object> result =
          callExecutorServer(pair.getFirst(), ConnectorParams.LOG_ACTION,
              typeParam, offsetParam, lengthParam);
      return LogData.createLogDataFromObject(result);
    } else {
      LogData value =
          executorLoader.fetchLogs(exFlow.getExecutionId(), "", 0, offset,
              length);
      return value;
    }
  }

  @Override
  public LogData getExecutionJobLog(ExecutableFlow exFlow, String jobId,
      int offset, int length, int attempt) throws ExecutorManagerException {
    Pair<ExecutionReference, ExecutableFlow> pair =
        runningFlows.get(exFlow.getExecutionId());
    if (pair != null) {
      Pair<String, String> typeParam = new Pair<String, String>("type", "job");
      Pair<String, String> jobIdParam =
          new Pair<String, String>("jobId", jobId);
      Pair<String, String> offsetParam =
          new Pair<String, String>("offset", String.valueOf(offset));
      Pair<String, String> lengthParam =
          new Pair<String, String>("length", String.valueOf(length));
      Pair<String, String> attemptParam =
          new Pair<String, String>("attempt", String.valueOf(attempt));

      @SuppressWarnings("unchecked")
      Map<String, Object> result =
          callExecutorServer(pair.getFirst(), ConnectorParams.LOG_ACTION,
              typeParam, jobIdParam, offsetParam, lengthParam, attemptParam);
      return LogData.createLogDataFromObject(result);
    } else {
      LogData value =
          executorLoader.fetchLogs(exFlow.getExecutionId(), jobId, attempt,
              offset, length);
      return value;
    }
  }

  @Override
  public List<Object> getExecutionJobStats(ExecutableFlow exFlow, String jobId,
      int attempt) throws ExecutorManagerException {
    Pair<ExecutionReference, ExecutableFlow> pair =
        runningFlows.get(exFlow.getExecutionId());
    if (pair == null) {
      return executorLoader.fetchAttachments(exFlow.getExecutionId(), jobId,
          attempt);
    }

    Pair<String, String> jobIdParam = new Pair<String, String>("jobId", jobId);
    Pair<String, String> attemptParam =
        new Pair<String, String>("attempt", String.valueOf(attempt));

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        callExecutorServer(pair.getFirst(), ConnectorParams.ATTACHMENTS_ACTION,
            jobIdParam, attemptParam);

    @SuppressWarnings("unchecked")
    List<Object> jobStats = (List<Object>) result.get("attachments");

    return jobStats;
  }

  @Override
  public JobMetaData getExecutionJobMetaData(ExecutableFlow exFlow,
      String jobId, int offset, int length, int attempt)
      throws ExecutorManagerException {
    Pair<ExecutionReference, ExecutableFlow> pair =
        runningFlows.get(exFlow.getExecutionId());
    if (pair != null) {

      Pair<String, String> typeParam = new Pair<String, String>("type", "job");
      Pair<String, String> jobIdParam =
          new Pair<String, String>("jobId", jobId);
      Pair<String, String> offsetParam =
          new Pair<String, String>("offset", String.valueOf(offset));
      Pair<String, String> lengthParam =
          new Pair<String, String>("length", String.valueOf(length));
      Pair<String, String> attemptParam =
          new Pair<String, String>("attempt", String.valueOf(attempt));

      @SuppressWarnings("unchecked")
      Map<String, Object> result =
          callExecutorServer(pair.getFirst(), ConnectorParams.METADATA_ACTION,
              typeParam, jobIdParam, offsetParam, lengthParam, attemptParam);
      return JobMetaData.createJobMetaDataFromObject(result);
    } else {
      return null;
    }
  }

  /**
   * if flows was dispatched to an executor, cancel by calling Executor else if
   * flow is still in queue, remove from queue and finalize {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#cancelFlow(azkaban.executor.ExecutableFlow,
   *      java.lang.String)
   */
  @Override
  public void cancelFlow(ExecutableFlow exFlow, String userId)
    throws ExecutorManagerException {
    synchronized (exFlow) {
      if (runningFlows.containsKey(exFlow.getExecutionId())) {
        Pair<ExecutionReference, ExecutableFlow> pair =
          runningFlows.get(exFlow.getExecutionId());
        callExecutorServer(pair.getFirst(), ConnectorParams.CANCEL_ACTION,
          userId);
      } else if (queuedFlows.hasExecution(exFlow.getExecutionId())) {
        queuedFlows.dequeue(exFlow.getExecutionId());
        finalizeFlows(exFlow);
      } else {
        throw new ExecutorManagerException("Execution "
          + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
          + " isn't running.");
      }
    }
  }

  @Override
  public void resumeFlow(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      Pair<ExecutionReference, ExecutableFlow> pair =
          runningFlows.get(exFlow.getExecutionId());
      if (pair == null) {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
      callExecutorServer(pair.getFirst(), ConnectorParams.RESUME_ACTION, userId);
    }
  }

  @Override
  public void pauseFlow(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      Pair<ExecutionReference, ExecutableFlow> pair =
          runningFlows.get(exFlow.getExecutionId());
      if (pair == null) {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
      callExecutorServer(pair.getFirst(), ConnectorParams.PAUSE_ACTION, userId);
    }
  }

  @Override
  public void pauseExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_PAUSE_JOBS, userId,
        jobIds);
  }

  @Override
  public void resumeExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RESUME_JOBS, userId,
        jobIds);
  }

  @Override
  public void retryFailures(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_FAILURES, userId);
  }

  @Override
  public void retryExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_JOBS, userId,
        jobIds);
  }

  @Override
  public void disableExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_DISABLE_JOBS, userId,
        jobIds);
  }

  @Override
  public void enableExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_ENABLE_JOBS, userId,
        jobIds);
  }

  @Override
  public void cancelExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_CANCEL_JOBS, userId,
        jobIds);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> modifyExecutingJobs(ExecutableFlow exFlow,
      String command, String userId, String... jobIds)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      Pair<ExecutionReference, ExecutableFlow> pair =
          runningFlows.get(exFlow.getExecutionId());
      if (pair == null) {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }

      Map<String, Object> response = null;
      if (jobIds != null && jobIds.length > 0) {
        for (String jobId : jobIds) {
          if (!jobId.isEmpty()) {
            ExecutableNode node = exFlow.getExecutableNode(jobId);
            if (node == null) {
              throw new ExecutorManagerException("Job " + jobId
                  + " doesn't exist in execution " + exFlow.getExecutionId()
                  + ".");
            }
          }
        }
        String ids = StringUtils.join(jobIds, ',');
        response =
            callExecutorServer(pair.getFirst(),
                ConnectorParams.MODIFY_EXECUTION_ACTION, userId,
                new Pair<String, String>(
                    ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE, command),
                new Pair<String, String>(ConnectorParams.MODIFY_JOBS_LIST, ids));
      } else {
        response =
            callExecutorServer(pair.getFirst(),
                ConnectorParams.MODIFY_EXECUTION_ACTION, userId,
                new Pair<String, String>(
                    ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE, command));
      }

      return response;
    }
  }

  private void applyDisabledJobs(List<Object> disabledJobs,
      ExecutableFlowBase exflow) {
    for (Object disabled : disabledJobs) {
      if (disabled instanceof String) {
        String nodeName = (String) disabled;
        ExecutableNode node = exflow.getExecutableNode(nodeName);
        if (node != null) {
          node.setStatus(Status.DISABLED);
        }
      } else if (disabled instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedDisabled = (Map<String, Object>) disabled;
        String nodeName = (String) nestedDisabled.get("id");
        @SuppressWarnings("unchecked")
        List<Object> subDisabledJobs =
            (List<Object>) nestedDisabled.get("children");

        if (nodeName == null || subDisabledJobs == null) {
          return;
        }

        ExecutableNode node = exflow.getExecutableNode(nodeName);
        if (node != null && node instanceof ExecutableFlowBase) {
          applyDisabledJobs(subDisabledJobs, (ExecutableFlowBase) node);
        }
      }
    }
  }

  @Override
  public String submitExecutableFlow(ExecutableFlow exflow, String userId)
    throws ExecutorManagerException {
    synchronized (exflow) {
      String flowId = exflow.getFlowId();

      logger.info("Submitting execution flow " + flowId + " by " + userId);

      String message = "";
      if (queuedFlows.isFull()) {
        message =
          String
            .format(
              "Failed to submit %s for project %s. Azkaban has overrun its webserver queue capacity",
              flowId, exflow.getProjectName());
        logger.error(message);
      } else {
        int projectId = exflow.getProjectId();
        exflow.setSubmitUser(userId);
        exflow.setSubmitTime(System.currentTimeMillis());

        List<Integer> running = getRunningFlows(projectId, flowId);

        ExecutionOptions options = exflow.getExecutionOptions();
        if (options == null) {
          options = new ExecutionOptions();
        }

        if (options.getDisabledJobs() != null) {
          applyDisabledJobs(options.getDisabledJobs(), exflow);
        }

        if (!running.isEmpty()) {
          if (options.getConcurrentOption().equals(
            ExecutionOptions.CONCURRENT_OPTION_PIPELINE)) {
            Collections.sort(running);
            Integer runningExecId = running.get(running.size() - 1);

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

        boolean memoryCheck =
          !ProjectWhitelist.isProjectWhitelisted(exflow.getProjectId(),
            ProjectWhitelist.WhitelistType.MemoryCheck);
        options.setMemoryCheck(memoryCheck);

        // The exflow id is set by the loader. So it's unavailable until after
        // this call.
        executorLoader.uploadExecutableFlow(exflow);

        // We create an active flow reference in the datastore. If the upload
        // fails, we remove the reference.
        ExecutionReference reference =
          new ExecutionReference(exflow.getExecutionId());

        if (isMultiExecutorMode()) {
          //Take MultiExecutor route
          executorLoader.addActiveExecutableReference(reference);
          queuedFlows.enqueue(exflow, reference);
        } else {
          // assign only local executor we have
          Executor choosenExecutor = activeExecutors.iterator().next();
          executorLoader.addActiveExecutableReference(reference);
          try {
            dispatch(reference, exflow, choosenExecutor);
          } catch (ExecutorManagerException e) {
            executorLoader.removeActiveExecutableReference(reference
              .getExecId());
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

  private void cleanOldExecutionLogs(long millis) {
    try {
      int count = executorLoader.removeExecutionLogsByTime(millis);
      logger.info("Cleaned up " + count + " log entries.");
    } catch (ExecutorManagerException e) {
      e.printStackTrace();
    }
  }

  private Map<String, Object> callExecutorServer(ExecutableFlow exflow,
    Executor executor, String action) throws ExecutorManagerException {
    try {
      return callExecutorServer(executor.getHost(), executor.getPort(), action,
        exflow.getExecutionId(), null, (Pair<String, String>[]) null);
    } catch (IOException e) {
      throw new ExecutorManagerException(e);
    }
  }

  private Map<String, Object> callExecutorServer(ExecutionReference ref,
      String action, String user) throws ExecutorManagerException {
    try {
      return callExecutorServer(ref.getHost(), ref.getPort(), action,
          ref.getExecId(), user, (Pair<String, String>[]) null);
    } catch (IOException e) {
      throw new ExecutorManagerException(e);
    }
  }

  private Map<String, Object> callExecutorServer(ExecutionReference ref,
      String action, Pair<String, String>... params)
      throws ExecutorManagerException {
    try {
      return callExecutorServer(ref.getHost(), ref.getPort(), action,
          ref.getExecId(), null, params);
    } catch (IOException e) {
      throw new ExecutorManagerException(e);
    }
  }

  private Map<String, Object> callExecutorServer(ExecutionReference ref,
      String action, String user, Pair<String, String>... params)
      throws ExecutorManagerException {
    try {
      return callExecutorServer(ref.getHost(), ref.getPort(), action,
          ref.getExecId(), user, params);
    } catch (IOException e) {
      throw new ExecutorManagerException(e);
    }
  }

  private Map<String, Object> callExecutorServer(String host, int port,
      String action, Integer executionId, String user,
      Pair<String, String>... params) throws IOException {
    List<Pair<String, String>> paramList = new ArrayList<Pair<String,String>>();

    // if params = null
    if(params != null) {
      paramList.addAll(Arrays.asList(params));
    }

    paramList
      .add(new Pair<String, String>(ConnectorParams.ACTION_PARAM, action));
    paramList.add(new Pair<String, String>(ConnectorParams.EXECID_PARAM, String
      .valueOf(executionId)));
    paramList.add(new Pair<String, String>(ConnectorParams.USER_PARAM, user));

    Map<String, Object> jsonResponse =
      callExecutorForJsonObject(host, port, "/executor", paramList);

    return jsonResponse;
  }

  /*
   * Helper method used by ExecutorManager to call executor and return json
   * object map
   */
  private Map<String, Object> callExecutorForJsonObject(String host, int port,
    String path, List<Pair<String, String>> paramList) throws IOException {
    String responseString =
      callExecutorForJsonString(host, port, path, paramList);

    @SuppressWarnings("unchecked")
    Map<String, Object> jsonResponse =
      (Map<String, Object>) JSONUtils.parseJSONFromString(responseString);
    String error = (String) jsonResponse.get(ConnectorParams.RESPONSE_ERROR);
    if (error != null) {
      throw new IOException(error);
    }
    return jsonResponse;
  }

  /*
   * Helper method used by ExecutorManager to call executor and return raw json
   * string
   */
  private String callExecutorForJsonString(String host, int port, String path,
    List<Pair<String, String>> paramList) throws IOException {
    if (paramList == null) {
      paramList = new ArrayList<Pair<String, String>>();
    }

    ExecutorApiClient apiclient = ExecutorApiClient.getInstance();
    @SuppressWarnings("unchecked")
    URI uri =
      ExecutorApiClient.buildUri(host, port, path, true,
        paramList.toArray(new Pair[0]));

    return apiclient.httpGet(uri, null);
  }

  /**
   * Manage servlet call for stats servlet in Azkaban execution server
   * {@inheritDoc}
   *
   * @throws ExecutorManagerException
   *
   * @see azkaban.executor.ExecutorManagerAdapter#callExecutorStats(java.lang.String,
   *      azkaban.utils.Pair[])
   */
  @Override
  public Map<String, Object> callExecutorStats(int executorId, String action,
    Pair<String, String>... params) throws IOException, ExecutorManagerException {
    Executor executor = fetchExecutor(executorId);

    List<Pair<String, String>> paramList =
      new ArrayList<Pair<String, String>>();

    // if params = null
    if (params != null) {
      paramList.addAll(Arrays.asList(params));
    }

    paramList
      .add(new Pair<String, String>(ConnectorParams.ACTION_PARAM, action));

    return callExecutorForJsonObject(executor.getHost(), executor.getPort(),
      "/stats", paramList);
  }


  @Override
  public Map<String, Object> callExecutorJMX(String hostPort, String action,
      String mBean) throws IOException {
    List<Pair<String, String>> paramList =
      new ArrayList<Pair<String, String>>();

    paramList.add(new Pair<String, String>(action, ""));
    if(mBean != null) {
      paramList.add(new Pair<String, String>(ConnectorParams.JMX_MBEAN, mBean));
    }

    String[] hostPortSplit = hostPort.split(":");
    return callExecutorForJsonObject(hostPortSplit[0],
      Integer.valueOf(hostPortSplit[1]), "/jmx", paramList);
  }

  @Override
  public void shutdown() {
    if (isMultiExecutorMode()) {
      queueProcessor.shutdown();
    }
    executingManager.shutdown();
  }

  private class ExecutingManagerUpdaterThread extends Thread {
    private boolean shutdown = false;

    public ExecutingManagerUpdaterThread() {
      this.setName("ExecutorManagerUpdaterThread");
    }

    // 10 mins recently finished threshold.
    private long recentlyFinishedLifetimeMs = 600000;
    private int waitTimeIdleMs = 2000;
    private int waitTimeMs = 500;

    // When we have an http error, for that flow, we'll check every 10 secs, 6
    // times (1 mins) before we evict.
    private int numErrors = 6;
    private long errorThreshold = 10000;

    private void shutdown() {
      shutdown = true;
    }

    @SuppressWarnings("unchecked")
    public void run() {
      while (!shutdown) {
        try {
          lastThreadCheckTime = System.currentTimeMillis();
          updaterStage = "Starting update all flows.";

          Map<Executor, List<ExecutableFlow>> exFlowMap =
              getFlowToExecutorMap();
          ArrayList<ExecutableFlow> finishedFlows =
              new ArrayList<ExecutableFlow>();
          ArrayList<ExecutableFlow> finalizeFlows =
              new ArrayList<ExecutableFlow>();

          if (exFlowMap.size() > 0) {
            for (Map.Entry<Executor, List<ExecutableFlow>> entry : exFlowMap
                .entrySet()) {
              List<Long> updateTimesList = new ArrayList<Long>();
              List<Integer> executionIdsList = new ArrayList<Integer>();

              Executor executor = entry.getKey();

              updaterStage =
                  "Starting update flows on " + executor.getHost() + ":"
                      + executor.getPort();

              // We pack the parameters of the same host together before we
              // query.
              fillUpdateTimeAndExecId(entry.getValue(), executionIdsList,
                  updateTimesList);

              Pair<String, String> updateTimes =
                  new Pair<String, String>(
                      ConnectorParams.UPDATE_TIME_LIST_PARAM,
                      JSONUtils.toJSON(updateTimesList));
              Pair<String, String> executionIds =
                  new Pair<String, String>(ConnectorParams.EXEC_ID_LIST_PARAM,
                      JSONUtils.toJSON(executionIdsList));

              Map<String, Object> results = null;
              try {
                results =
                    callExecutorServer(executor.getHost(),
                      executor.getPort(), ConnectorParams.UPDATE_ACTION,
                        null, null, executionIds, updateTimes);
              } catch (IOException e) {
                logger.error(e);
                for (ExecutableFlow flow : entry.getValue()) {
                  Pair<ExecutionReference, ExecutableFlow> pair =
                      runningFlows.get(flow.getExecutionId());

                  updaterStage =
                      "Failed to get update. Doing some clean up for flow "
                          + pair.getSecond().getExecutionId();

                  if (pair != null) {
                    ExecutionReference ref = pair.getFirst();
                    int numErrors = ref.getNumErrors();
                    if (ref.getNumErrors() < this.numErrors) {
                      ref.setNextCheckTime(System.currentTimeMillis()
                          + errorThreshold);
                      ref.setNumErrors(++numErrors);
                    } else {
                      logger.error("Evicting flow " + flow.getExecutionId()
                          + ". The executor is unresponsive.");
                      // TODO should send out an unresponsive email here.
                      finalizeFlows.add(pair.getSecond());
                    }
                  }
                }
              }

              // We gets results
              if (results != null) {
                List<Map<String, Object>> executionUpdates =
                    (List<Map<String, Object>>) results
                        .get(ConnectorParams.RESPONSE_UPDATED_FLOWS);
                for (Map<String, Object> updateMap : executionUpdates) {
                  try {
                    ExecutableFlow flow = updateExecution(updateMap);

                    updaterStage = "Updated flow " + flow.getExecutionId();

                    if (isFinished(flow)) {
                      finishedFlows.add(flow);
                      finalizeFlows.add(flow);
                    }
                  } catch (ExecutorManagerException e) {
                    ExecutableFlow flow = e.getExecutableFlow();
                    logger.error(e);

                    if (flow != null) {
                      logger.error("Finalizing flow " + flow.getExecutionId());
                      finalizeFlows.add(flow);
                    }
                  }
                }
              }
            }

            updaterStage = "Evicting old recently finished flows.";

            evictOldRecentlyFinished(recentlyFinishedLifetimeMs);
            // Add new finished
            for (ExecutableFlow flow : finishedFlows) {
              if (flow.getScheduleId() >= 0
                  && flow.getStatus() == Status.SUCCEEDED) {
                ScheduleStatisticManager.invalidateCache(flow.getScheduleId(),
                    cacheDir);
              }
              fireEventListeners(Event.create(flow, Type.FLOW_FINISHED, new EventData(flow.getStatus())));
              recentlyFinished.put(flow.getExecutionId(), flow);
            }

            updaterStage =
                "Finalizing " + finalizeFlows.size() + " error flows.";

            // Kill error flows
            for (ExecutableFlow flow : finalizeFlows) {
              finalizeFlows(flow);
            }
          }

          updaterStage = "Updated all active flows. Waiting for next round.";

          synchronized (this) {
            try {
              if (runningFlows.size() > 0) {
                this.wait(waitTimeMs);
              } else {
                this.wait(waitTimeIdleMs);
              }
            } catch (InterruptedException e) {
            }
          }
        } catch (Exception e) {
          logger.error(e);
        }
      }
    }
  }

  private void finalizeFlows(ExecutableFlow flow) {

    int execId = flow.getExecutionId();

    updaterStage = "finalizing flow " + execId;
    // First we check if the execution in the datastore is complete
    try {
      ExecutableFlow dsFlow;
      if (isFinished(flow)) {
        dsFlow = flow;
      } else {
        updaterStage = "finalizing flow " + execId + " loading from db";
        dsFlow = executorLoader.fetchExecutableFlow(execId);

        // If it's marked finished, we're good. If not, we fail everything and
        // then mark it finished.
        if (!isFinished(dsFlow)) {
          updaterStage = "finalizing flow " + execId + " failing the flow";
          failEverything(dsFlow);
          executorLoader.updateExecutableFlow(dsFlow);
        }
      }

      updaterStage = "finalizing flow " + execId + " deleting active reference";

      // Delete the executing reference.
      if (flow.getEndTime() == -1) {
        flow.setEndTime(System.currentTimeMillis());
        executorLoader.updateExecutableFlow(dsFlow);
      }
      executorLoader.removeActiveExecutableReference(execId);

      updaterStage = "finalizing flow " + execId + " cleaning from memory";
      runningFlows.remove(execId);
      fireEventListeners(Event.create(dsFlow, Type.FLOW_FINISHED, new EventData(dsFlow.getStatus())));
      recentlyFinished.put(execId, dsFlow);

    } catch (ExecutorManagerException e) {
      logger.error(e);
    }

    // TODO append to the flow log that we forced killed this flow because the
    // target no longer had
    // the reference.

    updaterStage = "finalizing flow " + execId + " alerting and emailing";
    ExecutionOptions options = flow.getExecutionOptions();
    // But we can definitely email them.
    Alerter mailAlerter = alerters.get("email");
    if (flow.getStatus() == Status.FAILED || flow.getStatus() == Status.KILLED) {
      if (options.getFailureEmails() != null
          && !options.getFailureEmails().isEmpty()) {
        try {
          mailAlerter.alertOnError(flow);
        } catch (Exception e) {
          logger.error(e);
        }
      }
      if (options.getFlowParameters().containsKey("alert.type")) {
        String alertType = options.getFlowParameters().get("alert.type");
        Alerter alerter = alerters.get(alertType);
        if (alerter != null) {
          try {
            alerter.alertOnError(flow);
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error("Failed to alert by " + alertType);
          }
        } else {
          logger.error("Alerter type " + alertType
              + " doesn't exist. Failed to alert.");
        }
      }
    } else {
      if (options.getSuccessEmails() != null
          && !options.getSuccessEmails().isEmpty()) {
        try {

          mailAlerter.alertOnSuccess(flow);
        } catch (Exception e) {
          logger.error(e);
        }
      }
      if (options.getFlowParameters().containsKey("alert.type")) {
        String alertType = options.getFlowParameters().get("alert.type");
        Alerter alerter = alerters.get(alertType);
        if (alerter != null) {
          try {
            alerter.alertOnSuccess(flow);
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error("Failed to alert by " + alertType);
          }
        } else {
          logger.error("Alerter type " + alertType
              + " doesn't exist. Failed to alert.");
        }
      }
    }

  }

  private void failEverything(ExecutableFlow exFlow) {
    long time = System.currentTimeMillis();
    for (ExecutableNode node : exFlow.getExecutableNodes()) {
      switch (node.getStatus()) {
      case SUCCEEDED:
      case FAILED:
      case KILLED:
      case SKIPPED:
      case DISABLED:
        continue;
        // case UNKNOWN:
      case READY:
        node.setStatus(Status.KILLED);
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

  private void evictOldRecentlyFinished(long ageMs) {
    ArrayList<Integer> recentlyFinishedKeys =
        new ArrayList<Integer>(recentlyFinished.keySet());
    long oldAgeThreshold = System.currentTimeMillis() - ageMs;
    for (Integer key : recentlyFinishedKeys) {
      ExecutableFlow flow = recentlyFinished.get(key);

      if (flow.getEndTime() < oldAgeThreshold) {
        // Evict
        recentlyFinished.remove(key);
      }
    }
  }

  private ExecutableFlow updateExecution(Map<String, Object> updateData)
      throws ExecutorManagerException {

    Integer execId =
        (Integer) updateData.get(ConnectorParams.UPDATE_MAP_EXEC_ID);
    if (execId == null) {
      throw new ExecutorManagerException(
          "Response is malformed. Need exec id to update.");
    }

    Pair<ExecutionReference, ExecutableFlow> refPair =
        this.runningFlows.get(execId);
    if (refPair == null) {
      throw new ExecutorManagerException(
          "No running flow found with the execution id. Removing " + execId);
    }

    ExecutionReference ref = refPair.getFirst();
    ExecutableFlow flow = refPair.getSecond();
    if (updateData.containsKey("error")) {
      // The flow should be finished here.
      throw new ExecutorManagerException((String) updateData.get("error"), flow);
    }

    // Reset errors.
    ref.setNextCheckTime(0);
    ref.setNumErrors(0);
    Status oldStatus = flow.getStatus();
    flow.applyUpdateObject(updateData);
    Status newStatus = flow.getStatus();

    ExecutionOptions options = flow.getExecutionOptions();
    if (oldStatus != newStatus && newStatus.equals(Status.FAILED_FINISHING)) {
      // We want to see if we should give an email status on first failure.
      if (options.getNotifyOnFirstFailure()) {
        Alerter mailAlerter = alerters.get("email");
        try {
          mailAlerter.alertOnFirstError(flow);
        } catch (Exception e) {
          e.printStackTrace();
          logger.error("Failed to send first error email." + e.getMessage());
        }
      }
      if (options.getFlowParameters().containsKey("alert.type")) {
        String alertType = options.getFlowParameters().get("alert.type");
        Alerter alerter = alerters.get(alertType);
        if (alerter != null) {
          try {
            alerter.alertOnFirstError(flow);
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error("Failed to alert by " + alertType);
          }
        } else {
          logger.error("Alerter type " + alertType
              + " doesn't exist. Failed to alert.");
        }
      }
    }

    return flow;
  }

  public boolean isFinished(ExecutableFlow flow) {
    switch (flow.getStatus()) {
    case SUCCEEDED:
    case FAILED:
    case KILLED:
      return true;
    default:
      return false;
    }
  }

  private void fillUpdateTimeAndExecId(List<ExecutableFlow> flows,
      List<Integer> executionIds, List<Long> updateTimes) {
    for (ExecutableFlow flow : flows) {
      executionIds.add(flow.getExecutionId());
      updateTimes.add(flow.getUpdateTime());
    }
  }

  /* Group Executable flow by Executors to reduce number of REST calls */
  private Map<Executor, List<ExecutableFlow>> getFlowToExecutorMap() {
    HashMap<Executor, List<ExecutableFlow>> exFlowMap =
      new HashMap<Executor, List<ExecutableFlow>>();

    for (Pair<ExecutionReference, ExecutableFlow> runningFlow : runningFlows
      .values()) {
      ExecutionReference ref = runningFlow.getFirst();
      ExecutableFlow flow = runningFlow.getSecond();
      Executor executor = ref.getExecutor();

      // We can set the next check time to prevent the checking of certain
      // flows.
      if (ref.getNextCheckTime() >= System.currentTimeMillis()) {
        continue;
      }

      List<ExecutableFlow> flows = exFlowMap.get(executor);
      if (flows == null) {
        flows = new ArrayList<ExecutableFlow>();
        exFlowMap.put(executor, flows);
      }

      flows.add(flow);
    }

    return exFlowMap;
  }

  @Override
  public int getExecutableFlows(int projectId, String flowId, int from,
      int length, List<ExecutableFlow> outputList)
      throws ExecutorManagerException {
    List<ExecutableFlow> flows =
        executorLoader.fetchFlowHistory(projectId, flowId, from, length);
    outputList.addAll(flows);
    return executorLoader.fetchNumExecutableFlows(projectId, flowId);
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(int projectId, String flowId,
      int from, int length, Status status) throws ExecutorManagerException {
    return executorLoader.fetchFlowHistory(projectId, flowId, from, length,
        status);
  }

  /*
   * cleaner thread to clean up execution_logs, etc in DB. Runs every day.
   */
  private class CleanerThread extends Thread {
    // log file retention is 1 month.

    // check every day
    private static final long CLEANER_THREAD_WAIT_INTERVAL_MS =
        24 * 60 * 60 * 1000;

    private final long executionLogsRetentionMs;

    private boolean shutdown = false;
    private long lastLogCleanTime = -1;

    public CleanerThread(long executionLogsRetentionMs) {
      this.executionLogsRetentionMs = executionLogsRetentionMs;
      this.setName("AzkabanWebServer-Cleaner-Thread");
    }

    @SuppressWarnings("unused")
    public void shutdown() {
      shutdown = true;
      this.interrupt();
    }

    public void run() {
      while (!shutdown) {
        synchronized (this) {
          try {
            lastCleanerThreadCheckTime = System.currentTimeMillis();

            // Cleanup old stuff.
            long currentTime = System.currentTimeMillis();
            if (currentTime - CLEANER_THREAD_WAIT_INTERVAL_MS > lastLogCleanTime) {
              cleanExecutionLogs();
              lastLogCleanTime = currentTime;
            }

            wait(CLEANER_THREAD_WAIT_INTERVAL_MS);
          } catch (InterruptedException e) {
            logger.info("Interrupted. Probably to shut down.");
          }
        }
      }
    }

    private void cleanExecutionLogs() {
      logger.info("Cleaning old logs from execution_logs");
      long cutoff = DateTime.now().getMillis() - executionLogsRetentionMs;
      logger.info("Cleaning old log files before "
          + new DateTime(cutoff).toString());
      cleanOldExecutionLogs(DateTime.now().getMillis()
          - executionLogsRetentionMs);
    }
  }

  /**
   * Calls executor to dispatch the flow, update db to assign the executor and
   * in-memory state of executableFlow
   */
  private void dispatch(ExecutionReference reference, ExecutableFlow exflow,
    Executor choosenExecutor) throws ExecutorManagerException {
    exflow.setUpdateTime(System.currentTimeMillis());

    executorLoader.assignExecutor(choosenExecutor.getId(),
      exflow.getExecutionId());
    try {
      callExecutorServer(exflow, choosenExecutor,
        ConnectorParams.EXECUTE_ACTION);
    } catch (ExecutorManagerException ex) {
      logger.error("Rolling back executor assignment for execution id:"
        + exflow.getExecutionId(), ex);
      executorLoader.unassignExecutor(exflow.getExecutionId());
      throw new ExecutorManagerException(ex);
    }
    reference.setExecutor(choosenExecutor);

    // move from flow to running flows
    runningFlows.put(exflow.getExecutionId(),
      new Pair<ExecutionReference, ExecutableFlow>(reference, exflow));

    logger.info(String.format(
      "Successfully dispatched exec %d with error count %d",
      exflow.getExecutionId(), reference.getNumErrors()));
  }

  /*
   * This thread is responsible for processing queued flows using dispatcher and
   * making rest api calls to executor server
   */
  private class QueueProcessorThread extends Thread {
    private static final long QUEUE_PROCESSOR_WAIT_IN_MS = 1000;
    private final int maxDispatchingErrors;
    private final long activeExecutorRefreshWindowInMilisec;
    private final int activeExecutorRefreshWindowInFlows;

    private volatile boolean shutdown = false;
    private volatile boolean isActive = true;

    public QueueProcessorThread(boolean isActive,
      long activeExecutorRefreshWindowInTime,
      int activeExecutorRefreshWindowInFlows,
      int maxDispatchingErrors) {
      setActive(isActive);
      this.maxDispatchingErrors = maxDispatchingErrors;
      this.activeExecutorRefreshWindowInFlows =
        activeExecutorRefreshWindowInFlows;
      this.activeExecutorRefreshWindowInMilisec =
        activeExecutorRefreshWindowInTime;
      this.setName("AzkabanWebServer-QueueProcessor-Thread");
    }

    public void setActive(boolean isActive) {
      this.isActive = isActive;
      logger.info("QueueProcessorThread active turned " + this.isActive);
    }

    public boolean isActive() {
      return isActive;
    }

    public void shutdown() {
      shutdown = true;
      this.interrupt();
    }

    public void run() {
      // Loops till QueueProcessorThread is shutdown
      while (!shutdown) {
        synchronized (this) {
          try {
            // start processing queue if active, other wait for sometime
            if (isActive) {
              processQueuedFlows(activeExecutorRefreshWindowInMilisec,
                activeExecutorRefreshWindowInFlows);
            }
            wait(QUEUE_PROCESSOR_WAIT_IN_MS);
          } catch (Exception e) {
            logger.error(
              "QueueProcessorThread Interrupted. Probably to shut down.", e);
          }
        }
      }
    }

    /* Method responsible for processing the non-dispatched flows */
    private void processQueuedFlows(long activeExecutorsRefreshWindow,
      int maxContinuousFlowProcessed) throws InterruptedException,
      ExecutorManagerException {
      long lastExecutorRefreshTime = 0;
      Pair<ExecutionReference, ExecutableFlow> runningCandidate;
      int currentContinuousFlowProcessed = 0;

      while (isActive() && (runningCandidate = queuedFlows.fetchHead()) != null) {
        ExecutionReference reference = runningCandidate.getFirst();
        ExecutableFlow exflow = runningCandidate.getSecond();

        long currentTime = System.currentTimeMillis();

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
        if(exflow.getUpdateTime() > lastExecutorRefreshTime) {
          // put back in the queue
          queuedFlows.enqueue(exflow, reference);
          long sleepInterval =
            activeExecutorsRefreshWindow
              - (currentTime - lastExecutorRefreshTime);
          // wait till next executor refresh
          sleep(sleepInterval);
        } else {
          exflow.setUpdateTime(currentTime);
          // process flow with current snapshot of activeExecutors
          selectExecutorAndDispatchFlow(reference, exflow, new HashSet<Executor>(activeExecutors));
        }

        // do not count failed flow processsing (flows still in queue)
        if(queuedFlows.getFlow(exflow.getExecutionId()) == null) {
          currentContinuousFlowProcessed++;
        }
      }
    }

    /* process flow with a snapshot of available Executors */
    private void selectExecutorAndDispatchFlow(ExecutionReference reference,
      ExecutableFlow exflow, Set<Executor> availableExecutors)
      throws ExecutorManagerException {
      synchronized (exflow) {
        Executor selectedExecutor = selectExecutor(exflow, availableExecutors);
        if (selectedExecutor != null) {
          try {
            dispatch(reference, exflow, selectedExecutor);
          } catch (ExecutorManagerException e) {
            logger.warn(String.format(
              "Executor %s responded with exception for exec: %d",
              selectedExecutor, exflow.getExecutionId()), e);
            handleDispatchExceptionCase(reference, exflow, selectedExecutor,
              availableExecutors);
          }
        } else {
          handleNoExecutorSelectedCase(reference, exflow);
        }
      }
    }

    /* Helper method to fetch  overriding Executor, if a valid user has specifed otherwise return null */
    private Executor getUserSpecifiedExecutor(ExecutionOptions options,
      int executionId) {
      Executor executor = null;
      if (options != null
        && options.getFlowParameters() != null
        && options.getFlowParameters().containsKey(
          ExecutionOptions.USE_EXECUTOR)) {
        try {
          int executorId =
            Integer.valueOf(options.getFlowParameters().get(
              ExecutionOptions.USE_EXECUTOR));
          executor = fetchExecutor(executorId);

          if (executor == null) {
            logger
              .warn(String
                .format(
                  "User specified executor id: %d for execution id: %d is not active, Looking up db.",
                  executorId, executionId));
            executor = executorLoader.fetchExecutor(executorId);
            if (executor == null) {
              logger
                .warn(String
                  .format(
                    "User specified executor id: %d for execution id: %d is missing from db. Defaulting to availableExecutors",
                    executorId, executionId));
            }
          }
        } catch (ExecutorManagerException ex) {
          logger.error("Failed to fetch user specified executor for exec_id = "
            + executionId, ex);
        }
      }
      return executor;
    }

    /* Choose Executor for exflow among the available executors */
    private Executor selectExecutor(ExecutableFlow exflow,
      Set<Executor> availableExecutors) {
      Executor choosenExecutor =
        getUserSpecifiedExecutor(exflow.getExecutionOptions(),
          exflow.getExecutionId());

      // If no executor was specified by admin
      if (choosenExecutor == null) {
        logger.info("Using dispatcher for execution id :"
          + exflow.getExecutionId());
        ExecutorSelector selector = new ExecutorSelector(filterList, comparatorWeightsMap);
        choosenExecutor = selector.getBest(availableExecutors, exflow);
      }
      return choosenExecutor;
    }

    private void handleDispatchExceptionCase(ExecutionReference reference,
      ExecutableFlow exflow, Executor lastSelectedExecutor,
      Set<Executor> remainingExecutors) throws ExecutorManagerException {
      logger
        .info(String
          .format(
            "Reached handleDispatchExceptionCase stage for exec %d with error count %d",
            exflow.getExecutionId(), reference.getNumErrors()));
      reference.setNumErrors(reference.getNumErrors() + 1);
      if (reference.getNumErrors() > this.maxDispatchingErrors
        || remainingExecutors.size() <= 1) {
        logger.error("Failed to process queued flow");
        finalizeFlows(exflow);
      } else {
        remainingExecutors.remove(lastSelectedExecutor);
        // try other executors except chosenExecutor
        selectExecutorAndDispatchFlow(reference, exflow, remainingExecutors);
      }
    }

    private void handleNoExecutorSelectedCase(ExecutionReference reference,
      ExecutableFlow exflow) throws ExecutorManagerException {
      logger
      .info(String
        .format(
          "Reached handleNoExecutorSelectedCase stage for exec %d with error count %d",
            exflow.getExecutionId(), reference.getNumErrors()));
      // TODO: handle scenario where a high priority flow failing to get
      // schedule can starve all others
      queuedFlows.enqueue(exflow, reference);
    }
  }
}
