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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.alert.Alerter;
import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.event.EventHandler;
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
  private static Logger logger = Logger.getLogger(ExecutorManager.class);
  private ExecutorLoader executorLoader;

  private CleanerThread cleanerThread;

  private ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> runningFlows =
    new ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>>();
  private ConcurrentHashMap<Integer, ExecutableFlow> recentlyFinished =
    new ConcurrentHashMap<Integer, ExecutableFlow>();
  /* all flows ExecutorManager is currently dealing with */
  private ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> queuedFlowMap =
    new ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>>();
  private ConcurrentLinkedQueue<Pair<ExecutionReference, ExecutableFlow>> queuedFlows =
    new ConcurrentLinkedQueue<Pair<ExecutionReference, ExecutableFlow>>();

  private Set<Executor> activeExecutors = new HashSet<Executor>();

  private ExecutingManagerUpdaterThread executingManager;
  private QueueProcessorThread queueProcessor;

  private static final long DEFAULT_EXECUTION_LOGS_RETENTION_MS = 3 * 4 * 7
    * 24 * 60 * 60 * 1000l;

  private long lastCleanerThreadCheckTime = -1;

  private long lastThreadCheckTime = -1;
  private String updaterStage = "not started";

  private Map<String, Alerter> alerters;

  File cacheDir;

  public ExecutorManager(Props props, ExecutorLoader loader,
    Map<String, Alerter> alters) throws ExecutorManagerException {
    this.executorLoader = loader;
    this.loadRunningFlows();
    this.loadQueuedFlows();

    setupExecutors(props);

    alerters = alters;

    cacheDir = new File(props.getString("cache.directory", "cache"));

    executingManager = new ExecutingManagerUpdaterThread();
    executingManager.start();

    queueProcessor = new QueueProcessorThread();
    queueProcessor.start();

    long executionLogsRetentionMs =
      props.getLong("execution.logs.retention.ms",
        DEFAULT_EXECUTION_LOGS_RETENTION_MS);
    cleanerThread = new CleanerThread(executionLogsRetentionMs);
    cleanerThread.start();

  }

  /* setup activeExecutors using azkaban.properties and database executors */
  private void setupExecutors(Props props) throws ExecutorManagerException {
    // Add local executor, if specified as per properties
    if (props.containsKey("executor.port")) {
      String executorHost = props.getString("executor.host", "localhost");
      int executorPort = props.getInt("executor.port");
      Executor executor =
        executorLoader.fetchExecutor(executorHost, executorPort);
      if (executor == null) {
        executor = executorLoader.addExecutor(executorHost, executorPort);
      } else if (!executor.isActive()) {
        executorLoader.activateExecutor(executor.getId());
      }
      activeExecutors.add(new Executor(executor.getId(), executorHost,
        executorPort));
    }

    if (props.getBoolean("azkaban.multiple.executors", false)) {
      activeExecutors.addAll(executorLoader.fetchActiveExecutors());
    }

    if (activeExecutors.isEmpty()) {
      throw new ExecutorManagerException("No active executor found");
    }
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
  public Set<String> getPrimaryServerHosts() {
    // TODO: do we want to have a primary
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
    queuedFlows.addAll(executorLoader.fetchQueuedFlows());
    for (Pair<ExecutionReference, ExecutableFlow> ref : queuedFlows) {
      queuedFlowMap.put(ref.getSecond().getExecutionId(), ref);
    }
  }

  /**
   * Gets a list of all the active (running, non-dispatched) executions for a
   * given project and flow {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows(int,
   *      java.lang.String)
   */
  @Override
  public List<Integer> getRunningFlows(int projectId, String flowId) {
    ArrayList<Integer> executionIds = new ArrayList<Integer>();
    for (Pair<ExecutionReference, ExecutableFlow> ref : queuedFlowMap.values()) {
      if (ref.getSecond().getFlowId().equals(flowId)
        && ref.getSecond().getProjectId() == projectId) {
        executionIds.add(ref.getFirst().getExecId());
      }
    }
    for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
      if (ref.getSecond().getFlowId().equals(flowId)
        && ref.getSecond().getProjectId() == projectId) {
        executionIds.add(ref.getFirst().getExecId());
      }
    }
    return executionIds;
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
    for (Pair<ExecutionReference, ExecutableFlow> ref : queuedFlowMap.values()) {
      if (ref.getSecond().getProjectId() == projectId
        && ref.getSecond().getFlowId().equals(flowId)) {
        return true;
      }
    }
    for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
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
    } else if (queuedFlowMap.containsKey(execId)) {
      return queuedFlowMap.get(execId).getSecond();
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
    for (Pair<ExecutionReference, ExecutableFlow> ref : queuedFlowMap.values()) {
      flows.add(ref.getSecond());
    }
    for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
      flows.add(ref.getSecond());
    }
    return flows;
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
    for (Pair<ExecutionReference, ExecutableFlow> ref : queuedFlowMap.values()) {
      allIds.add(ref.getSecond().getExecutionId());
    }
    for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
      allIds.add(ref.getSecond().getExecutionId());
    }
    Collections.sort(allIds);
    return allIds.toString();
  }

  /**
   * Get recently finished flows {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRecentlyFinishedFlows()
   */
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
        executorLoader
          .fetchLogs(exFlow.getExecutionId(), "", 0, offset, length);
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
      } else if (queuedFlowMap.containsKey(exFlow.getExecutionId())) {
        Pair<ExecutionReference, ExecutableFlow> pair =
          queuedFlowMap.get(exFlow.getExecutionId());
        synchronized (pair) {
          queuedFlows.remove(pair);
          queuedFlowMap.remove(exFlow.getExecutionId());
          finalizeFlows(exFlow);
        }
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
      logger.info("Submitting execution flow " + exflow.getFlowId() + " by "
        + userId);

      int projectId = exflow.getProjectId();
      String flowId = exflow.getFlowId();
      exflow.setSubmitUser(userId);
      exflow.setSubmitTime(System.currentTimeMillis());

      List<Integer> running = getRunningFlows(projectId, flowId);

      ExecutionOptions options = exflow.getExecutionOptions();
      if (options == null) {
        options = new ExecutionOptions();
      }

      String message = "";
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
      // Added to db queue
      executorLoader.addActiveExecutableReference(reference);
      Pair<ExecutionReference, ExecutableFlow> pair =
        new Pair<ExecutionReference, ExecutableFlow>(reference, exflow);
      queuedFlowMap.put(exflow.getExecutionId(), pair);
      queuedFlows.add(pair);
      message +=
        "Execution submitted successfully with exec id "
          + exflow.getExecutionId();
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

  private Map<String, Object> callExecutorServer(ExecutionReference ref,
    String action) throws ExecutorManagerException {
    try {
      return callExecutorServer(ref.getHost(), ref.getPort(), action,
        ref.getExecId(), null, (Pair<String, String>[]) null);
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
    URIBuilder builder = new URIBuilder();
    builder.setScheme("http").setHost(host).setPort(port).setPath("/executor");

    builder.setParameter(ConnectorParams.ACTION_PARAM, action);

    if (executionId != null) {
      builder.setParameter(ConnectorParams.EXECID_PARAM,
        String.valueOf(executionId));
    }

    if (user != null) {
      builder.setParameter(ConnectorParams.USER_PARAM, user);
    }

    if (params != null) {
      for (Pair<String, String> pair : params) {
        builder.setParameter(pair.getFirst(), pair.getSecond());
      }
    }

    URI uri = null;
    try {
      uri = builder.build();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    ResponseHandler<String> responseHandler = new BasicResponseHandler();

    HttpClient httpclient = new DefaultHttpClient();
    HttpGet httpget = new HttpGet(uri);
    String response = null;
    try {
      response = httpclient.execute(httpget, responseHandler);
    } catch (IOException e) {
      throw e;
    } finally {
      httpclient.getConnectionManager().shutdown();
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> jsonResponse =
      (Map<String, Object>) JSONUtils.parseJSONFromString(response);
    String error = (String) jsonResponse.get(ConnectorParams.RESPONSE_ERROR);
    if (error != null) {
      throw new IOException(error);
    }

    return jsonResponse;
  }

  /**
   * Manage servlet call for stats servlet in Azkaban execution server
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorManagerAdapter#callExecutorStats(java.lang.String,
   *      azkaban.utils.Pair[])
   */
  @Override
  public Map<String, Object> callExecutorStats(String action,
    Pair<String, String>... params) throws IOException {

    URIBuilder builder = new URIBuilder();
    // TODO: fix to take host and port form user
    // builder.setScheme("http").setHost(executorHost).setPort(executorPort).setPath("/stats");

    builder.setParameter(ConnectorParams.ACTION_PARAM, action);

    if (params != null) {
      for (Pair<String, String> pair : params) {
        builder.setParameter(pair.getFirst(), pair.getSecond());
      }
    }

    URI uri = null;
    try {
      uri = builder.build();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    ResponseHandler<String> responseHandler = new BasicResponseHandler();

    HttpClient httpclient = new DefaultHttpClient();
    HttpGet httpget = new HttpGet(uri);
    String response = null;
    try {
      response = httpclient.execute(httpget, responseHandler);
    } catch (IOException e) {
      throw e;
    } finally {
      httpclient.getConnectionManager().shutdown();
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> jsonResponse =
      (Map<String, Object>) JSONUtils.parseJSONFromString(response);

    return jsonResponse;
  }

  @Override
  public Map<String, Object> callExecutorJMX(String hostPort, String action,
    String mBean) throws IOException {
    URIBuilder builder = new URIBuilder();

    String[] hostPortSplit = hostPort.split(":");
    builder.setScheme("http").setHost(hostPortSplit[0])
      .setPort(Integer.parseInt(hostPortSplit[1])).setPath("/jmx");

    builder.setParameter(action, "");
    if (mBean != null) {
      builder.setParameter(ConnectorParams.JMX_MBEAN, mBean);
    }

    URI uri = null;
    try {
      uri = builder.build();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    ResponseHandler<String> responseHandler = new BasicResponseHandler();

    HttpClient httpclient = new DefaultHttpClient();
    HttpGet httpget = new HttpGet(uri);
    String response = null;
    try {
      response = httpclient.execute(httpget, responseHandler);
    } catch (IOException e) {
      throw e;
    } finally {
      httpclient.getConnectionManager().shutdown();
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> jsonResponse =
      (Map<String, Object>) JSONUtils.parseJSONFromString(response);
    String error = (String) jsonResponse.get(ConnectorParams.RESPONSE_ERROR);
    if (error != null) {
      throw new IOException(error);
    }
    return jsonResponse;
  }

  @Override
  public void shutdown() {
    queueProcessor.shutdown();
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
                  callExecutorServer(executor.getHost(), executor.getPort(),
                    ConnectorParams.UPDATE_ACTION, null, null, executionIds,
                    updateTimes);
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
              fireEventListeners(Event.create(flow, Type.FLOW_FINISHED));
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

      fireEventListeners(Event.create(dsFlow, Type.FLOW_FINISHED));
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
          mailAlerter
            .alertOnError(
              flow,
              "Executor no longer seems to be running this execution. Most likely due to executor bounce.");
        } catch (Exception e) {
          logger.error(e);
        }
      }
      if (options.getFlowParameters().containsKey("alert.type")) {
        String alertType = options.getFlowParameters().get("alert.type");
        Alerter alerter = alerters.get(alertType);
        if (alerter != null) {
          try {
            alerter
              .alertOnError(
                flow,
                "Executor no longer seems to be running this execution. Most likely due to executor bounce.");
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
   * This thread is responsible for processing queued flows.
   */
  private class QueueProcessorThread extends Thread {
    private static final long QUEUE_PROCESSOR_WAIT_IN_MS = 1000;
    private boolean shutdown = false;
    private long lastProcessingTime = -1;
    private long maxContinousSubmission = 10;

    public QueueProcessorThread() {
      this.setName("AzkabanWebServer-QueueProcessor-Thread");
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
            refreshExecutors();
            lastCleanerThreadCheckTime = System.currentTimeMillis();
            long currentTime = System.currentTimeMillis();
            if (currentTime - QUEUE_PROCESSOR_WAIT_IN_MS > lastProcessingTime) {
              processQueuedFlows(maxContinousSubmission);
              lastProcessingTime = currentTime;
            }
            wait(QUEUE_PROCESSOR_WAIT_IN_MS);
          } catch (InterruptedException e) {
            logger.info("Interrupted. Probably to shut down.");
          }
        }
      }
    }
  }

  private void refreshExecutors() {
    // TODO: rest api call to refresh executor stats
  }

  private void processQueuedFlows(long maxContinousSubmission) {
    try {
      Pair<ExecutionReference, ExecutableFlow> runningCandidate;
      int submissionNum = 0;
      while (submissionNum < maxContinousSubmission
        && (runningCandidate = queuedFlows.peek()) != null) {
        synchronized (runningCandidate) {

          ExecutionReference reference = runningCandidate.getFirst();
          ExecutableFlow exflow = runningCandidate.getSecond();

          // TODO: use dispatcher
          Executor choosenExecutor;

          synchronized (activeExecutors) {
            choosenExecutor = activeExecutors.iterator().next();
          }

          if (choosenExecutor != null) {
            queuedFlows.poll();
            queuedFlowMap.remove(exflow.getExecutionId());

            try {
              reference.setExecutor(choosenExecutor);
              executorLoader.assignExecutor(choosenExecutor.getId(),
                exflow.getExecutionId());
              // TODO: ADD rest call to do an actual dispatch
              callExecutorServer(reference, ConnectorParams.EXECUTE_ACTION);
              runningFlows
                .put(exflow.getExecutionId(),
                  new Pair<ExecutionReference, ExecutableFlow>(reference,
                    exflow));
            } catch (ExecutorManagerException e) {
              logger.error("Failed to process queued flow", e);
              // TODO: allow N errors and re-try
              finalizeFlows(exflow);
            }
          }
        }
        submissionNum++;
      }
    } catch (Throwable th) {
      logger.error("Failed to process the queue", th);
    }
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
}
