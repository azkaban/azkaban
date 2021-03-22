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
package azkaban.executor;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventHandler;
import azkaban.executor.container.PodEventListener;
import azkaban.flow.FlowUtils;
import azkaban.metrics.CommonMetrics;
import azkaban.project.Project;
import azkaban.project.ProjectWhitelist;
import azkaban.spi.EventType;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used as an abstract implementation for ExecutorManagerAdapter. It has common code
 * for all the dispatch method implementations.
 */
public abstract class AbstractExecutorManagerAdapter extends EventHandler implements
    ExecutorManagerAdapter {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractExecutorManagerAdapter.class);
  protected final Props azkProps;
  protected final ExecutorLoader executorLoader;
  protected final CommonMetrics commonMetrics;
  protected final ExecutorApiGateway apiGateway;
  private final AlerterHolder alerterHolder;
  private final int maxConcurrentRunsOneFlow;
  private final Map<Pair<String, String>, Integer> maxConcurrentRunsPerFlowMap;
  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(10);
  private final PodEventListener podEventListener;

  protected AbstractExecutorManagerAdapter(final Props azkProps,
      final ExecutorLoader executorLoader,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway,
      final AlerterHolder alerterHolder) {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
    this.commonMetrics = commonMetrics;
    this.apiGateway = apiGateway;
    this.alerterHolder = alerterHolder;
    this.maxConcurrentRunsOneFlow = ExecutorUtils.getMaxConcurrentRunsOneFlow(azkProps);
    this.maxConcurrentRunsPerFlowMap = ExecutorUtils.getMaxConcurentRunsPerFlowMap(azkProps);
    this.podEventListener = new PodEventListener();
    this.addListener(podEventListener);
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
   * This method is used to get size of aged queued flows from database.
   *
   * @return
   */
  @Override
  public long getAgedQueuedFlowSize() {
    long size = 0L;
    final int minimumAgeInMinutes = this.azkProps.getInt(
        ConfigurationKeys.MIN_AGE_FOR_CLASSIFYING_A_FLOW_AGED_MINUTES,
        Constants.DEFAULT_MIN_AGE_FOR_CLASSIFYING_A_FLOW_AGED_MINUTES);
    long startTime = System.currentTimeMillis();
    // TODO(anish-mal) FetchQueuedExecutableFlows does a lot of processing that is redundant, since
    // all we care about is the count. Write a new class that's more performant and can be used for
    // metrics. this.executorLoader.fetchAgedQueuedFlows internally calls FetchQueuedExecutableFlows.
    try {
      size = this.executorLoader.fetchAgedQueuedFlows(Duration.ofMinutes(minimumAgeInMinutes))
          .size();
      logger.info("Time taken to fetch size of queued flows is {}",
          (System.currentTimeMillis() - startTime) / 1000);
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get flows queued for a long time.", e);
    }
    return size;
  }

  /**
   * This method is used to get recently finished flows from database.
   *
   * @return
   */
  @Override
  public List<ExecutableFlow> getRecentlyFinishedFlows() {
    List<ExecutableFlow> flows = new ArrayList<>();
    try {
      flows = this.executorLoader.fetchRecentlyFinishedFlows(
          RECENTLY_FINISHED_LIFETIME);
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to fetch recently finished flows.", e);
    }
    return flows;
  }

  /**
   * This method is used to get history of executions for a flow.
   *
   * @param skip
   * @param size
   * @return
   * @throws ExecutorManagerException
   */
  @Override
  public List<ExecutableFlow> getExecutableFlows(final int skip, final int size)
      throws ExecutorManagerException {
    return this.executorLoader.fetchFlowHistory(skip, size);
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final String flowIdContains,
      final int skip, final int size) throws ExecutorManagerException {
    return this.executorLoader.fetchFlowHistory(null, '%' + flowIdContains + '%', null,
        0, -1, -1, skip, size);
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final String projContain,
      final String flowContain, final String userContain, final int status, final long begin,
      final long end,
      final int skip, final int size) throws ExecutorManagerException {
    return this.executorLoader.fetchFlowHistory(projContain, flowContain, userContain,
        status, begin, end, skip, size);
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
   * Manage servlet call for jmx servlet in Azkaban execution server {@inheritDoc}
   *
   * @param hostPort
   * @param action
   * @param mBean
   * @return
   * @throws IOException
   */
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
        Integer.valueOf(hostPortSplit[1]), "/jmx", null, paramList);
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
        "/stats", null, paramList);
  }

  @Override
  public Map<String, String> doRampActions(final List<Map<String, Object>> rampActions)
      throws ExecutorManagerException {
    return this.executorLoader.doRampActions(rampActions);
  }

  /**
   * This method is used to get start status for executions in queue. By default it is PREPARING.
   * Implementation of this abstract class can have it's own start status in queue.
   *
   * @return
   */
  @Override
  public Status getStartStatus() {
    return Status.PREPARING;
  }

  /**
   * When a flow is submitted, insert a new execution into the database queue. {@inheritDoc}
   */
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
    // Use project and flow name to prevent race condition when same flow is submitted by API and
    // schedule at the same time
    // causing two same flow submission entering this piece.
    synchronized (exFlowKey.intern()) {
      final String flowId = exflow.getFlowId();
      logger.info("Submitting execution flow " + flowId + " by " + userId);

      String message = uploadExecutableFlow(exflow, userId, flowId, "");

      // Emit ready flow event
      this.fireEventListeners(Event.create(exflow,
          EventType.FLOW_STATUS_CHANGED, new EventData(exflow)));

      this.commonMetrics.markSubmitFlowSuccess();
      message += "Execution queued successfully with exec id " + exflow.getExecutionId();
      return message;
    }
  }

  @Override
  public DispatchMethod getDispatchMethod(final ExecutableFlow flow) {
    return getDispatchMethod();
  }

  protected String uploadExecutableFlow(
      final ExecutableFlow exflow, final String userId, final String flowId,
      String message) throws ExecutorManagerException {
    final int projectId = exflow.getProjectId();
    exflow.setSubmitUser(userId);
    exflow.setStatus(getStartStatus());
    exflow.setSubmitTime(System.currentTimeMillis());
    exflow.setDispatchMethod(getDispatchMethod(exflow));

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
    // Emit dispatching flow event
    exflow.setStatus(Status.DISPATCHING);
    this.fireEventListeners(Event.create(exflow, EventType.FLOW_STATUS_CHANGED,
        new EventData(exflow)));

    return message;
  }

  @Override
  public List<ExecutableJobInfo> getExecutableJobs(final Project project,
      final String jobId, final int skip, final int size) throws ExecutorManagerException {
    return this.executorLoader.fetchJobHistory(project.getId(), jobId, skip, size);
  }

  @Override
  public int getNumberOfJobExecutions(final Project project, final String jobId)
      throws ExecutorManagerException {
    return this.executorLoader.fetchNumExecutableNodes(project.getId(), jobId);
  }

  /**
   * Gets a list of all the unfinished (both dispatched and non-dispatched) executions for a given
   * project and flow {@inheritDoc}.
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows(int, java.lang.String)
   */
  @Override
  public List<Integer> getRunningFlows(final int projectId, final String flowId) {
    final List<Integer> executionIds = new ArrayList<>();
    try {
      executionIds.addAll(ExecutorUtils.getRunningFlowsHelper(projectId, flowId,
          this.executorLoader.fetchUnfinishedFlows().values()));
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get running flows for project " + projectId + ", flow "
          + flowId, e);
    }
    return executionIds;
  }

  /**
   * Get all running (unfinished) flows from database. {@inheritDoc}
   */
  @Override
  public List<ExecutableFlow> getRunningFlows() {
    final ArrayList<ExecutableFlow> flows = new ArrayList<>();
    try {
      getFlowsHelper(flows, this.executorLoader.fetchUnfinishedFlows().values());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get running flows.", e);
    }
    return flows;
  }

  protected LogData getFlowLogData(final ExecutableFlow exFlow, final int offset, final int length,
      final Pair<ExecutionReference, ExecutableFlow> pair) throws ExecutorManagerException {
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
      return this.executorLoader.fetchLogs(exFlow.getExecutionId(), "", 0, offset,
          length);
    }
  }

  protected LogData getJobLogData(final ExecutableFlow exFlow, final String jobId, final int offset,
      final int length,
      final int attempt, final Pair<ExecutionReference, ExecutableFlow> pair)
      throws ExecutorManagerException {
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
      return this.executorLoader.fetchLogs(exFlow.getExecutionId(), jobId, attempt,
          offset, length);
    }
  }

  protected List<Object> getExecutionJobStats(
      final ExecutableFlow exFlow, final String jobId, final int attempt,
      final Pair<ExecutionReference, ExecutableFlow> pair) throws ExecutorManagerException {
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
  public LogData getExecutableFlowLog(final ExecutableFlow exFlow, final int offset,
      final int length) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair = this.executorLoader
        .fetchActiveFlowByExecId(exFlow.getExecutionId());
    return getFlowLogData(exFlow, offset, length, pair);
  }

  @Override
  public LogData getExecutionJobLog(final ExecutableFlow exFlow, final String jobId,
      final int offset, final int length, final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair = this.executorLoader
        .fetchActiveFlowByExecId(exFlow.getExecutionId());
    return getJobLogData(exFlow, jobId, offset, length, attempt, pair);
  }

  @Override
  public List<Object> getExecutionJobStats(final ExecutableFlow exFlow, final String jobId,
      final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
    return getExecutionJobStats(exFlow, jobId, attempt, pair);
  }

  /**
   * If a flow is already dispatched to an executor, cancel by calling Executor. Else if it's still
   * queued in DB, remove it from DB queue and finalize. {@inheritDoc}
   */
  @Override
  public void cancelFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows = this.executorLoader
          .fetchUnfinishedFlows();
      if (unfinishedFlows.containsKey(exFlow.getExecutionId())) {
        final Pair<ExecutionReference, ExecutableFlow> pair = unfinishedFlows
            .get(exFlow.getExecutionId());
        if (pair.getFirst().getExecutor().isPresent()) {
          // Flow is already dispatched to an executor, so call that executor to cancel the flow.
          this.apiGateway
              .callWithReferenceByUser(pair.getFirst(), ConnectorParams.CANCEL_ACTION, userId);
        } else {
          // Flow is still queued, need to finalize it and update the status in DB.
          ExecutionControllerUtils.finalizeFlow(this.executorLoader, this.alerterHolder, exFlow,
              "Cancelled before dispatching to executor", null);
        }
      } else {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
    }
  }

  protected Map<String, Object> modifyExecutingJobs(final ExecutableFlow exFlow,
      final String command,
      final String userId, final Pair<ExecutionReference, ExecutableFlow> pair,
      final String[] jobIds)
      throws ExecutorManagerException {
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

  /**
   * If the Resource Manager and Job History server urls are configured, find all the Hadoop/Spark
   * application ids present in the Azkaban job's log and then construct the url to job logs in the
   * Hadoop/Spark server for each application id found. Application ids are returned in the order
   * they appear in the Azkaban job log.
   *
   * @param exFlow  The executable flow.
   * @param jobId   The job id.
   * @param attempt The job execution attempt.
   * @return The map of (application id, job log url)
   */
  @Override
  public Map<String, String> getExternalJobLogUrls(final ExecutableFlow exFlow, final String jobId,
      final int attempt) {
    final Map<String, String> jobLogUrlsByAppId = new LinkedHashMap<>();
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

  @Override
  public List<Pair<ExecutableFlow, Optional<Executor>>> getActiveFlowsWithExecutor() {
    final List<Pair<ExecutableFlow, Optional<Executor>>> flows = new ArrayList<>();
    try {
      getActiveFlowsWithExecutorHelper(flows, this.executorLoader.fetchUnfinishedFlows().values());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get active flows with executor.", e);
    }
    return flows;
  }

  /**
   * Checks whether the given flow has an active (running, non-dispatched) execution from database.
   * {@inheritDoc}
   */
  @Override
  public boolean isFlowRunning(final int projectId, final String flowId) {
    boolean isRunning = false;
    try {
      isRunning = isFlowRunningHelper(projectId, flowId,
          this.executorLoader.fetchUnfinishedFlows().values());

    } catch (final ExecutorManagerException e) {
      logger.error(
          "Failed to check if the flow is running for project " + projectId + ", flow " + flowId,
          e);
    }
    return isRunning;
  }

  /**
   * Find all the Hadoop/Spark application ids present in the Azkaban job log. When iterating over
   * the set returned by this method the application ids are in the same order they appear in the
   * log.
   *
   * @param exFlow  The executable flow.
   * @param jobId   The job id.
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
        logger.info("Get application ID for execution " + exFlow.getExecutionId() + ", job"
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
      logger.error("Failed to get application ID for execution " + exFlow.getExecutionId() +
          ", job " + jobId + ", attempt " + attempt + ", data offset " + offset, e);
    }
    return applicationIds;
  }

  /* Helper method to get all execution ids from collection in sorted order. */
  protected void getExecutionIdsHelper(final List<Integer> allIds,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    collection.stream().forEach(ref -> allIds.add(ref.getSecond().getExecutionId()));
    Collections.sort(allIds);
  }

  /* Search a running flow in a collection */
  protected boolean isFlowRunningHelper(final int projectId, final String flowId,
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
   * Helper method to get all flows from collection.
   */
  protected void getFlowsHelper(final ArrayList<ExecutableFlow> flows,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    collection.stream().forEach(ref -> flows.add(ref.getSecond()));
  }

  /* Helper method for getActiveFlowsWithExecutor */
  protected void getActiveFlowsWithExecutorHelper(
      final List<Pair<ExecutableFlow, Optional<Executor>>> flows,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      flows.add(new Pair<>(ref.getSecond(), ref
          .getFirst().getExecutor()));
    }
  }

}
