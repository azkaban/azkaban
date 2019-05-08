/*
 * Copyright 2018 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.executor;

import azkaban.Constants.ConfigurationKeys;
import azkaban.event.EventHandler;
import azkaban.flow.FlowUtils;
import azkaban.metrics.CommonMetrics;
import azkaban.project.Project;
import azkaban.project.ProjectWhitelist;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.IOException;
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
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controls flow executions on web server. This module implements the polling model
 * in the new AZ dispatching design. It's injected only when azkaban.poll.model is configured to
 * true. It will ultimately replace ExecutorManager in the future.
 */
@Singleton
public class ExecutionController extends EventHandler implements ExecutorManagerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(ExecutionController.class);
  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(10);
  private final ExecutorLoader executorLoader;
  private final ExecutorApiGateway apiGateway;
  private final AlerterHolder alerterHolder;
  private final ExecutorHealthChecker executorHealthChecker;
  private final int maxConcurrentRunsOneFlow;
  private final Map<Pair<String,String>, Integer> maxConcurrentRunsPerFlowMap;
  private final CommonMetrics commonMetrics;
  private final Props azkProps;

  @Inject
  ExecutionController(final Props azkProps, final ExecutorLoader executorLoader,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway, final AlerterHolder alerterHolder, final
  ExecutorHealthChecker executorHealthChecker) {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
    this.commonMetrics = commonMetrics;
    this.apiGateway = apiGateway;
    this.alerterHolder = alerterHolder;
    this.executorHealthChecker = executorHealthChecker;
    this.maxConcurrentRunsOneFlow = ExecutorUtils.getMaxConcurrentRunsOneFlow(azkProps);
    this.maxConcurrentRunsPerFlowMap = ExecutorUtils.getMaxConcurentRunsPerFlowMap(azkProps);
  }

  @Override
  public void setupExecutors() throws ExecutorManagerException {
    // Todo: deprecate this method
  }

  @Override
  public void disableQueueProcessorThread() {
    // Todo: deprecate this method
  }

  @Override
  public void enableQueueProcessorThread() {
    // Todo: deprecate this method
  }

  @Override
  public State getExecutorManagerThreadState() {
    // Todo: deprecate this method
    return State.RUNNABLE;
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    // Todo: deprecate this method
    return true;
  }

  @Override
  public long getLastExecutorManagerThreadCheckTime() {
    // Todo: deprecate this method
    return 1L;
  }

  @Override
  public Collection<Executor> getAllActiveExecutors() {
    List<Executor> executors = new ArrayList<>();
    try {
      executors = this.executorLoader.fetchActiveExecutors();
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get all active executors.", e);
    }
    return executors;
  }

  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    return this.executorLoader.fetchExecutor(executorId);
  }

  @Override
  public Set<String> getPrimaryServerHosts() {
    final HashSet<String> ports = new HashSet<>();
    try {
      for (final Executor executor : this.executorLoader.fetchActiveExecutors()) {
        ports.add(executor.getHost() + ":" + executor.getPort());
      }
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get primary server hosts.", e);
    }
    return ports;
  }

  @Override
  public Set<String> getAllActiveExecutorServerHosts() {
    final Set<String> ports = getPrimaryServerHosts();
    // include executor which were initially active and still has flows running
    try {
      for (final Pair<ExecutionReference, ExecutableFlow> running : this.executorLoader
          .fetchActiveFlows().values()) {
        final ExecutionReference ref = running.getFirst();
        if (ref.getExecutor().isPresent()) {
          final Executor executor = ref.getExecutor().get();
          ports.add(executor.getHost() + ":" + executor.getPort());
        }
      }
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get all active executor server hosts.", e);
    }
    return ports;
  }

  /**
   * Gets a list of all the unfinished (both dispatched and non-dispatched) executions for a
   * given project and flow {@inheritDoc}.
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows(int, java.lang.String)
   */
  @Override
  public List<Integer> getRunningFlows(final int projectId, final String flowId) {
    final List<Integer> executionIds = new ArrayList<>();
    try {
      executionIds.addAll(getRunningFlowsHelper(projectId, flowId,
          this.executorLoader.fetchUnfinishedFlows().values()));
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get running flows for project " + projectId + ", flow "
          + flowId, e);
    }
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

  @Override
  public List<Pair<ExecutableFlow, Optional<Executor>>> getActiveFlowsWithExecutor()
      throws IOException {
    final List<Pair<ExecutableFlow, Optional<Executor>>> flows = new ArrayList<>();
    try {
      getActiveFlowsWithExecutorHelper(flows, this.executorLoader.fetchUnfinishedFlows().values());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get active flows with executor.", e);
    }
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
   * Checks whether the given flow has an active (running, non-dispatched) execution from
   * database. {@inheritDoc}
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
   * Fetch ExecutableFlow from database. {@inheritDoc}
   */
  @Override
  public ExecutableFlow getExecutableFlow(final int execId)
      throws ExecutorManagerException {
    return this.executorLoader.fetchExecutableFlow(execId);
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

  /**
   * Helper method to get all flows from collection.
   */
  private void getFlowsHelper(final ArrayList<ExecutableFlow> flows,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    collection.stream().forEach(ref -> flows.add(ref.getSecond()));
  }

  /**
   * Get execution ids of all running (unfinished) flows from database.
   */
  public List<Integer> getRunningFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    try {
      getExecutionIdsHelper(allIds, this.executorLoader.fetchUnfinishedFlows().values());
    } catch (final ExecutorManagerException e) {
      this.logger.error("Failed to get running flow ids.", e);
    }
    return allIds;
  }

  /**
   * Get execution ids of all non-dispatched flows from database.
   */
  public List<Integer> getQueuedFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    try {
      getExecutionIdsHelper(allIds, this.executorLoader.fetchQueuedFlows());
    } catch (final ExecutorManagerException e) {
      this.logger.error("Failed to get queued flow ids.", e);
    }
    return allIds;
  }

  /* Helper method to get all execution ids from collection in sorted order. */
  private void getExecutionIdsHelper(final List<Integer> allIds,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    collection.stream().forEach(ref -> allIds.add(ref.getSecond().getExecutionId()));
    Collections.sort(allIds);
  }

  /**
   * Get the number of non-dispatched flows from database. {@inheritDoc}
   */
  @Override
  public long getQueuedFlowSize() {
    long size = 0L;
    try {
      size = this.executorLoader.fetchQueuedFlows().size();
    } catch (final ExecutorManagerException e) {
      this.logger.error("Failed to get queued flow size.", e);
    }
    return size;
  }

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
    final Pair<ExecutionReference, ExecutableFlow> pair = this.executorLoader
        .fetchActiveFlowByExecId(exFlow.getExecutionId());
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
    final Pair<ExecutionReference, ExecutableFlow> pair = this.executorLoader
        .fetchActiveFlowByExecId(exFlow.getExecutionId());
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
        this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
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
   * If the resource manager and job history server urls are configured, fetch the application
   * id from the job log and then construct the job link url.
   *
   * @param exFlow The executable flow.
   * @param jobId The job id.
   * @param attempt The job execution attempt.
   * @return the job link url.
   */
  @Override
  public String getJobLinkUrl(final ExecutableFlow exFlow, final String jobId, final int attempt) {
    if (!this.azkProps.containsKey(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL) || !this.azkProps
        .containsKey(ConfigurationKeys.HISTORY_SERVER_JOB_URL) || !this.azkProps
        .containsKey(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL)) {
      return null;
    }
    final String applicationId = getApplicationId(exFlow, jobId, attempt);
    return ExecutionControllerUtils.createJobLinkUrl(exFlow, jobId, applicationId, this.azkProps);
  }

  /**
   * Get the Hadoop/Spark application id from the job log.
   *
   * @param exFlow The executable flow.
   * @param jobId The job id.
   * @param attempt The job execution attempt.
   * @return the application id.
   */
  String getApplicationId(final ExecutableFlow exFlow, final String jobId, final int attempt) {
    String applicationId;
    boolean finished = false;
    int offset = 0;
    try {
      while (!finished) {
        final LogData data = getExecutionJobLog(exFlow, jobId, offset, 50000, attempt);
        if (data != null && data.getLength() != 0) {
          applicationId = ExecutionControllerUtils.findApplicationIdFromLog(data.getData());
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

  @Override
  public void resumeFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
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
          this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
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
          this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
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

      String message = "";

      final int projectId = exflow.getProjectId();
      exflow.setSubmitUser(userId);
      exflow.setStatus(Status.PREPARING);
      exflow.setSubmitTime(System.currentTimeMillis());

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

      this.commonMetrics.markSubmitFlowSuccess();
      message += "Execution queued successfully with exec id " + exflow.getExecutionId();
      return message;
    }
  }

  @Override
  public Map<String, Object> callExecutorStats(final int executorId, final String action,
      final Pair<String, String>... params) throws IOException, ExecutorManagerException {
    final Executor executor = fetchExecutor(executorId);
    final List<Pair<String, String>> paramList = new ArrayList<>();

    if (params != null) {
      paramList.addAll(Arrays.asList(params));
    }

    paramList.add(new Pair<>(ConnectorParams.ACTION_PARAM, action));

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
  public void start() {
    this.executorHealthChecker.start();
  }

  @Override
  public void shutdown() {
    this.executorHealthChecker.shutdown();
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

}
