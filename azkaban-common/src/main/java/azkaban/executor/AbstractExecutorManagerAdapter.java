package azkaban.executor;


import azkaban.Constants;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractExecutorManagerAdapter extends EventHandler implements
    ExecutorManagerAdapter {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractExecutorManagerAdapter.class);
  protected final Props azkProps;
  protected final ExecutorLoader executorLoader;
  protected final CommonMetrics commonMetrics;
  protected final ExecutorApiGateway apiGateway;
  private final int maxConcurrentRunsOneFlow;
  private final Map<Pair<String, String>, Integer> maxConcurrentRunsPerFlowMap;
  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(10);

  protected AbstractExecutorManagerAdapter(final Props azkProps,
      final ExecutorLoader executorLoader,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway) {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
    this.commonMetrics = commonMetrics;
    this.apiGateway = apiGateway;
    this.maxConcurrentRunsOneFlow = ExecutorUtils.getMaxConcurrentRunsOneFlow(azkProps);
    this.maxConcurrentRunsPerFlowMap = ExecutorUtils.getMaxConcurentRunsPerFlowMap(azkProps);
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

  @Override
  public long getAgedQueuedFlowSize() {
    long size = 0L;
    int minimum_age_minutes = this.azkProps.getInt(
        ConfigurationKeys.MIN_AGE_FOR_CLASSIFYING_A_FLOW_AGED_MINUTES,
        Constants.DEFAULT_MIN_AGE_FOR_CLASSIFYING_A_FLOW_AGED_MINUTES);

    // TODO(anish-mal) FetchQueuedExecutableFlows does a lot of processing that is redundant, since
    // all we care about is the count. Write a new class that's more performant and can be used for
    // metrics. this.executorLoader.fetchAgedQueuedFlows internally calls FetchQueuedExecutableFlows.
    try {
      size = this.executorLoader.fetchAgedQueuedFlows(Duration.ofMinutes(minimum_age_minutes))
          .size();
    } catch (final ExecutorManagerException e) {
      this.logger.error("Failed to get flows queued for a long time.", e);
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
  public Map<String, String> doRampActions(List<Map<String, Object>> rampActions)
      throws ExecutorManagerException {
    return this.executorLoader.doRampActions(rampActions);
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

      this.commonMetrics.markSubmitFlowSuccess();
      message += "Execution queued successfully with exec id " + exflow.getExecutionId();
      return message;
    }
  }

  protected String uploadExecutableFlow(ExecutableFlow exflow, String userId, String flowId,
      String message) throws ExecutorManagerException {
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
    return message;
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

  protected LogData getFlowLogData(ExecutableFlow exFlow, int offset, int length,
      Pair<ExecutionReference, ExecutableFlow> pair) throws ExecutorManagerException {
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

  protected LogData getJobLogData(ExecutableFlow exFlow, String jobId, int offset, int length,
      int attempt, Pair<ExecutionReference, ExecutableFlow> pair) throws ExecutorManagerException {
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

  protected List<Object> getExecutionJobStats(ExecutableFlow exFlow, String jobId, int attempt,
      Pair<ExecutionReference, ExecutableFlow> pair) throws ExecutorManagerException {
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

  protected Map<String, Object> modifyExecutingJobs(ExecutableFlow exFlow, String command,
      String userId, Pair<ExecutionReference, ExecutableFlow> pair, String[] jobIds)
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

  /* Helper method for getRunningFlows */
  protected List<Integer> getRunningFlowsHelper(final int projectId, final String flowId,
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
}
