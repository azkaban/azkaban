/*
 * Copyright 2018 LinkedIn Corp.
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

import static azkaban.executor.ExecutableFlow.DEFAULT_SYSTEM_FLOW_RETRY_LIMIT;
import static azkaban.executor.Status.RESTARTABLE_NON_TERMINAL_STATUSES;
import static azkaban.executor.Status.RESTARTABLE_TERMINAL_STATUSES;
import static azkaban.executor.Status.StatusBeforeRunningSet;
import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.FlowParameters;
import azkaban.alert.Alerter;
import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventHandler;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.spi.EventType;
import azkaban.utils.AuthenticationUtils;
import azkaban.utils.Props;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils for controlling executions.
 */
public class ExecutionControllerUtils {

  private static final Logger logger = LoggerFactory.getLogger(
      ExecutionControllerUtils.class);
  private static final String SPARK_JOB_TYPE = "spark";
  static final String OLD_APPLICATION_ID = "${application.id}";
  // URLs coming from routing cluster info cannot use `${}` as a placeholder because it is already
  // used for property substitution in Props class, which URLs are propagated through.
  static final String NEW_APPLICATION_ID = "<application.id>";
  // The regex to look for while fetching application ID from the Hadoop/Spark job log
  private static final Pattern APPLICATION_ID_PATTERN = Pattern.compile("application_(\\d+_\\d+)");
  // The regex to look for while validating the content from RM job link
  private static final Pattern FAILED_TO_READ_APPLICATION_PATTERN = Pattern
      .compile("Failed to read the application");
  private static final Pattern INVALID_APPLICATION_ID_PATTERN = Pattern
      .compile("Invalid Application ID");

  public static OnExecutionEventListener onExecutionEventListener;
  public static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("azk-restart-flow").build());

  /**
   * If the current status of the execution is not one of the finished statuses, mark the execution
   * as failed in the DB.
   *
   * @param executorLoader  the executor loader
   * @param alerterHolder   the alerter holder
   * @param flow            the execution
   * @param reason          reason for finalizing the execution
   * @param originalError   the cause, if execution is being finalized because of an error
   * @param finalFlowStatus Status to be set if the flow is not in one of the "finished" statues.
   */
  @Deprecated
  public static void finalizeFlow(final ExecutorLoader executorLoader,
      @Nullable final AlerterHolder
          alerterHolder, final ExecutableFlow flow, final String reason,
      @Nullable final Throwable originalError, final Status finalFlowStatus) {
    finalizeFlow(null, null, executorLoader, alerterHolder, flow, reason, originalError,
        finalFlowStatus);
  }

  /**
   * If the current status of the execution is not one of the finished statuses, mark the execution
   * as failed in the DB.
   *
   * @param eventHandler    the event handler to fire job events
   * @param projectManager  the project manager to load job props from DB
   * @param executorLoader  the executor loader
   * @param alerterHolder   the alerter holder
   * @param flow            the execution
   * @param reason          reason for finalizing the execution
   * @param originalError   the cause, if execution is being finalized because of an error
   * @param finalFlowStatus Status to be set if the flow is not in one of the "finished" statues.
   */
  public static void finalizeFlow(final EventHandler eventHandler,
      final ProjectManager projectManager,
      final ExecutorLoader executorLoader,
      @Nullable final AlerterHolder
          alerterHolder, final ExecutableFlow flow, final String reason,
      @Nullable final Throwable originalError, final Status finalFlowStatus) {
    boolean alertUser = true;

    // First check if the execution in the datastore is finished.
    try {
      final ExecutableFlow dsFlow;
      if (isFinished(flow)) {
        dsFlow = flow;
      } else {
        dsFlow = executorLoader.fetchExecutableFlow(flow.getExecutionId());

        // If it's marked finished, we're good. If not, we fail everything and then mark it
        // finished.
        if (!isFinished(dsFlow)) {
          failEverything(eventHandler, projectManager, dsFlow, finalFlowStatus, executorLoader);
          executorLoader.updateExecutableFlow(dsFlow);
        }
        // flow will be used for event reporter afterwards, thus the final status needs to be set
        flow.setStatus(finalFlowStatus);
      }

      if (flow.getUpdateTime() == -1) {
        flow.setUpdateTime(System.currentTimeMillis());
        executorLoader.updateExecutableFlow(dsFlow);
      }

      if (flow.getStartTime() == -1) {
        flow.setStartTime(System.currentTimeMillis());
        executorLoader.updateExecutableFlow(dsFlow);
      }

      if (flow.getEndTime() == -1) {
        flow.setEndTime(System.currentTimeMillis());
        executorLoader.updateExecutableFlow(dsFlow);
      }
    } catch (final ExecutorManagerException e) {
      // If failed due to azkaban internal error, do not alert user.
      alertUser = false;
      logger.error("Failed to finalize flow " + flow.getExecutionId() + ", do not alert user.", e);
    }

    if (null != alerterHolder && alertUser) {
      alertUserOnFlowFinished(flow, alerterHolder, getFinalizeFlowReasons(reason, originalError));
    }
  }

  /**
   * This method tries to determine whether the flow should be restarted for certain statuses
   * otherwise simply return.
   * There are 4 scenarios that this method is called:
   * 1. a flow is cleaned up by ContainerCleanupManager;
   * 2. a flow has dispatch failure;
   * 3. a flow encounters pod failure;
   * 4. a flow finalized normally.
   * If the original status is EXECUTION_STOPPED or FAILED, then it will be retried only if
   * "flow.retry.statuses" is defined with the status.
   *
   * @param flow
   * @param originalStatus
   */
  static ExecutableFlow getFlowToRestart(final ExecutableFlow flow,
      final Status originalStatus){
    final ExecutionOptions options = flow.getExecutionOptions();
    if (options == null) {
      logger.debug("ExecutableFlow: " + flow.getExecutionId() + " has ExecutionOptions == null");
      return null;
    }
    // If the original execution status is restartable non-terminal status, it can be retried
    // if the system-retry-times hasn't reached limit
    if (RESTARTABLE_NON_TERMINAL_STATUSES.contains(originalStatus)) {
      if (flow.getSystemDefinedRetryCount() >= DEFAULT_SYSTEM_FLOW_RETRY_LIMIT){
        logger.info("ExecutableFlow: " + flow.getExecutionId() + " has reached max retry limit "
            + "for non-terminal status, ");
        return null;
      }
      logger.info("Submitting flow for restart: " + flow.getExecutionId());
      flow.setSystemDefinedRetryCount(flow.getSystemDefinedRetryCount() + 1);
      return flow;
    }

    if (!RESTARTABLE_TERMINAL_STATUSES.contains(originalStatus)) {
      return null;
    }

    // if the original execution status is restartable but is terminal status, we will need to
    // check the runtime properties (flow parameters):
    // - If the original execution status is EXECUTION_STOPPED (which indicates the program
    // detects there's an invalid pod state transition and flow is terminated before final states)
    // - Or some other status defined in flow parameters can also be restarted
    final Map<String, String> flowParams = options.getFlowParameters();
    if (flowParams == null || flowParams.isEmpty()) {
      logger.debug("ExecutableFlow: " + flow.getExecutionId() + " has ExecutionOptions == null");
      return null;
    }

    // user defined restart statuses list
    final Set<String> restartedStatuses = Arrays.stream(
        flowParams.getOrDefault(FlowParameters.FLOW_PARAM_ALLOWED_RETRY_STATUS, "")
            .split("\\s*,\\s*")
    ).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());

    // backwards compatible to flows that historically defined with
    // "allow.restart.on.execution.stopped"
    if (Boolean.parseBoolean(flowParams.getOrDefault(
        FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED, "false"))){
      restartedStatuses.add(Status.EXECUTION_STOPPED.name());
    }

    // flow can only retry if custom-retry-times not reach the limit
    // and for the case where the retry-times not defined, default to 1
    int flowMaxRetryLimit = 0;
    if (flowParams.containsKey(FlowParameters.FLOW_PARAM_MAX_RETRIES)){
      flowMaxRetryLimit = Integer.parseInt(flowParams.get(FlowParameters.FLOW_PARAM_MAX_RETRIES));
    }
    else if (!restartedStatuses.isEmpty()){
      logger.info(String.format("ExecutableFlow: %s has `%s=%s but `%s` not set, "
              + "default to retry once",
          flow.getExecutionId(),
          FlowParameters.FLOW_PARAM_ALLOWED_RETRY_STATUS,
          restartedStatuses,
          FlowParameters.FLOW_PARAM_MAX_RETRIES
          ));
      flowMaxRetryLimit = 1;
    }
    if (flow.getUserDefinedRetryCount() >= flowMaxRetryLimit) {
      logger.info("ExecutableFlow: " + flow.getExecutionId() + " has reached max retry limit, "
          + "retried=" + flow.getUserDefinedRetryCount() + ", limit= " + flowMaxRetryLimit);
      return null;
    }

    if (restartedStatuses.contains(originalStatus.name())) {
      logger.info("Submitting flow for restart: " + flow.getExecutionId()
          + " from originalStatus: " + originalStatus);
      flow.setUserDefinedRetryCount(flow.getUserDefinedRetryCount() + 1);
      return flow;
    }
    return null;
  }

  public static void restartFlow(final ExecutableFlow flow, final Status originalStatus) {
    ExecutableFlow flowToRestart = getFlowToRestart(flow, originalStatus);
    if (flowToRestart != null){
      ExecutionControllerUtils.submitRestartFlow(flowToRestart);
    }
  }

  private static void submitRestartFlow(final ExecutableFlow flow) {
    EXECUTOR_SERVICE.execute(() -> restartFlow(flow));
  }

  /**
   * This method invokes a callback to submit the flow with a new execution id
   *
   * @param flow
   */
  protected static void restartFlow(final ExecutableFlow flow) {
    onExecutionEventListener.onExecutionEvent(flow, Constants.RESTART_FLOW);
  }

  /**
   * When a flow is finished, alert the user as is configured in the execution options.
   *
   * @param flow          the execution
   * @param alerterHolder the alerter holder
   * @param extraReasons  the extra reasons for alerting
   */
  public static void alertUserOnFlowFinished(final ExecutableFlow flow,
      final AlerterHolder alerterHolder, final String[] extraReasons) {
    if (flow.getStatus() != Status.SUCCEEDED) {
      alerterHolder.forEach((String alerterName, Alerter alerter) -> {
        try {
          alerter.alertOnError(flow, extraReasons);
        } catch (final Exception e) {
          logger.error("Failed to alert on error for execution " + flow.getExecutionId(), e);
        }
      });
    } else {
      alerterHolder.forEach((String alerterName, Alerter alerter) -> {
        try {
          alerter.alertOnSuccess(flow);
        } catch (final Exception e) {
          logger.error("Failed to alert on success for execution " + flow.getExecutionId(), e);
        }
      });
    }
  }

  /**
   * Alert the user when the flow has encountered the first error.
   *
   * @param flow          the execution
   * @param alerterHolder the alerter holder
   */
  public static void alertUserOnFirstError(final ExecutableFlow flow,
      final AlerterHolder alerterHolder) {
    final ExecutionOptions options = flow.getExecutionOptions();
    if (options.getNotifyOnFirstFailure()) {
      logger.info("Alert on first error of execution " + flow.getExecutionId());
      alerterHolder.forEach((String alerterName, Alerter alerter) -> {
        try {
          alerter.alertOnFirstError(flow);
        } catch (final Exception e) {
          logger.error("Failed to send first error email." + e.getMessage(), e);
        }
      });
    }
  }

  /**
   * Alert the user when job property is overridden in a project
   *
   * @param project
   * @param flow
   * @param eventData
   */
  public static void alertUserOnJobPropertyOverridden(final Project project, final Flow flow,
      final Map<String, Object> eventData, final AlerterHolder alerterHolder) {
    if (!flow.getOverrideEmails().isEmpty()) {
      logger.info("Alert on job property overridden event in project: " + project.getName());
      final Alerter mailAlerter = alerterHolder.get("email");
      try {
        mailAlerter.alertOnJobPropertyOverridden(project, flow, eventData);
      } catch (final Exception e) {
        logger.error("Failed to send email alert." + e.getMessage(), e);
      }
    }
  }

  /**
   * Get the reasons to finalize the flow.
   *
   * @param reason        the reason
   * @param originalError the original error
   * @return the reasons to finalize the flow
   */
  public static String[] getFinalizeFlowReasons(final String reason, final Throwable
      originalError) {
    final List<String> reasons = new LinkedList<>();
    reasons.add(reason);
    if (originalError != null) {
      reasons.add(ExceptionUtils.getStackTrace(originalError));
    }
    return reasons.toArray(new String[reasons.size()]);
  }

  /**
   * Set the flow status to failed and fail every node inside the flow.
   *
   * @param exFlow          the executable flow
   * @param finalFlowStatus Status to be set if the flow is not in one of the "finished" statues.
   */
  @Deprecated
  public static void failEverything(final ExecutableFlow exFlow,
      final Status finalFlowStatus, final ExecutorLoader executorLoader) {
    failEverything(null, null, exFlow, finalFlowStatus, executorLoader);
  }

  /**
   * Set the flow status to failed and fail every node inside the flow.
   *
   * @param eventHandler    the event handler to fire job events
   * @param projectManager  the project manager to load job props from DB
   * @param exFlow          the executable flow
   * @param finalFlowStatus Status to be set if the flow is not in one of the "finished" statues.
   */
  public static void failEverything(final EventHandler eventHandler,
      final ProjectManager projectManager, final ExecutableFlow exFlow,
      final Status finalFlowStatus, final ExecutorLoader executorLoader) {
    final long time = System.currentTimeMillis();
    final Map<ExecutableNode, Status> nodeToOrigStatus = new HashMap<>();
    final Queue<ExecutableNode> queue = new LinkedList<>();
    queue.add(exFlow);
    // Traverse the DAG and fail every node that's not in a terminal state
    while (!queue.isEmpty()) {
      final ExecutableNode node = queue.poll();
      if (node instanceof ExecutableFlowBase) {
        final ExecutableFlowBase base = (ExecutableFlowBase) node;
        for (final ExecutableNode subNode : base.getExecutableNodes()) {
          queue.add(subNode);
        }
      }
      if (node != exFlow) {
        switch (node.getStatus()) {
          case SUCCEEDED:
          case FAILED:
          case KILLED:
          case SKIPPED:
          case DISABLED:
            continue;
            // case UNKNOWN:
          case READY:
            nodeToOrigStatus.put(node, node.getStatus());
            if (finalFlowStatus == Status.KILLED) {
              // if the finalFlowStatus is KILLED, then set the sub node status to KILLED.
              node.setStatus(Status.KILLED);
            } else if (finalFlowStatus == Status.EXECUTION_STOPPED) {
              // if flow status is EXECUTION_STOPPED due to e.g. pod failure, set sub node to
              // KILLED.
              node.setStatus(Status.KILLED);
            } else {
              node.setStatus(Status.KILLING);
            }
            break;
          default:
            nodeToOrigStatus.put(node, node.getStatus());
            // Set the default job/node status as the finalFlowStatus
            node.setStatus(finalFlowStatus);
            break;
        }

        if (node.getStartTime() == -1) {
          node.setStartTime(time);
        }
        if (node.getEndTime() == -1) {
          node.setEndTime(time);
        }
        try {
          executorLoader.updateExecutableNode(node);
        } catch (ExecutorManagerException e) {
          logger.error("fail to update job status in DB, ", e);
        }
      }
    }

    // Fire correct JOB_STARTED and JOB_FINISHED events.
    if (eventHandler != null && projectManager != null) {
      final Project project = projectManager.getProject(exFlow.getProjectId());
      for (final Entry<ExecutableNode, Status> entry : nodeToOrigStatus.entrySet()) {
        final ExecutableNode node = entry.getKey();
        final Status origStatus = entry.getValue();
        // We shouldn't use project.getFlow(exFlow.getFlowId()) b/c the node might be in embedded
        // flows and exFlow might be the ancestor flow but not node's direct parent flow.
        final Flow flow = project.getFlow(node.getParentFlow().getFlowId());

        // Fill in job props by following the logic in ProjectManagerServlet.
        if (node.getInputProps() == null) {
          node.setInputProps(projectManager.getJobOverrideProperty(project, flow, node.getId(),
              node.getJobSource()));
        }
        if (node.getInputProps() == null) {
          node.setInputProps(projectManager.getProperties(project, flow, node.getId(),
              node.getJobSource()));
        }

        if (StatusBeforeRunningSet.contains(origStatus)) {
          final Status finalStatus = node.getStatus();
          node.setStatus(origStatus);
          eventHandler.fireEventListeners(Event.create(new JobRunnerBase(node.getInputProps()),
              EventType.JOB_STARTED, new EventData(node)));
          node.setStatus(finalStatus);
        }

        if (Status.isStatusFinished(node.getStatus())) {
          eventHandler.fireEventListeners(Event.create(new JobRunnerBase(node.getInputProps()),
              EventType.JOB_FINISHED, new EventData(node)));
        }
      }
    }

    if (exFlow.getUpdateTime() == -1) {
      exFlow.setUpdateTime(time);
    }

    if (exFlow.getStartTime() == -1) {
      exFlow.setStartTime(time);
    }

    if (exFlow.getEndTime() == -1) {
      exFlow.setEndTime(time);
    }

    if (!Status.isStatusFinished(exFlow.getStatus())) {
      exFlow.setStatus(finalFlowStatus);
    }
  }

  /**
   * Check if the flow status is finished.
   *
   * @param flow the executable flow
   * @return the boolean
   */
  public static boolean isFinished(final ExecutableFlow flow) {
    switch (flow.getStatus()) {
      case EXECUTION_STOPPED:
      case SUCCEEDED:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
    }
  }

  /**
   * Dynamically create the job link url. Construct the job link url from resource manager url. If
   * it's valid, just return the job link url. Otherwise, construct the job link url from
   * Hadoop/Spark job history server.
   *
   * @param exFlow        The executable flow.
   * @param jobId         The job id.
   * @param applicationId The application id.
   * @param azkProps      The azkaban props.
   * @return the job link url.
   */
  public static String createJobLinkUrl(final ExecutableFlow exFlow, final String jobId,
      final String applicationId, final Props azkProps) {
    if (applicationId == null) {
      return null;
    }

    final ExecutableNode node = exFlow.getExecutableNodePath(jobId);
    final boolean executableNodeFound = (node != null) ? true : false;

    String resourceManagerJobUrl = null;
    String sparkHistoryServerUrl = null;
    String jobHistoryServerUrl = null;
    final String applicationPlaceholder;

    if (executableNodeFound && node.getClusterInfo() != null) {
      // use the information of the cluster where the job is previously routed to
      final ClusterInfo cluster = node.getClusterInfo();
      applicationPlaceholder = NEW_APPLICATION_ID;
      resourceManagerJobUrl = cluster.resourceManagerURL;
      sparkHistoryServerUrl = cluster.sparkHistoryServerURL;
      jobHistoryServerUrl = cluster.historyServerURL;
    } else {
      // fall back to web server's own configuration if cluster is missing for this job
      applicationPlaceholder = OLD_APPLICATION_ID;
      if (azkProps.containsKey(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL)) {
        resourceManagerJobUrl = azkProps.getString(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL);
      }
      if (azkProps.containsKey(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL)) {
        sparkHistoryServerUrl = azkProps.getString(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL);
      }
      if (azkProps.containsKey(ConfigurationKeys.HISTORY_SERVER_JOB_URL)) {
        jobHistoryServerUrl = azkProps.getString(ConfigurationKeys.HISTORY_SERVER_JOB_URL);
      }
    }

    if (resourceManagerJobUrl == null || sparkHistoryServerUrl == null
        || jobHistoryServerUrl == null) {
      logger.info("Missing Resource Manager, Spark History Server or History Server URL");
      return null;
    }

    final URL url;
    final String jobLinkUrl;
    boolean isRMJobLinkValid = true;

    try {
      url = new URL(resourceManagerJobUrl.replace(applicationPlaceholder, applicationId));
      final String keytabPrincipal = requireNonNull(
          azkProps.getString(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL));
      final String keytabPath = requireNonNull(azkProps.getString(ConfigurationKeys
          .AZKABAN_KEYTAB_PATH));
      final HttpURLConnection connection = AuthenticationUtils.loginAuthenticatedURL(url,
          keytabPrincipal, keytabPath);

      try (final BufferedReader in = new BufferedReader(
          new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          if (FAILED_TO_READ_APPLICATION_PATTERN.matcher(inputLine).find()) {
            logger.info("RM job link has expired for application_" + applicationId);
            isRMJobLinkValid = false;
            break;
          }
          if (INVALID_APPLICATION_ID_PATTERN.matcher(inputLine).find()) {
            logger.info("Invalid application id application_" + applicationId);
            return null;
          }
        }
      }
    } catch (final Exception e) {
      logger.error("Failed to get job link for application_" + applicationId, e);
      return null;
    }

    if (isRMJobLinkValid) {
      jobLinkUrl = url.toString();
    } else {
      // If RM job url has expired, build the url to the JHS or SHS instead.
      if (!executableNodeFound) {
        logger.error(
            "Failed to create job url. Job " + jobId + " doesn't exist in " + exFlow
                .getExecutionId());
        return null;
      }
      if (node.getType().equals(SPARK_JOB_TYPE)) {
        jobLinkUrl = sparkHistoryServerUrl.replace(applicationPlaceholder, applicationId);
      } else {
        jobLinkUrl = jobHistoryServerUrl.replace(applicationPlaceholder, applicationId);
      }
    }

    logger.info("Job link url is " + jobLinkUrl + " for execution " + exFlow.getExecutionId() +
        ", job " + jobId);
    return jobLinkUrl;
  }

  /**
   * Find all the application ids the job log data contains by matching "application_<id>" pattern.
   * Application ids are returned in the order they appear.
   *
   * @param logData The log data.
   * @return The set of application ids found.
   */
  public static Set<String> findApplicationIdsFromLog(final String logData) {
    final Set<String> applicationIds = new LinkedHashSet<>();
    final Matcher matcher = APPLICATION_ID_PATTERN.matcher(logData);

    while (matcher.find()) {
      final String appId = matcher.group(1);
      applicationIds.add(appId);
    }

    logger.info("Application Ids found: " + applicationIds.toString());
    return applicationIds;
  }

  /**
   * Create a string by combining the cluster name with the execution Id.
   *
   * @param clusterName name of the azkaban cluster
   * @param executionId execution id of a flow
   * @return
   */
  public static String clusterQualifiedExecId(final String clusterName, final int executionId) {
    requireNonNull(clusterName, "cluster name must not be null");
    return String.format("%s-%d", clusterName, executionId);
  }
}
