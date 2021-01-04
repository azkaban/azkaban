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

import static java.util.Objects.requireNonNull;

import azkaban.Constants.ConfigurationKeys;
import azkaban.alert.Alerter;
import azkaban.utils.AuthenticationUtils;
import azkaban.utils.Props;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  /**
   * If the current status of the execution is not one of the finished statuses, mark the execution
   * as failed in the DB.
   *
   * @param executorLoader the executor loader
   * @param alerterHolder the alerter holder
   * @param flow the execution
   * @param reason reason for finalizing the execution
   * @param originalError the cause, if execution is being finalized because of an error
   */
  public static void finalizeFlow(final ExecutorLoader executorLoader, final AlerterHolder
      alerterHolder, final ExecutableFlow flow, final String reason,
      @Nullable final Throwable originalError) {
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
          failEverything(dsFlow);
          executorLoader.updateExecutableFlow(dsFlow);
        }
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

    if (alertUser) {
      alertUserOnFlowFinished(flow, alerterHolder, getFinalizeFlowReasons(reason, originalError));
    }
  }

  /**
   * When a flow is finished, alert the user as is configured in the execution options.
   *
   * @param flow the execution
   * @param alerterHolder the alerter holder
   * @param extraReasons the extra reasons for alerting
   */
  public static void alertUserOnFlowFinished(final ExecutableFlow flow, final AlerterHolder
      alerterHolder, final String[] extraReasons) {
    final ExecutionOptions options = flow.getExecutionOptions();
    final Alerter mailAlerter = alerterHolder.get("email");
    if (flow.getStatus() != Status.SUCCEEDED) {
      if (options.getFailureEmails() != null && !options.getFailureEmails().isEmpty()) {
        try {
          mailAlerter.alertOnError(flow, extraReasons);
        } catch (final Exception e) {
          logger.error("Failed to alert on error for execution " + flow.getExecutionId(), e);
        }
      }
      if (options.getFlowParameters().containsKey("alert.type")) {
        final String alertType = options.getFlowParameters().get("alert.type");
        final Alerter alerter = alerterHolder.get(alertType);
        if (alerter != null) {
          try {
            alerter.alertOnError(flow, extraReasons);
          } catch (final Exception e) {
            logger.error("Failed to alert on error by " + alertType + " for execution " + flow
                .getExecutionId(), e);
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
          logger.error("Failed to alert on success for execution " + flow.getExecutionId(), e);
        }
      }
      if (options.getFlowParameters().containsKey("alert.type")) {
        final String alertType = options.getFlowParameters().get("alert.type");
        final Alerter alerter = alerterHolder.get(alertType);
        if (alerter != null) {
          try {
            alerter.alertOnSuccess(flow);
          } catch (final Exception e) {
            logger.error("Failed to alert on success by " + alertType + " for execution " + flow
                .getExecutionId(), e);
          }
        } else {
          logger.error("Alerter type " + alertType + " doesn't exist. Failed to alert.");
        }
      }
    }
  }

  /**
   * Alert the user when the flow has encountered the first error.
   *
   * @param flow the execution
   * @param alerterHolder the alerter holder
   */
  public static void alertUserOnFirstError(final ExecutableFlow flow,
      final AlerterHolder alerterHolder) {
    final ExecutionOptions options = flow.getExecutionOptions();
    if (options.getNotifyOnFirstFailure()) {
      logger.info("Alert on first error of execution " + flow.getExecutionId());
      final Alerter mailAlerter = alerterHolder.get("email");
      try {
        mailAlerter.alertOnFirstError(flow);
      } catch (final Exception e) {
        logger.error("Failed to send first error email." + e.getMessage(), e);
      }

      if (options.getFlowParameters().containsKey("alert.type")) {
        final String alertType = options.getFlowParameters().get("alert.type");
        final Alerter alerter = alerterHolder.get(alertType);
        if (alerter != null) {
          try {
            alerter.alertOnFirstError(flow);
          } catch (final Exception e) {
            logger.error("Failed to alert by " + alertType, e);
          }
        } else {
          logger.error("Alerter type " + alertType + " doesn't exist. Failed to alert.");
        }
      }
    }
  }

  /**
   * Get the reasons to finalize the flow.
   *
   * @param reason the reason
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
   * @param exFlow the executable flow
   */
  public static void failEverything(final ExecutableFlow exFlow) {
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

  /**
   * Check if the flow status is finished.
   *
   * @param flow the executable flow
   * @return the boolean
   */
  public static boolean isFinished(final ExecutableFlow flow) {
    switch (flow.getStatus()) {
      case SUCCEEDED:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
    }
  }

  /**
   * Dynamically create the job link url. Construct the job link url from resource manager url.
   * If it's valid, just return the job link url. Otherwise, construct the job link url from
   * Hadoop/Spark job history server.
   *
   * @param exFlow The executable flow.
   * @param jobId The job id.
   * @param applicationId The application id.
   * @param azkProps The azkaban props.
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

    if (resourceManagerJobUrl == null || sparkHistoryServerUrl == null || jobHistoryServerUrl == null) {
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
}
