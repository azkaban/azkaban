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

import azkaban.alert.Alerter;
import java.util.LinkedList;
import java.util.List;
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
      alertUser(flow, alerterHolder, getFinalizeFlowReasons(reason, originalError));
    }
  }

  /**
   * When a flow is finished, alert the user as is configured in the execution options.
   *
   * @param flow the execution
   * @param alerterHolder the alerter holder
   * @param extraReasons the extra reasons for alerting
   */
  public static void alertUser(final ExecutableFlow flow, final AlerterHolder alerterHolder,
      final String[] extraReasons) {
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
}
