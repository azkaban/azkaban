/*
 * Copyright 2022 LinkedIn Corp.
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

package azkaban.scheduler;

import azkaban.Constants;
import azkaban.metrics.MetricsManager;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.spi.AzkabanException;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.DaemonThreadFactory;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import azkaban.utils.TimeUtils;
import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Miss Schedule Manager is a failure recover manager for schedules. Schedule might be missed to execute due to
 * Trigger Scanner thread busy or web server down. This manager maintains a configurable fixed sized thread pool
 * to execute tasks including send email notification or back execute flow.
 * */
@Singleton
public class MissedSchedulesManager {
  private static final Logger LOG = LoggerFactory.getLogger(MissedSchedulesManager.class);

  private ExecutorService missedSchedulesExecutor;
  private final boolean missedSchedulesManagerEnabled;
  private final ProjectManager projectManager;
  // TODO: Refactored to AlerterHolder to support missed scheduler use case
  private final Emailer emailer;

  // parameters to adjust MissScheduler
  private final int threadPoolSize;
  private final int DEFAULT_THREAD_POOL_SIZE = 5;
  private final MetricsManager metricsManager;
  private final Counter emailCounter;
  private final Counter missScheduleCounter;

  @Inject
  public MissedSchedulesManager(final Props azkProps,
                                final ProjectManager projectManager,
                                final Emailer emailer,
                                final MetricsManager metricsManager) {
    this.projectManager = projectManager;
    this.emailer = emailer;
    this.threadPoolSize = azkProps.getInt(Constants.MISSED_SCHEDULE_THREAD_POOL_SIZE, this.DEFAULT_THREAD_POOL_SIZE);
    this.missedSchedulesManagerEnabled = azkProps.getBoolean(Constants.MISSED_SCHEDULE_MANAGER_ENABLED, false);
    this.metricsManager = metricsManager;
    this.emailCounter = metricsManager.addCounter("missed-schedule-email-notification-count");
    this.missScheduleCounter = metricsManager.addCounter("missed-schedule-count");
    if (this.missedSchedulesManagerEnabled && this.threadPoolSize <= 0) {
      final String errorMsg =
          "MissedSchedulesManager is enabled but thread pool size is not positive: " + this.threadPoolSize;
      LOG.error(errorMsg);
      throw new AzkabanException(errorMsg);
    }
  }

  /**
   * Put timestamps for Missed executions along with email notifications into Task Queue.
   *
   * @param missedScheduleTimesInMs, timestamps of missed schedule
   * @param action, execute flow action
   * @param backExecutionEnabled, a schedule config from user, the default is false
   * */
  public boolean addMissedSchedule(final List<Long> missedScheduleTimesInMs,
      @NotNull final ExecuteFlowAction action,
      final boolean backExecutionEnabled) {
    if (!this.missedSchedulesManagerEnabled) {
      LOG.warn("missed Schedule manager is not enabled, can not add tasks.");
      return false;
    }
    final String projectId = action.getProjectName();
    final String flowName = action.getFlowName();
    final Project project = this.projectManager.getProject(projectId);
    if (project == null) {
      return false;
    }
    LOG.info("received a missed schedule on times {} by action {}", missedScheduleTimesInMs, action.toJson());
    List<String> emailRecipients = project.getFlow(flowName).getFailureEmails();
    if (action.getExecutionOptions().isFailureEmailsOverridden()) {
      emailRecipients = action.getExecutionOptions().getFailureEmails();
    }
    this.missScheduleCounter.inc(missedScheduleTimesInMs.size());
    this.emailCounter.inc();

    try {
      final Future taskFuture = this.missedSchedulesExecutor.submit(
          new MissedSchedulesOperationTask(missedScheduleTimesInMs, this.emailer, emailRecipients, backExecutionEnabled,
              action));
      if (backExecutionEnabled) {
        LOG.info("Missed schedule task submitted with emailer {} and action {}", emailRecipients, action);
      }
      return true;
    } catch (final RejectedExecutionException e) {
      LOG.error("Failed to add more missed schedules tasks to the thread pool", e);
      return false;
    }
  }

  public void start() {
    // TODO: add metrics to monitor active threads count, total number of missed schedule tasks, etc.
    // Create the thread pool
    this.missedSchedulesExecutor =
        this.missedSchedulesManagerEnabled ? Executors.newFixedThreadPool(this.threadPoolSize,
            new DaemonThreadFactory("azk-missed-schedules-task-pool")) : null;
    if (this.missedSchedulesManagerEnabled) {
      LOG.info("Missed Schedule Manager is ready to take tasks.");
    } else {
      LOG.info("Missed Schedule Manager is disabled.");
    }
  }

  public boolean stop() throws InterruptedException {
    if (this.missedSchedulesExecutor != null) {
      this.missedSchedulesExecutor.shutdown();
      if (!this.missedSchedulesExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        this.missedSchedulesExecutor.shutdownNow();
      }
    }
    return true;
  }

  /**
   * A MissedSchedule Task is a unit that store the critical information to execute a missedSchedule operation.
   * A Task is capable to
   * 1. notice user
   * 2. execute back execution if enabled feature flag (TODO)
   * when a missed schedule happens.
   * */
  public static class MissedSchedulesOperationTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MissedSchedulesOperationTask.class);
    private final Emailer emailer;
    // emailRecipients to notify users the schedules missed
    private final List<String> emailRecipients;
    // the email body
    private final String emailMessage;
    // back execute action
    // if user disabled the config, the action could be null.
    private final ExecuteFlowAction executeFlowAction;

    public MissedSchedulesOperationTask(final List<Long> missedScheduleTimes, final Emailer emailer,
        final List<String> emailRecipients, final boolean backExecutionEnabled,
        final ExecuteFlowAction executeFlowAction) {
      this.emailer = emailer;
      this.emailRecipients = emailRecipients;
      this.executeFlowAction = backExecutionEnabled ? executeFlowAction : null;
      final String missScheduleTimestampToDate = formatTimestamps(missedScheduleTimes);
      final MessageFormat messageFormat = new MessageFormat("This is an auto generated email to notify to flow owners"
          + " that the flow {0} in project {1} has scheduled the executions on {2} but failed to trigger on time. " + (
          backExecutionEnabled ? "Back execution will start soon as you enabled the config." : ""));

      this.emailMessage = messageFormat.format(
          new Object[]{executeFlowAction.getFlowName(), executeFlowAction.getProjectName(),
              missScheduleTimestampToDate});
    }

    /**
     * Chain timestamps using "," into human-readable string.
     *
     * @param timestamps Epoch timestamps in milliseconds
     * @return chained String
     * */
    private String formatTimestamps(final List<Long> timestamps) {
      final List<String> datesFromTimestamps =
          timestamps.stream().map(TimeUtils::formatDateTime).collect(Collectors.toList());
      return String.join(",", datesFromTimestamps);
    }

    @Override
    public void run() {
      try {
        this.emailer.sendEmail(this.emailRecipients, "Missed Azkaban Schedule Notification", this.emailMessage);

        if (this.executeFlowAction != null) {
          this.executeFlowAction.doAction();
        }
      } catch (final InterruptedException e) {
        final String warningMsg = "MissedScheduleTask thread is being interrupted, throwing out the exception";
        LOG.warn(warningMsg, e);
        throw new RuntimeException(warningMsg, e);
      } catch (final Exception e) {
        LOG.error("Error in executing task, it might due to fail to execute back execution flow", e);
      }
    }

    @VisibleForTesting
    protected String getEmailMessage() {
      return this.emailMessage;
    }
  }
}
