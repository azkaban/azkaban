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
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.flow.NoSuchAzkabanResourceException;
import azkaban.metrics.MetricsManager;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.spi.AzkabanException;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import azkaban.utils.TimeUtils;
import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

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
public class MissedSchedulesManager extends ScheduleEmailerManager {

  private final boolean missedSchedulesManagerEnabled;
  private final ProjectManager projectManager;
  private final Counter emailCounter;
  private final Counter missScheduleCounter; // total number of missed schedules
  private final Counter missScheduleWithNonBackExecutionEnabledCounter; // number of missed schedules with no back execution enabled
  private final Counter missScheduleWithBackExecutionEnabledCounter; // number of missed schedules with back execution enabled
  private final Counter backExecutionCounter; // back execution happened when missed schedule is detected

  @Inject
  public MissedSchedulesManager(final Props azkProps,
                                final ProjectManager projectManager,
                                final Emailer emailer,
                                final MetricsManager metricsManager) {
    super(azkProps, emailer, "azk-missed-schedules-task-pool");
    this.projectManager = projectManager;
    this.missedSchedulesManagerEnabled = azkProps.getBoolean(Constants.MISSED_SCHEDULE_MANAGER_ENABLED, false);
    this.emailCounter = metricsManager.addCounter("missed-schedule-email-notification-count");
    this.missScheduleCounter = metricsManager.addCounter("missed-schedule-count");
    this.missScheduleWithNonBackExecutionEnabledCounter =
        metricsManager.addCounter("missed-schedule-non-back-exec-part-count");
    this.missScheduleWithBackExecutionEnabledCounter =
        metricsManager.addCounter("missed-schedule-back-exec-part-count");

    this.backExecutionCounter = metricsManager.addCounter("missed-schedule-back-execution-count");
    if (this.missedSchedulesManagerEnabled && this.threadPoolSize <= 0) {
      final String errorMsg =
          "MissedSchedulesManager is enabled but thread pool size is not positive: " + this.threadPoolSize;
      this.log.error(errorMsg);
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
      final boolean backExecutionEnabled) throws NoSuchAzkabanResourceException {
    if (!this.missedSchedulesManagerEnabled) {
      this.log.warn("missed Schedule manager is not enabled, can not add tasks.");
      return false;
    }
    this.log.info("received a missed schedule on times {} by action {}", missedScheduleTimesInMs, action.toJson());
    List<String> emailRecipients = getEmailRecipientsFromFlow(action);

    try {
      final Future taskFuture = this.executor.submit(
          new MissedSchedulesOperationTask(missedScheduleTimesInMs, this.emailer, emailRecipients, backExecutionEnabled,
              action));
      this.missScheduleCounter.inc(missedScheduleTimesInMs.size());
      if (!emailRecipients.isEmpty()) {
        this.emailCounter.inc();
      }
      if (backExecutionEnabled) {
        this.backExecutionCounter.inc();
        this.missScheduleWithBackExecutionEnabledCounter.inc(missedScheduleTimesInMs.size());
        this.log.info("Missed schedule task submitted with email recipients {} and action {}", emailRecipients, action);
      } else {
        this.missScheduleWithNonBackExecutionEnabledCounter.inc(missedScheduleTimesInMs.size());
      }
      return true;
    } catch (final RejectedExecutionException e) {
      this.log.error("Failed to add more missed schedules tasks to the thread pool", e);
      return false;
    }
  }

  @Override
  public boolean isEmailerManagerEnabled() {
    return this.missedSchedulesManagerEnabled;
  }

  private List<String> getEmailRecipientsFromFlow(ExecuteFlowAction action) {
    final int projectId = action.getProjectId();
    final String flowName = action.getFlowName();
    final Project project = FlowUtils.getProject(this.projectManager, projectId);
    final Flow flow = FlowUtils.getFlow(project, flowName);
    List<String> emailRecipients = flow.getFailureEmails();
    if (action.getExecutionOptions().isFailureEmailsOverridden()) {
      emailRecipients = action.getExecutionOptions().getFailureEmails();
    }
    return emailRecipients;
  }

  /**
   * A MissedSchedule Task is a unit that store the critical information to execute a missedSchedule operation.
   * A Task is capable to
   * 1. notice user
   * 2. execute back execution if enabled feature flag
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
          backExecutionEnabled ? "Back execution will start soon as you enabled the config "
              + "'Back Execute Once When missed Schedule Detects' through UI."  : ""));

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
