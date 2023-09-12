/*
 * Copyright 2023 LinkedIn Corp.
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
import azkaban.executor.ExecutionOptions;
import azkaban.flow.NoSuchAzkabanResourceException;
import azkaban.metrics.MetricsManager;
import azkaban.sla.SlaOption;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScheduleChangeEmailerManager is designed to send email notification to flow owners when schedule is deleted,
 * it can be extended further to send emails on different scenarios especially on schedule change.
 * */
public class ScheduleChangeEmailerManager extends ScheduleEmailerManager {
  private final boolean scheduleDeletionEmailerEnabled;
  private final Meter scheduleDeletionCount;
  @Inject
  public ScheduleChangeEmailerManager(Props azkProps, Emailer emailer, MetricsManager metricsManager) {
    super(azkProps, emailer, "azk-schedule-deletion-emailer-task-pool");
    this.scheduleDeletionEmailerEnabled = azkProps.getBoolean(Constants.SCHEDULE_EMAILER_ENABLED, false);
    this.scheduleDeletionCount = metricsManager.addMeter("schedule-deletion-count");
  }

  @Override
  public boolean isEmailerManagerEnabled() {
    return this.scheduleDeletionEmailerEnabled;
  }

  public void addNotificationTaskOnScheduleDelete(@NotNull final Schedule schedule,
      final String userName, String reason) {
    this.scheduleDeletionCount.mark();
    if (!scheduleDeletionEmailerEnabled) {
      this.log.info("Schedule deletion email feature is not enabled. Skip sending email.");
      return;
    }
    try {
      List<String> emailRecipients = getEmailRecipientsFromSchedule(schedule);
      if (emailRecipients.isEmpty()) {
        this.log.info("No email recipients found for schedule {}", schedule.toString());
        return;
      }
      final Future taskFuture = this.executor.submit(
          new ScheduleDeletionTask(this.emailer, emailRecipients, schedule, userName, reason));
    } catch (Exception e) {
      log.error("Failed to add email notification task for schedule deletion of {}", schedule.toString(), e);
    }
  }

  /**
   * Fetch email recipients from schedule. If slaOptions is not empty, use slaOptions to get email recipients,
   * otherwise use successEmails and failureEmails provided from executionOptions of schedule.
   * */
  @VisibleForTesting
  protected List<String> getEmailRecipientsFromSchedule(Schedule schedule) {
    ExecutionOptions executionOptions = schedule.getExecutionOptions();
    List<SlaOption> slaOptions = executionOptions.getSlaOptions();
    Set<String> emailRecipients = new HashSet<>();
    if (slaOptions != null && !slaOptions.isEmpty()) {
      slaOptions.stream().filter(slaOption -> slaOption != null && slaOption.getEmails() != null)
          .map(SlaOption::getEmails).forEach(emailRecipients::addAll);
    } else {
      emailRecipients.addAll(executionOptions.getFailureEmails());
      emailRecipients.addAll(executionOptions.getSuccessEmails());
    }
    return new ArrayList<>(emailRecipients);
  }

  public static class ScheduleDeletionTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleDeletionTask.class);
    private final Emailer emailer;
    // emailRecipients to notify users the schedules missed
    private final List<String> emailRecipients;
    // the email body
    private final String emailMessage;

    public ScheduleDeletionTask(final Emailer emailer,
        final List<String> emailRecipients,
        final Schedule schedule,
        final String user,
        final String reason) {
      this.emailer = emailer;
      this.emailRecipients = emailRecipients;
      final MessageFormat messageFormat = new MessageFormat(
          "This is an auto generated email to notify flow owners. \n"
              + "The schedule id {0} with cron expression {1} for flow {2} in project {3} "
              + "has been deleted by user {4} due to the following reason: {5}. ");
      this.emailMessage = messageFormat.format(
          new Object[]{String.valueOf(schedule.getScheduleId()), schedule.getCronExpression(), schedule.getFlowName(),
              schedule.getProjectName(), user, reason});
    }
    @Override
    public void run() {
      try {
        emailer.sendEmail(this.emailRecipients, "Azkaban Schedule Deletion Notification", this.emailMessage);
      } catch (final Exception e) {
        LOG.error("Failed to send email to " + emailRecipients + " due to exception", e);
      }
    }
  }
}
