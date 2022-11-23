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
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.DaemonThreadFactory;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

  @Inject
  public MissedSchedulesManager(final Props azkProps,
      final ProjectManager projectManager,
      final Emailer emailer) {
    this.projectManager = projectManager;
    this.emailer = emailer;
    this.threadPoolSize = azkProps.getInt(Constants.MISSED_SCHEDULE_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
    this.missedSchedulesManagerEnabled = azkProps.getBoolean(Constants.MISSED_SCHEDULE_MANAGER_ENABLED, false);
    if (this.missedSchedulesManagerEnabled && threadPoolSize <= 0) {
      String errorMsg = "MissedSchedulesManager is enabled but thread pool size is not positive: " + threadPoolSize;
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
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
      final ExecuteFlowAction action,
      final boolean backExecutionEnabled) {
    String projectId = action.getProjectName();
    String flowName = action.getFlowName();
    Project project = projectManager.getProject(projectId);
    if (project == null) {
      return false;
    }
    List<String> emailRecipients = project.getFlow(flowName).getFailureEmails();
    try {
      Future taskFuture = this.missedSchedulesExecutor.submit(
          new MissedSchedulesOperationTask(
              missedScheduleTimesInMs,
              this.emailer,
              emailRecipients,
              backExecutionEnabled,
              action));
      if (backExecutionEnabled) {
        LOG.info("Missed schedule task submitted with emailer {} and action {}", emailRecipients, action);
      }
      return true;
    } catch (RejectedExecutionException e) {
      LOG.error("Failed to add more missed schedules tasks to the thread pool", e);
      return false;
    }
  }

  public void start() {
    // TODO: add metrics to monitor active threads count, total number of missed schedule tasks, etc.
    // Create the thread pool
    this.missedSchedulesExecutor = this.missedSchedulesManagerEnabled
        ? Executors.newFixedThreadPool(threadPoolSize, new DaemonThreadFactory("azk-missed-schedules-task-pool"))
        : null;
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
    private ExecuteFlowAction executeFlowAction;

    public MissedSchedulesOperationTask(List<Long> missedScheduleTimes,
        Emailer emailer,
        List<String> emailRecipients,
        boolean backExecutionEnabled,
        ExecuteFlowAction executeFlowAction) {
      this.emailer = emailer;
      this.emailRecipients = emailRecipients;
      this.executeFlowAction = backExecutionEnabled ? executeFlowAction : null;
      final String missScheduleTimestampToDate = formatTimestamps(missedScheduleTimes);
      MessageFormat messageFormat = new MessageFormat("This is autogenerated email to notify to flow owners that"
          + " the flow {0} in project {1} has scheduled the executions on {2} but failed to trigger on time. "
          + (backExecutionEnabled ? "Back execution will start soon as you enabled the config." : ""));

      this.emailMessage = messageFormat.format(
          new Object[]{executeFlowAction.getFlowName(), executeFlowAction.getProjectName(), missScheduleTimestampToDate});
    }

    private String formatTimestamps(List<Long> missScheduleTimestamps) {
      final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      List<String> datesFromTimestamps = missScheduleTimestamps.stream()
          .map(timestamps -> sdf.format(new Date(timestamps)))
          .collect(Collectors.toList());
      return String.join(",", datesFromTimestamps);
    }

    @Override
    public void run() {
      try {
        this.emailer.sendEmail(this.emailRecipients, "Missed Azkaban Schedule Notification", this.emailMessage);
        if (this.executeFlowAction != null) {
          this.executeFlowAction.doAction();
        }
      } catch (InterruptedException e) {
        String warningMsg = "MissedScheduleTask thread is being interrupted, throwing out the exception";
        LOG.warn(warningMsg, e);
        throw new RuntimeException(warningMsg, e);
      } catch (Exception e) {
        LOG.error("Error in executing task, it might due to fail to execute back execution flow", e);
      }
    }

    @VisibleForTesting
    protected String getEmailMessage() {
      return emailMessage;
    }
  }
}
