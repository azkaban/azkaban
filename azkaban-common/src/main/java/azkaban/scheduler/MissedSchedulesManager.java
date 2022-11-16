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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

  private final ExecutorService missedSchedulesExecutor;
  private final List<MissedSchedulesOperationTask> backgroundTasksList;
  private final Queue<MissedScheduleTaskQueueNode> missedScheduleTaskQueue = new ConcurrentLinkedQueue<>();
  private final ProjectManager projectManager;
  private final Emailer emailer;

  // parameters to adjust MissScheduler
  private final long sleepIntervalInMs;
  private final long DEFAULT_THREAD_IDLE_TIME_IN_MS = 60000;
  private final int threadPoolSize;
  private final int DEFAULT_THREAD_POOL_SIZE = 50;

  @Inject
  public MissedSchedulesManager(final Props azkProps,
      final ProjectManager projectManager,
      final Emailer emailer) {
    this.projectManager = projectManager;
    this.emailer = emailer;
    this.sleepIntervalInMs = azkProps.getLong(Constants.MISSED_SCHEDULE_THREAD_IDLE_TIME, DEFAULT_THREAD_IDLE_TIME_IN_MS);
    this.threadPoolSize = azkProps.getInt(Constants.MISSED_SCHEDULE_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
    if (!azkProps.getBoolean(Constants.MISSED_SCHEDULE_MANAGER_ENABLED, false) || threadPoolSize <= 0) {
      if (threadPoolSize <= 0) {
        LOG.warn("An invalid thread pool size is passed in: " + threadPoolSize + ", assuming that the service "
            + "is disabled, will not start any thread to do missed schedules operations.");
      }
      this.missedSchedulesExecutor = null;
      this.backgroundTasksList = null;
      return;
    }
    this.missedSchedulesExecutor =
        Executors.newFixedThreadPool(threadPoolSize, new DaemonThreadFactory("MissedSchedulesTask_thread"));
    this.backgroundTasksList = new ArrayList<>(threadPoolSize);
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
    if (backExecutionEnabled) {
      return this.missedScheduleTaskQueue.offer(
          new MissedScheduleTaskQueueNode(
              missedScheduleTimesInMs,
              project.getFlow(flowName).getFailureEmails(),
              action)
      );
    }
    return this.missedScheduleTaskQueue.offer(
        new MissedScheduleTaskQueueNode(
            missedScheduleTimesInMs,
            project.getFlow(flowName).getFailureEmails(),
            projectId,
            flowName));
  }

  @VisibleForTesting
  protected boolean isTaskQueueEmpty() {
    return this.missedScheduleTaskQueue.isEmpty();
  }

  @VisibleForTesting
  protected MissedScheduleTaskQueueNode peekFirstTask() {
    return this.missedScheduleTaskQueue.peek();
  }

  public void start() {
    // Start the thread pool
    for (int i = 0; i < threadPoolSize; i++) {
      MissedSchedulesOperationTask task = new MissedSchedulesOperationTask(i, sleepIntervalInMs);
      this.backgroundTasksList.add(task);
      // No need to maintain a list of task futures, since the life-cycles of the threads are maintained through
      // the Executors thread pool
      Future future = this.missedSchedulesExecutor.submit(task);
    }
    LOG.info("Missed Schedule Manager is ready to take tasks.");
  }

  public boolean stop() throws InterruptedException {
    if (this.backgroundTasksList != null) {
      for (MissedSchedulesOperationTask task : backgroundTasksList) {
        task.stop();
      }
    }
    if (missedSchedulesExecutor != null) {
      missedSchedulesExecutor.shutdown();
      if (!missedSchedulesExecutor.awaitTermination(10, TimeUnit.MINUTES)) {
        missedSchedulesExecutor.shutdownNow();
        missedSchedulesExecutor.awaitTermination(30, TimeUnit.SECONDS);
      }
    }
    return true;
  }

  /**
   * A Task contains the information to execute
   * 1. notice user
   * 2. execute back execution if enabled feature flag (TODO)
   * when a missed schedule happens.
   * */
  private class MissedSchedulesOperationTask implements Runnable {
    private Logger logger;
    private final String threadId;
    private boolean isRunning;
    private long sleepIntervalInMs;
    public MissedSchedulesOperationTask(int threadId, long sleepIntervalInMs) {
      this.threadId = "MissedSchedulesTask_thread_" + threadId;
      logger = LoggerFactory.getLogger(this.threadId);
      this.isRunning = true;
      this.sleepIntervalInMs = sleepIntervalInMs;
    }

    public void stop() {
      this.isRunning = false;
    }

    @Override
    public void run() {
      while (this.isRunning) {
        try {
          if (missedScheduleTaskQueue.isEmpty()) {
            Thread.sleep(this.sleepIntervalInMs);
          }
          MissedScheduleTaskQueueNode taskNode = missedScheduleTaskQueue.poll();
          // Must check whether it's null, since it's possible there is only one node in the queue, but multiple threads
          // think there is an available task at the same time and all of them poll from the queue, but only one of them
          // will succeed.
          if (taskNode != null) {
            emailer.sendEmail(taskNode.emailRecipients, "Missed Azkaban Schedule Notification", taskNode.emailMessage);
            if (taskNode.executeFlowAction != null) {
              taskNode.executeFlowAction.doAction();
            }
          }
        } catch (InterruptedException e) {
          String warningMsg = "Thread " + this.threadId + " is being interrupted, throwing out the exception";
          logger.warn(warningMsg, e);
          throw new RuntimeException(warningMsg, e);
        } catch (Exception e) {
          logger.error("Error in executing task, it might due to fail to execute back execution flow", e);
        }
      }
    }
  }
  /**
  * A MissedSchedule Task Node is a unit that store the critical information to execute a missedSchedule Task.
  * */
  public static class MissedScheduleTaskQueueNode {
    // emailRecipients to notify users the schedules missed
    private final List<String> emailRecipients;
    // the email body
    private final String emailMessage;
    // back execute action
    // if user disabled the config, the action could be null.
    private ExecuteFlowAction executeFlowAction;

    public MissedScheduleTaskQueueNode(List<Long> missedScheduleTimes,
        List<String> emailRecipients,
        String projectName,
        String flowName) {
      this.emailRecipients = emailRecipients;
      final String missScheduleTimestampToDate = formatTimestamps(missedScheduleTimes);
      MessageFormat messageFormat = new MessageFormat("This is autogenerated email to notify to flow owners that"
          + " the flow {0} in project {1} has scheduled the executions on {2} but failed to trigger on time.");
      this.emailMessage = messageFormat.format(new Object[]{flowName, projectName, missScheduleTimestampToDate});
      this.executeFlowAction = null;
    }

    public MissedScheduleTaskQueueNode(List<Long> missedScheduleTimes,
        List<String> emailRecipients,
        ExecuteFlowAction executeFlowAction) {
      this.emailRecipients = emailRecipients;
      this.executeFlowAction = executeFlowAction;
      final String missScheduleTimestampToDate = formatTimestamps(missedScheduleTimes);
      MessageFormat messageFormat = new MessageFormat("This is autogenerated email to notify to flow owners that"
          + " the flow {0} in project {1} has scheduled the executions on {2} but failed to trigger on time. "
          + "Back execution will start soon as you enabled the config.");
      this.emailMessage = messageFormat.format(
          new Object[]{executeFlowAction.getFlowName(), executeFlowAction.getProjectName(), missScheduleTimestampToDate});
    }

    private String formatTimestamps(List<Long> missScheduleTimestamps) {
      List<String> datesFromTimestamps = missScheduleTimestamps.stream()
          .map(timestamps -> new Date(timestamps).toString())
          .collect(Collectors.toList());
      return String.join(",", datesFromTimestamps);
    }

    @VisibleForTesting
    protected String getEmailMessage() {
      return emailMessage;
    }
  }
}
