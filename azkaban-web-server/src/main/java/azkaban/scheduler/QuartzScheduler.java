/*
 * Copyright 2017 LinkedIn Corp.
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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static java.util.Objects.requireNonNull;

import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages Quartz schedules. Azkaban regards QuartzJob and QuartzTrigger as an one-to-one
 * mapping.
 * Quartz job key naming standard:
 * Job key is composed of job name and group name. Job type denotes job name. Project id+flow
 * name denotes group name.
 * E.x FLOW_TRIGGER as job name, 1.flow1 as group name
 */
@Singleton
public class QuartzScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(QuartzScheduler.class);
  private Scheduler scheduler = null;

  @Inject
  public QuartzScheduler(final Props azProps) throws SchedulerException {
    if (!azProps.getBoolean(ConfigurationKeys.ENABLE_QUARTZ, false)) {
      return;
    }
    // TODO kunkun-tang: Many quartz properties should be defaulted such that not necessarily being
    // checked into azkaban.properties. Also, we need to only assemble Quartz related properties
    // here, which should be done in Azkaban WebServer Guice Module.
    final StdSchedulerFactory schedulerFactory =
        new StdSchedulerFactory(azProps.toAllProperties());
    this.scheduler = schedulerFactory.getScheduler();

    // Currently Quartz only support internal job schedules. When we migrate to User Production
    // flows, we need to construct a Guice-Free JobFactory for use.
    this.scheduler.setJobFactory(SERVICE_PROVIDER.getInstance(SchedulerJobFactory.class));
  }

  public void start() throws SchedulerException {
    this.scheduler.start();
    LOG.info("Quartz Scheduler started.");
  }

  @VisibleForTesting
  void cleanup() throws SchedulerException {
    this.scheduler.clear();
  }

  public void shutdown() throws SchedulerException {
    this.scheduler.shutdown();
    LOG.info("Quartz Scheduler shut down.");
  }

  /**
   * Pause a job if it's present.
   * @param jobName
   * @param groupName
   * @return true if job has been paused, no if job doesn't exist.
   * @throws SchedulerException
   */
  public synchronized boolean pauseJobIfPresent(final String jobName, final String groupName)
      throws SchedulerException {
    if (ifJobExist(jobName, groupName)) {
      this.scheduler.pauseJob(new JobKey(jobName, groupName));
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check if job is paused.
   *
   * @return true if job is paused, false otherwise.
   */
  public synchronized boolean isJobPaused(final String jobName, final String groupName)
      throws SchedulerException {
    if (!ifJobExist(jobName, groupName)) {
      throw new SchedulerException(String.format("Job (job name %s, group name %s) doesn't "
          + "exist'", jobName, groupName));
    }
    final JobKey jobKey = new JobKey(jobName, groupName);
    final JobDetail jobDetail = this.scheduler.getJobDetail(jobKey);
    final List<? extends Trigger> triggers = this.scheduler.getTriggersOfJob(jobDetail.getKey());
    for (final Trigger trigger : triggers) {
      final TriggerState triggerState = this.scheduler.getTriggerState(trigger.getKey());
      if (TriggerState.PAUSED.equals(triggerState)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Resume a job.
   * @param jobName
   * @param groupName
   * @return true the job has been resumed, no if the job doesn't exist.
   * @throws SchedulerException
   */
  public synchronized boolean resumeJobIfPresent(final String jobName, final String groupName)
      throws SchedulerException {
    if (ifJobExist(jobName, groupName)) {
      this.scheduler.resumeJob(new JobKey(jobName, groupName));
      return true;
    } else {
      return false;
    }
  }

  /**
   * Unschedule a job.
   * @param jobName
   * @param groupName
   * @return true if job is found and unscheduled.
   * @throws SchedulerException
   */
  public synchronized boolean unscheduleJob(final String jobName, final String groupName) throws
      SchedulerException {
    return this.scheduler.deleteJob(new JobKey(jobName, groupName));
  }

  /**
   * Only cron schedule register is supported. Since register might be called when
   * concurrently uploading projects, so synchronized is added to ensure thread safety.
   *
   * @param cronExpression the cron schedule for this job
   * @param jobDescription Regarding QuartzJobDescription#groupName, in order to guarantee no
   * duplicate quartz schedules, we design the naming convention depending on use cases: <ul>
   * <li>User flow schedule: we use {@link JobKey#JobKey} to represent the identity of a
   * flow's schedule. The format follows "$projectID.$flowName" to guarantee no duplicates.
   * <li>Quartz schedule for AZ internal use: the groupName should start with letters, rather
   * than
   * number, which is the first case.</ul>
   *
   * @return true if job has been scheduled, false if the same job exists already.
   */
  public synchronized boolean scheduleJobIfAbsent(final String cronExpression, final QuartzJobDescription
      jobDescription) throws SchedulerException {

    requireNonNull(jobDescription, "jobDescription is null");

    if (ifJobExist(jobDescription.getJobName(), jobDescription.getGroupName())) {
      LOG.warn(String.format("can not register existing job with job name: "
          + "%s and group name: %s", jobDescription.getJobName(), jobDescription.getGroupName()));
      return false;
    }

    if (!CronExpression.isValidExpression(cronExpression)) {
      throw new SchedulerException(
          "The cron expression string <" + cronExpression + "> is not valid.");
    }

    // TODO kunkun-tang: we will modify this when we start supporting multi schedules per flow.
    final JobDetail job = JobBuilder.newJob(jobDescription.getJobClass())
        .withIdentity(jobDescription.getJobName(), jobDescription.getGroupName()).build();

    // Add external dependencies to Job Data Map.
    job.getJobDataMap().putAll(jobDescription.getContextMap());

    // TODO kunkun-tang: Need management code to deal with different misfire policy
    final Trigger trigger = TriggerBuilder
        .newTrigger()
        .withSchedule(
            CronScheduleBuilder.cronSchedule(cronExpression)
                .withMisfireHandlingInstructionFireAndProceed()
//            .withMisfireHandlingInstructionDoNothing()
//            .withMisfireHandlingInstructionIgnoreMisfires()
        )
        .build();

    this.scheduler.scheduleJob(job, trigger);
    LOG.info("Quartz Schedule with jobDetail " + job.getDescription() + " is registered.");
    return true;
  }


  @VisibleForTesting
  boolean ifJobExist(final String jobName, final String groupName)
      throws SchedulerException {
    return this.scheduler.getJobDetail(new JobKey(jobName, groupName)) != null;
  }

  public Scheduler getScheduler() {
    return this.scheduler;
  }
}
