package azkaban.scheduler;

import static java.util.Objects.requireNonNull;

import azkaban.utils.Props;
import java.util.Map;
import java.util.Set;
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
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class QuartzScheduler {
  private static final Logger logger = LoggerFactory.getLogger(QuartzScheduler.class);
  private Scheduler scheduler = null;

  @Inject
  public QuartzScheduler(final Props azProps) throws SchedulerException{
    final StdSchedulerFactory schedulerFactory =
        new StdSchedulerFactory(azProps.toProperties());
    this.scheduler = schedulerFactory.getScheduler();
  }

  public void start() {
    try {
      this.scheduler.start();
    } catch (final SchedulerException e) {
      logger.warn("Error starting Quartz scheduler: ", e);
    }
    logger.info("Quartz Scheduler started.");
  }

  public void cleanup() {
    logger.info("Cleaning up schedules in scheduler");
    try {
      this.scheduler.clear();
    } catch (final SchedulerException e) {
      logger.error("Exception clearing scheduler: ", e);
    }
  }

  public void shutdown() {
    logger.info("Shutting down scheduler");
    try {
      this.scheduler.shutdown();
    } catch (final SchedulerException e) {
      logger.error("Exception shutting down scheduler: ", e);
    }
  }

  /**
   *
   * @param cronExpression the cron schedule for this job
   * @param jobDescription Regarding QuartzJobDescription#groupName, in order to guarantee no
   * duplicate quartz schedules, we design the naming convention depending on use cases:
   * <ul>
   *   <li>User flow schedule: we use {@link org.quartz.JobKey#JobKey} to represent the identity
   *   of a flow's schedule. The format follows "$projectID_$flowName" to guarantee no duplicates.
   *   </li>
   *   <li>Quartz schedule for AZ internal use: the groupName should start with letters, rather
   *   than number, which is the first case. </li>
   * <ul>
   */
  public void register(final String cronExpression, final QuartzJobDescription jobDescription)
    throws SchedulerException {

    requireNonNull(jobDescription, "jobDescription is null");

    // Not allowed to register duplicate job name.
    if(ifJobExist(jobDescription.getGroupName())) {
      throw new SchedulerException("can not register existing job " + jobDescription.getGroupName());
    }

    if (!CronExpression.isValidExpression(cronExpression)) {
      throw new SchedulerException("The cron expression string <" +  cronExpression + "> is not valid.");
    }

    // TODO kunkun-tang: Today all JobDetail should use "job1", and we will modify this when we
    // start supporting multi schedules per flow.
    final JobDetail job = JobBuilder.newJob(jobDescription.getJobClass())
        .withIdentity("job1", jobDescription.getGroupName()).build();
    for (final Map.Entry<String, Object> entry: jobDescription.getContextMap().entrySet()) {
      job.getJobDataMap().put(entry.getKey(), entry.getValue());
    }

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
    logger.info("Quartz Schedule with jobDetail " + job.getDescription() + " is registered.");
  }


  public boolean ifJobExist(final String groupName) throws SchedulerException {
    final Set<JobKey> jobKeySet = this.scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName));
    return jobKeySet !=null && jobKeySet.size() > 0;
  }
}
