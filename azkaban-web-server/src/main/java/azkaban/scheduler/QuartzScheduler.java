package azkaban.scheduler;

import azkaban.utils.Props;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
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

  private void register(final Scheduler scheduler, final JobDescription desc) throws SchedulerException {
    final ScheduleBuilder schb = CronScheduleBuilder.cronSchedule(desc.getTimer().cron());
    final Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity(desc.getId(), "RestEasy")
        .withSchedule(schb)
        .build();
    final JobDetail detail = JobBuilder.newJob(MethodDirectCallJob.class)
        .withIdentity(desc.getId(), "RestEasy")
        .usingJobData(MethodDirectCallJob.DEFINITION_OF_JOB, desc.getId())
        .build();

    scheduler.getContext().put(desc.getId(), desc);
    scheduler.scheduleJob(detail, trigger);
  }
}
