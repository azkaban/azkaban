package azkaban.scheduler;

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

public class QuartzScheduler {
  private static final Logger logger = LoggerFactory.getLogger(QuartzScheduler.class);

  private static Scheduler scheduler = null;

  public static void main(final String[] args) throws SchedulerException {
    final QuartzScheduler quartz = new QuartzScheduler();
    quartz.initialize();
  }

  public void initialize() throws SchedulerException {

    final JobDetail job = JobBuilder.newJob(QuartzSampleJob.class)
        .withIdentity("dummyJobName4", "group1").build();
    final Trigger trigger = TriggerBuilder
        .newTrigger()
        .withIdentity("dummyTriggerName4", "group1")
        .withSchedule(
            CronScheduleBuilder.cronSchedule("*/20 * * * * ?")
            .withMisfireHandlingInstructionFireAndProceed()
//            .withMisfireHandlingInstructionDoNothing()
//            .withMisfireHandlingInstructionIgnoreMisfires()
        )
        .build();

    final StdSchedulerFactory schedulerFactory = new StdSchedulerFactory
        ("/Users/latang/Desktop/conf/quartz.properties");
    try {
//      schedulerFactory.initialize(new Properties());
      scheduler = schedulerFactory.getScheduler();
      scheduler.start();
//      scheduler.clear();
//      scheduler.scheduleJob(job, trigger);
    } catch (final SchedulerException e) {
      logger.warn("Error starting Quartz scheduler: " + e.getMessage());
      e.printStackTrace();
    }
    logger.debug("QrtzScheduler started");
  }

  public void cleanup() {
    logger.info("Shutting down scheduler");
    try {
      scheduler.shutdown();
    } catch (final SchedulerException e) {
      logger.error("Exception shutting down scheduler: " + e.getMessage());
    }
  }
}
