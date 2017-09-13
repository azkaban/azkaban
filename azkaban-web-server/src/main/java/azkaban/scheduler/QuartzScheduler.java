package azkaban.scheduler;

import azkaban.utils.Props;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
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
}
