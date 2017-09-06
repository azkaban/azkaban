package azkaban.scheduler;

import java.util.Properties;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
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
    // create the properties
    final Properties qrtzProperties = new Properties();

    // start the scheduler
    final StdSchedulerFactory schedulerFactory = new StdSchedulerFactory
        ("/Users/latang/Desktop/conf/quartz.properties");
    try {
      schedulerFactory.initialize(qrtzProperties);
      scheduler = schedulerFactory.getScheduler();
      scheduler.start();
    } catch (final SchedulerException e) {
      logger.warn("Error starting Quartz scheduler: " + e.getMessage());
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
