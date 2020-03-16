package azkaban.webapp;

import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.ExecutorLoader;
import azkaban.utils.Props;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@SuppressWarnings("FutureReturnValueIgnored")
public class ExecutionLogsCleaner {

   private static final Logger logger = LoggerFactory.getLogger(ExecutionLogsCleaner.class);
   private final ScheduledExecutorService scheduler;
   private final ExecutorLoader executorLoader;
   private final Props azkProps;
   private long executionLogsRetentionMs;
   // 12 weeks
   private static final long DEFAULT_EXECUTION_LOGS_RETENTION_MS = 3 * 4 * 7
       * 24 * 60 * 60 * 1000L;

   // 1 hour
   private static final long DEFAULT_LOG_CLEANUP_INTERVAL_SECONDS = 60 * 60;
   private long cleanupIntervalInSeconds;

   @Inject
   public ExecutionLogsCleaner(final Props azkProps, final ExecutorLoader executorLoader) {
      this.azkProps = azkProps;
      this.executorLoader = executorLoader;
      this.scheduler = Executors.newSingleThreadScheduledExecutor();
      this.executionLogsRetentionMs = this.azkProps.getLong(
          ConfigurationKeys.EXECUTION_LOGS_RETENTION_MS,
          DEFAULT_EXECUTION_LOGS_RETENTION_MS);
      this.cleanupIntervalInSeconds = this.azkProps.getLong(
          ConfigurationKeys.EXECUTION_LOGS_CLEANUP_INTERVAL_SECONDS,
          DEFAULT_LOG_CLEANUP_INTERVAL_SECONDS);
   }

   public void start() {
      logger.info("Starting execution logs clean up thread");
      this.scheduler.scheduleAtFixedRate(() -> cleanExecutionLogs(), 0L, cleanupIntervalInSeconds,
          TimeUnit.SECONDS);
   }

   private void cleanExecutionLogs() {
      logger.info("Cleaning old logs from execution_logs");
      final long cutoff = System.currentTimeMillis() - this.executionLogsRetentionMs;
      logger.info("Cleaning old log files before "
          + new DateTime(cutoff).toString());
      cleanOldExecutionLogs(cutoff);
   }


   private void cleanOldExecutionLogs(final long millis) {
      final long beforeDeleteLogsTimestamp = System.currentTimeMillis();
      try {
         final int count = this.executorLoader.removeExecutionLogsByTime(millis);
         logger.info("Cleaned up " + count + " log entries.");
      } catch (final Exception e) {
         logger.error("log clean up failed. ", e);
      }
      logger.info(
          "log clean up time: " + (System.currentTimeMillis() - beforeDeleteLogsTimestamp)
              + " ms.");
   }
}
