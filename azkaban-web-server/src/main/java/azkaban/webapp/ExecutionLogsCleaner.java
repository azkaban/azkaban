package azkaban.webapp;

import static azkaban.Constants.LogConstants.NEARLINE_LOGS;

import azkaban.Constants.ConfigurationKeys;
import azkaban.logs.ExecutionLogsLoader;
import azkaban.utils.Props;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@SuppressWarnings("FutureReturnValueIgnored")
public class ExecutionLogsCleaner {

   private static final Logger logger = LoggerFactory.getLogger(ExecutionLogsCleaner.class);
   private final ScheduledExecutorService scheduler;
   private final ExecutionLogsLoader executionLogsLoader;
   private final Props azkProps;
   private long executionLogsRetentionMs;
   // 12 weeks
   private static final long DEFAULT_EXECUTION_LOGS_RETENTION_MS = 3 * 4 * 7
       * 24 * 60 * 60 * 1000L;

   // 1 hour
   private static final long DEFAULT_LOG_CLEANUP_INTERVAL_SECONDS = 60 * 60;
   private long cleanupIntervalInSeconds;

   private static final int DEFAULT_LOG_CLEANUP_RECORD_LIMIT = 1000;
   private int executionLogCleanupRecordLimit;

   // Only applicable to nearline/jdbc logs.
   // We expect offline logs to be managed by another platform where retention is handled well.
   @Inject
   public ExecutionLogsCleaner(final Props azkProps,
       final @Named(NEARLINE_LOGS) ExecutionLogsLoader executionLogsLoader) {
      this.azkProps = azkProps;
      this.executionLogsLoader = executionLogsLoader;
      this.scheduler =
          Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(
              "azk-execlog-cleaner").build());
      this.executionLogsRetentionMs = this.azkProps.getLong(
          ConfigurationKeys.EXECUTION_LOGS_RETENTION_MS,
          DEFAULT_EXECUTION_LOGS_RETENTION_MS);
      this.cleanupIntervalInSeconds = this.azkProps.getLong(
          ConfigurationKeys.EXECUTION_LOGS_CLEANUP_INTERVAL_SECONDS,
          DEFAULT_LOG_CLEANUP_INTERVAL_SECONDS);
      this.executionLogCleanupRecordLimit =
          this.azkProps.getInt(ConfigurationKeys.EXECUTION_LOGS_CLEANUP_RECORD_LIMIT,
              DEFAULT_LOG_CLEANUP_RECORD_LIMIT);
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
         final int count = this.executionLogsLoader.removeExecutionLogsByTime(millis,
             this.executionLogCleanupRecordLimit);
         logger.info("Cleaned up " + count + " log entries.");
      } catch (final Exception e) {
         logger.error("log clean up failed. ", e);
      }
      logger.info(
          "log clean up time: " + (System.currentTimeMillis() - beforeDeleteLogsTimestamp)
              + " ms.");
   }
}
