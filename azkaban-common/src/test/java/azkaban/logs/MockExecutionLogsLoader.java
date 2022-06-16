package azkaban.logs;

import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.MockExecutorLoader;
import azkaban.utils.FileIOUtils.LogData;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used in unit tests to mock the "DB layer" (the real implementation is JdbcExecutionsLogLoader).
 * Captures status updates of jobs and flows (in memory) so that they can be checked in tests.
 */
public class MockExecutionLogsLoader implements ExecutionLogsLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockExecutionLogsLoader.class);

  @Override
  public void uploadLogFile(final int execId, final String name, final int attempt,
      final File... files)
      throws ExecutorManagerException {
    for (final File file : files) {
      try {
        final String logs = FileUtils.readFileToString(file, "UTF-8");
        LOGGER.info("Uploaded log for [" + name + "]:[" + execId + "]:\n" + logs);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public LogData fetchLogs(final int execId, final String name, final int attempt,
      final int startByte,
      final int endByte) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int removeExecutionLogsByTime(final long millis, final int recordCleanupLimit)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return 0;
  }
}
