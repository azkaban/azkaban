package azkaban.logs;

import azkaban.executor.ExecutionLogsDao;
import azkaban.executor.ExecutorManagerException;
import azkaban.utils.FileIOUtils.LogData;
import java.io.File;
import javax.inject.Inject;

public class JdbcExecutionLogsLoader implements ExecutionLogsLoader {

  private final ExecutionLogsDao executionLogsDao;

  @Inject
  public JdbcExecutionLogsLoader(final ExecutionLogsDao executionLogsDao) {
    this.executionLogsDao = executionLogsDao;
  }

  @Override
  public LogData fetchLogs(final int execId, final String name, final int attempt,
      final int startByte, final int length, final long flowStartTime, final long flowEndTime) throws ExecutorManagerException {

    return this.executionLogsDao.fetchLogs(execId, name, attempt, startByte, length);
  }

  @Override
  public void uploadLogFile(final int execId, final String name, final int attempt,
      final File... files)
      throws ExecutorManagerException {
    this.executionLogsDao.uploadLogFile(execId, name, attempt, files);
  }

  @Override
  public int removeExecutionLogsByTime(final long millis, final int recordCleanupLimit)
      throws ExecutorManagerException {
    return this.executionLogsDao.removeExecutionLogsByTime(millis, recordCleanupLimit);
  }
}
