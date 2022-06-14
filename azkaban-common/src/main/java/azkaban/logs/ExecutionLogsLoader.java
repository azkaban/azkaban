package azkaban.logs;

import azkaban.executor.ExecutorManagerException;
import azkaban.utils.FileIOUtils.LogData;
import java.io.File;

public interface ExecutionLogsLoader {

  void uploadLogFile(int execId, String name, int attempt, File... files)
      throws ExecutorManagerException;

  LogData fetchLogs(int execId, String name, int attempt, int startByte,
      int length) throws ExecutorManagerException;

  int removeExecutionLogsByTime(long millis, int recordCleanupLimit)
      throws ExecutorManagerException;
}
