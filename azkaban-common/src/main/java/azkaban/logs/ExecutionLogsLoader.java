package azkaban.logs;

import azkaban.executor.ExecutorManagerException;
import azkaban.utils.FileIOUtils.LogData;
import java.io.File;

public interface ExecutionLogsLoader {

  void uploadLogFile(int execId, String name, int attempt, File... files)
      throws ExecutorManagerException;

  // FlowEndTime is needed for offline logs to tell if the logs are complete or not considering
  // every offline logging platform has decent amount of delay from when log-is-sent to when
  // log-is-available.
  LogData fetchLogs(int execId, String name, int attempt, int startByte,
      int length, long flowEndTime) throws ExecutorManagerException;

  int removeExecutionLogsByTime(long millis, int recordCleanupLimit)
      throws ExecutorManagerException;
}
