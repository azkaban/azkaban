/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor;

import java.io.File;
import java.util.List;
import java.util.Map;

import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;

public interface ExecutorLoader {
  public void uploadExecutableFlow(ExecutableFlow flow)
      throws ExecutorManagerException;

  public ExecutableFlow fetchExecutableFlow(int execId)
      throws ExecutorManagerException;

  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException;

  public List<ExecutableFlow> fetchFlowHistory(int skip, int num)
      throws ExecutorManagerException;

  public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId,
      int skip, int num) throws ExecutorManagerException;

  public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId,
      int skip, int num, Status status) throws ExecutorManagerException;

  public List<ExecutableFlow> fetchFlowHistory(String projContain,
      String flowContains, String userNameContains, int status, long startData,
      long endData, int skip, int num) throws ExecutorManagerException;

  /**
   * Fetch all executors from executors table with active = true
   *
   * @return List<Executor>
   * @throws ExecutorManagerException
   */
  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException;

  /**
   * Fetch executor from executors with a given (host, port)
   *
   * @return Executor
   * @throws ExecutorManagerException
   */
  public Executor fetchExecutor(String host, int port)
    throws ExecutorManagerException;

  /**
   * Fetch executor from executors with a given executorId
   *
   * @return Executor
   * @throws ExecutorManagerException
   */
  public Executor fetchExecutor(int executorId) throws ExecutorManagerException;

  /**
   * create an executor and insert in executors table. Fails, if a executor with
   * (host, port) already exist
   *
   * @return Executor
   * @throws ExecutorManagerException
   */
  public Executor addExecutor(String host, int port)
    throws ExecutorManagerException;

  /**
   * inactivate an executor in executors table
   *
   * @param executorId
   * @throws ExecutorManagerException
   */
  public void inactivateExecutor(int executorId)
    throws ExecutorManagerException;

  /**
   * Re-activate an executor in executors table
   *
   * @param executorId
   * @throws ExecutorManagerException
   */
  void activateExecutor(int executorId) throws ExecutorManagerException;

  /**
   * Log an event in executor_event audit table
   *
   * @param executor
   * @param type
   * @param user
   * @param message
   * @return isSuccess
   */
  boolean postEvent(Executor executor, EventType type, String user,
    String message);

  /**
   * Fetch num events associated with a given executor, starting from skip
   *
   * @param executor
   * @param num
   * @param skip
   * @return List<ExecutorLogEvent>
   * @throws ExecutorManagerException
   */
  List<ExecutorLogEvent> getExecutorEvents(Executor executor, int num, int skip)
    throws ExecutorManagerException;

  public void addActiveExecutableReference(ExecutionReference ref)
      throws ExecutorManagerException;

  public void removeActiveExecutableReference(int execId)
      throws ExecutorManagerException;

  public boolean updateExecutableReference(int execId, long updateTime)
      throws ExecutorManagerException;

  public LogData fetchLogs(int execId, String name, int attempt, int startByte,
      int endByte) throws ExecutorManagerException;

  public List<Object> fetchAttachments(int execId, String name, int attempt)
      throws ExecutorManagerException;

  public void uploadLogFile(int execId, String name, int attempt, File... files)
      throws ExecutorManagerException;

  public void uploadAttachmentFile(ExecutableNode node, File file)
      throws ExecutorManagerException;

  public void updateExecutableFlow(ExecutableFlow flow)
      throws ExecutorManagerException;

  public void uploadExecutableNode(ExecutableNode node, Props inputParams)
      throws ExecutorManagerException;

  public List<ExecutableJobInfo> fetchJobInfoAttempts(int execId, String jobId)
      throws ExecutorManagerException;

  public ExecutableJobInfo fetchJobInfo(int execId, String jobId, int attempt)
      throws ExecutorManagerException;

  public List<ExecutableJobInfo> fetchJobHistory(int projectId, String jobId,
      int skip, int size) throws ExecutorManagerException;

  public void updateExecutableNode(ExecutableNode node)
      throws ExecutorManagerException;

  public int fetchNumExecutableFlows(int projectId, String flowId)
      throws ExecutorManagerException;

  public int fetchNumExecutableFlows() throws ExecutorManagerException;

  public int fetchNumExecutableNodes(int projectId, String jobId)
      throws ExecutorManagerException;

  public Props fetchExecutionJobInputProps(int execId, String jobId)
      throws ExecutorManagerException;

  public Props fetchExecutionJobOutputProps(int execId, String jobId)
      throws ExecutorManagerException;

  public Pair<Props, Props> fetchExecutionJobProps(int execId, String jobId)
      throws ExecutorManagerException;

  public int removeExecutionLogsByTime(long millis)
      throws ExecutorManagerException;
}
