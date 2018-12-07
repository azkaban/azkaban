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

import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public interface ExecutorLoader {

  void uploadExecutableFlow(ExecutableFlow flow)
      throws ExecutorManagerException;

  ExecutableFlow fetchExecutableFlow(int execId)
      throws ExecutorManagerException;

  List<ExecutableFlow> fetchRecentlyFinishedFlows(Duration maxAge)
      throws ExecutorManagerException;

  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException;

  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchUnfinishedFlows()
      throws ExecutorManagerException;

  Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(int execId)
      throws ExecutorManagerException;

  List<ExecutableFlow> fetchFlowHistory(int skip, int num)
      throws ExecutorManagerException;

  List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId,
      int skip, int num) throws ExecutorManagerException;

  List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId,
      int skip, int num, Status status) throws ExecutorManagerException;

  List<ExecutableFlow> fetchFlowHistory(String projContain,
      String flowContains, String userNameContains, int status, long startData,
      long endData, int skip, int num) throws ExecutorManagerException;

  List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
      final long startTime) throws ExecutorManagerException;

  /**
   * <pre>
   * Fetch all executors from executors table
   * Note:-
   * 1 throws an Exception in case of a SQL issue
   * 2 returns an empty list in case of no executor
   * </pre>
   *
   * @return List<Executor>
   */
  List<Executor> fetchAllExecutors() throws ExecutorManagerException;

  /**
   * <pre>
   * Fetch all executors from executors table with active = true
   * Note:-
   * 1 throws an Exception in case of a SQL issue
   * 2 returns an empty list in case of no active executor
   * </pre>
   *
   * @return List<Executor>
   */
  List<Executor> fetchActiveExecutors() throws ExecutorManagerException;

  /**
   * <pre>
   * Fetch executor from executors with a given (host, port)
   * Note:
   * 1. throws an Exception in case of a SQL issue
   * 2. return null when no executor is found
   * with the given (host,port)
   * </pre>
   *
   * @return Executor
   */
  Executor fetchExecutor(String host, int port)
      throws ExecutorManagerException;

  /**
   * <pre>
   * Fetch executor from executors with a given executorId
   * Note:
   * 1. throws an Exception in case of a SQL issue
   * 2. return null when no executor is found with the given executorId
   * </pre>
   *
   * @return Executor
   */
  Executor fetchExecutor(int executorId) throws ExecutorManagerException;

  /**
   * <pre>
   * create an executor and insert in executors table.
   * Note:-
   * 1. throws an Exception in case of a SQL issue
   * 2. throws an Exception if a executor with (host, port) already exist
   * 3. return null when no executor is found with the given executorId
   * </pre>
   *
   * @return Executor
   */
  Executor addExecutor(String host, int port)
      throws ExecutorManagerException;

  /**
   * <pre>
   * create an executor and insert in executors table.
   * Note:-
   * 1. throws an Exception in case of a SQL issue
   * 2. throws an Exception if there is no executor with the given id
   * 3. return null when no executor is found with the given executorId
   * </pre>
   */
  void updateExecutor(Executor executor) throws ExecutorManagerException;

  /**
   * <pre>
   * Remove the executor from executors table.
   * Note:-
   * 1. throws an Exception in case of a SQL issue
   * 2. throws an Exception if there is no executor in the table* </pre>
   * </pre>
   */
  void removeExecutor(String host, int port) throws ExecutorManagerException;

  /**
   * <pre>
   * Log an event in executor_event audit table Note:- throws an Exception in
   * case of a SQL issue
   * Note: throws an Exception in case of a SQL issue
   * </pre>
   *
   * @return isSuccess
   */
  void postExecutorEvent(Executor executor, EventType type, String user,
      String message) throws ExecutorManagerException;

  /**
   * <pre>
   * This method is to fetch events recorded in executor audit table, inserted
   * by postExecutorEvents with a given executor, starting from skip
   * Note:-
   * 1. throws an Exception in case of a SQL issue
   * 2. Returns an empty list in case of no events
   * </pre>
   *
   * @return List<ExecutorLogEvent>
   */
  List<ExecutorLogEvent> getExecutorEvents(Executor executor, int num,
      int offset) throws ExecutorManagerException;

  void addActiveExecutableReference(ExecutionReference ref)
      throws ExecutorManagerException;

  void removeActiveExecutableReference(int execId)
      throws ExecutorManagerException;


  /**
   * <pre>
   * Unset executor Id for an execution
   * Note:-
   * throws an Exception in case of a SQL issue
   * </pre>
   */
  void unassignExecutor(int executionId) throws ExecutorManagerException;

  /**
   * <pre>
   * Set an executor Id to an execution
   * Note:-
   * 1. throws an Exception in case of a SQL issue
   * 2. throws an Exception in case executionId or executorId do not exist
   * </pre>
   */
  void assignExecutor(int executorId, int execId)
      throws ExecutorManagerException;

  /**
   * <pre>
   * Fetches an executor corresponding to a given execution
   * Note:-
   * 1. throws an Exception in case of a SQL issue
   * 2. return null when no executor is found with the given executionId
   * </pre>
   *
   * @return fetched Executor
   */
  Executor fetchExecutorByExecutionId(int executionId)
      throws ExecutorManagerException;

  /**
   * <pre>
   * Fetch queued flows which have not yet dispatched
   * Note:
   * 1. throws an Exception in case of a SQL issue
   * 2. return empty list when no queued execution is found
   * </pre>
   *
   * @return List of queued flows and corresponding execution reference
   */
  List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows()
      throws ExecutorManagerException;

  boolean updateExecutableReference(int execId, long updateTime)
      throws ExecutorManagerException;

  LogData fetchLogs(int execId, String name, int attempt, int startByte,
      int endByte) throws ExecutorManagerException;

  List<Object> fetchAttachments(int execId, String name, int attempt)
      throws ExecutorManagerException;

  void uploadLogFile(int execId, String name, int attempt, File... files)
      throws ExecutorManagerException;

  void uploadAttachmentFile(ExecutableNode node, File file)
      throws ExecutorManagerException;

  void updateExecutableFlow(ExecutableFlow flow)
      throws ExecutorManagerException;

  void uploadExecutableNode(ExecutableNode node, Props inputParams)
      throws ExecutorManagerException;

  List<ExecutableJobInfo> fetchJobInfoAttempts(int execId, String jobId)
      throws ExecutorManagerException;

  ExecutableJobInfo fetchJobInfo(int execId, String jobId, int attempt)
      throws ExecutorManagerException;

  List<ExecutableJobInfo> fetchJobHistory(int projectId, String jobId,
      int skip, int size) throws ExecutorManagerException;

  void updateExecutableNode(ExecutableNode node)
      throws ExecutorManagerException;

  int fetchNumExecutableFlows(int projectId, String flowId)
      throws ExecutorManagerException;

  int fetchNumExecutableFlows() throws ExecutorManagerException;

  int fetchNumExecutableNodes(int projectId, String jobId)
      throws ExecutorManagerException;

  Props fetchExecutionJobInputProps(int execId, String jobId)
      throws ExecutorManagerException;

  Props fetchExecutionJobOutputProps(int execId, String jobId)
      throws ExecutorManagerException;

  Pair<Props, Props> fetchExecutionJobProps(int execId, String jobId)
      throws ExecutorManagerException;

  int removeExecutionLogsByTime(long millis)
      throws ExecutorManagerException;
}
