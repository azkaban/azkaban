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
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;

@Singleton
public class JdbcExecutorLoader implements ExecutorLoader {

  private final ExecutionFlowDao executionFlowDao;
  private final ExecutorDao executorDao;
  private final ExecutionJobDao executionJobDao;
  private final ExecutionLogsDao executionLogsDao;
  private final ExecutorEventsDao executorEventsDao;
  private final ActiveExecutingFlowsDao activeExecutingFlowsDao;
  private final FetchActiveFlowDao fetchActiveFlowDao;
  private final AssignExecutorDao assignExecutorDao;
  private final NumExecutionsDao numExecutionsDao;

  @Inject
  public JdbcExecutorLoader(final ExecutionFlowDao executionFlowDao,
      final ExecutorDao executorDao,
      final ExecutionJobDao executionJobDao,
      final ExecutionLogsDao executionLogsDao,
      final ExecutorEventsDao executorEventsDao,
      final ActiveExecutingFlowsDao activeExecutingFlowsDao,
      final FetchActiveFlowDao fetchActiveFlowDao,
      final AssignExecutorDao assignExecutorDao,
      final NumExecutionsDao numExecutionsDao) {
    this.executionFlowDao = executionFlowDao;
    this.executorDao = executorDao;
    this.executionJobDao = executionJobDao;
    this.executionLogsDao = executionLogsDao;
    this.executorEventsDao = executorEventsDao;
    this.activeExecutingFlowsDao = activeExecutingFlowsDao;
    this.fetchActiveFlowDao = fetchActiveFlowDao;
    this.numExecutionsDao = numExecutionsDao;
    this.assignExecutorDao = assignExecutorDao;
  }

  @Override
  public synchronized void uploadExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    this.executionFlowDao.uploadExecutableFlow(flow);
  }

  @Override
  public void updateExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    this.executionFlowDao.updateExecutableFlow(flow);
  }

  @Override
  public ExecutableFlow fetchExecutableFlow(final int id)
      throws ExecutorManagerException {
    return this.executionFlowDao.fetchExecutableFlow(id);
  }

  @Override
  public List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows()
      throws ExecutorManagerException {
    return this.executionFlowDao.fetchQueuedFlows();
  }

  /**
   * maxAge indicates how long finished flows are shown in Recently Finished flow page.
   */
  @Override
  public List<ExecutableFlow> fetchRecentlyFinishedFlows(final Duration maxAge)
      throws ExecutorManagerException {
    return this.executionFlowDao.fetchRecentlyFinishedFlows(maxAge);
  }

  @Override
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {

    return this.fetchActiveFlowDao.fetchActiveFlows();
  }

  @Override
  public Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(final int execId)
      throws ExecutorManagerException {

    return this.fetchActiveFlowDao.fetchActiveFlowByExecId(execId);
  }

  @Override
  public int fetchNumExecutableFlows() throws ExecutorManagerException {
    return this.numExecutionsDao.fetchNumExecutableFlows();
  }

  @Override
  public int fetchNumExecutableFlows(final int projectId, final String flowId)
      throws ExecutorManagerException {
    return this.numExecutionsDao.fetchNumExecutableFlows(projectId, flowId);
  }

  @Override
  public int fetchNumExecutableNodes(final int projectId, final String jobId)
      throws ExecutorManagerException {
    return this.numExecutionsDao.fetchNumExecutableNodes(projectId, jobId);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
      final int skip, final int num) throws ExecutorManagerException {
    return this.executionFlowDao.fetchFlowHistory(projectId, flowId, skip, num);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
      final int skip, final int num, final Status status) throws ExecutorManagerException {
    return this.executionFlowDao.fetchFlowHistory(projectId, flowId, skip, num, status);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int skip, final int num)
      throws ExecutorManagerException {
    return this.executionFlowDao.fetchFlowHistory(skip, num);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final String projContain,
      final String flowContains,
      final String userNameContains, final int status,
      final long startTime,
      final long endTime, final int skip, final int num) throws ExecutorManagerException {
    return this.executionFlowDao.fetchFlowHistory(projContain, flowContains,
        userNameContains, status, startTime, endTime, skip, num);
  }

  @Override
  public void addActiveExecutableReference(final ExecutionReference reference)
      throws ExecutorManagerException {

    this.activeExecutingFlowsDao.addActiveExecutableReference(reference);
  }

  @Override
  public void removeActiveExecutableReference(final int execid)
      throws ExecutorManagerException {

    this.activeExecutingFlowsDao.removeActiveExecutableReference(execid);
  }

  @Override
  public boolean updateExecutableReference(final int execId, final long updateTime)
      throws ExecutorManagerException {

    // Should be 1.
    return this.activeExecutingFlowsDao.updateExecutableReference(execId, updateTime);
  }

  @Override
  public void uploadExecutableNode(final ExecutableNode node, final Props inputProps)
      throws ExecutorManagerException {

    this.executionJobDao.uploadExecutableNode(node, inputProps);
  }

  @Override
  public void updateExecutableNode(final ExecutableNode node)
      throws ExecutorManagerException {

    this.executionJobDao.updateExecutableNode(node);
  }

  @Override
  public List<ExecutableJobInfo> fetchJobInfoAttempts(final int execId, final String jobId)
      throws ExecutorManagerException {

    return this.executionJobDao.fetchJobInfoAttempts(execId, jobId);
  }

  @Override
  public ExecutableJobInfo fetchJobInfo(final int execId, final String jobId, final int attempts)
      throws ExecutorManagerException {

    return this.executionJobDao.fetchJobInfo(execId, jobId, attempts);
  }

  @Override
  public Props fetchExecutionJobInputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    return this.executionJobDao.fetchExecutionJobInputProps(execId, jobId);
  }

  @Override
  public Props fetchExecutionJobOutputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    return this.executionJobDao.fetchExecutionJobOutputProps(execId, jobId);
  }

  @Override
  public Pair<Props, Props> fetchExecutionJobProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    return this.executionJobDao.fetchExecutionJobProps(execId, jobId);
  }

  @Override
  public List<ExecutableJobInfo> fetchJobHistory(final int projectId, final String jobId,
      final int skip, final int size)
      throws ExecutorManagerException {

    return this.executionJobDao.fetchJobHistory(projectId, jobId, skip, size);
  }

  @Override
  public LogData fetchLogs(final int execId, final String name, final int attempt,
      final int startByte,
      final int length) throws ExecutorManagerException {

    return this.executionLogsDao.fetchLogs(execId, name, attempt, startByte, length);
  }

  @Override
  public List<Object> fetchAttachments(final int execId, final String jobId, final int attempt)
      throws ExecutorManagerException {

    return this.executionJobDao.fetchAttachments(execId, jobId, attempt);
  }

  @Override
  public void uploadLogFile(final int execId, final String name, final int attempt,
      final File... files)
      throws ExecutorManagerException {
    this.executionLogsDao.uploadLogFile(execId, name, attempt, files);
  }

  @Override
  public void uploadAttachmentFile(final ExecutableNode node, final File file)
      throws ExecutorManagerException {
    this.executionJobDao.uploadAttachmentFile(node, file);
  }

  @Override
  public List<Executor> fetchAllExecutors() throws ExecutorManagerException {
    return this.executorDao.fetchAllExecutors();
  }

  @Override
  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    return this.executorDao.fetchActiveExecutors();
  }

  @Override
  public Executor fetchExecutor(final String host, final int port)
      throws ExecutorManagerException {
    return this.executorDao.fetchExecutor(host, port);
  }

  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    return this.executorDao.fetchExecutor(executorId);
  }

  @Override
  public void updateExecutor(final Executor executor) throws ExecutorManagerException {
    this.executorDao.updateExecutor(executor);
  }

  @Override
  public Executor addExecutor(final String host, final int port)
      throws ExecutorManagerException {
    return this.executorDao.addExecutor(host, port);
  }

  @Override
  public void removeExecutor(final String host, final int port) throws ExecutorManagerException {
    this.executorDao.removeExecutor(host, port);
  }

  @Override
  public void postExecutorEvent(final Executor executor, final EventType type, final String user,
      final String message) throws ExecutorManagerException {

    this.executorEventsDao.postExecutorEvent(executor, type, user, message);
  }

  @Override
  public List<ExecutorLogEvent> getExecutorEvents(final Executor executor, final int num,
      final int offset) throws ExecutorManagerException {
    return this.executorEventsDao.getExecutorEvents(executor, num, offset);
  }

  @Override
  public void assignExecutor(final int executorId, final int executionId)
      throws ExecutorManagerException {
    this.assignExecutorDao.assignExecutor(executorId, executionId);
  }

  @Override
  public Executor fetchExecutorByExecutionId(final int executionId)
      throws ExecutorManagerException {
    return this.executorDao.fetchExecutorByExecutionId(executionId);
  }

  @Override
  public int removeExecutionLogsByTime(final long millis)
      throws ExecutorManagerException {
    return this.executionLogsDao.removeExecutionLogsByTime(millis);
  }

  @Override
  public void unassignExecutor(final int executionId) throws ExecutorManagerException {
    this.assignExecutorDao.unassignExecutor(executionId);
  }
}
