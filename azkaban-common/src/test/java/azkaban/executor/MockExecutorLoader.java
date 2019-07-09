/*
 * Copyright 2014 LinkedIn Corp.
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
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Used in unit tests to mock the "DB layer" (the real implementation is JdbcExecutorLoader).
 * Captures status updates of jobs and flows (in memory) so that they can be checked in tests.
 */
public class MockExecutorLoader implements ExecutorLoader {

  private static final Logger logger = Logger.getLogger(MockExecutorLoader.class);

  Map<Integer, Integer> executionExecutorMapping = new ConcurrentHashMap<>();
  Map<Integer, ExecutableFlow> flows = new ConcurrentHashMap<>();
  Map<String, ExecutableNode> nodes = new ConcurrentHashMap<>();
  Map<Integer, ExecutionReference> refs = new ConcurrentHashMap<>();
  int flowUpdateCount = 0;
  Map<String, Integer> jobUpdateCount = new ConcurrentHashMap<>();
  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new ConcurrentHashMap<>();
  List<Executor> executors = new ArrayList<>();
  int executorIdCounter = 0;
  Map<Integer, ArrayList<ExecutorLogEvent>> executorEvents = new ConcurrentHashMap<>();

  @Override
  public void uploadExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    // Clone the flow node to mimick how it would be saved in DB.
    // If we would keep a handle to the original flow node, we would also see any changes made after
    // this method was called. We must only store a snapshot of the current state.
    // Also to avoid modifying statuses of the original job nodes in this.updateExecutableFlow()
    final ExecutableFlow exFlow = ExecutableFlow.createExecutableFlowFromObject(flow.toObject());
    this.flows.put(flow.getExecutionId(), exFlow);
    this.flowUpdateCount++;
  }

  @Override
  public ExecutableFlow fetchExecutableFlow(final int execId)
      throws ExecutorManagerException {
    final ExecutableFlow flow = this.flows.get(execId);
    return ExecutableFlow.createExecutableFlowFromObject(flow.toObject());
  }

  @Override
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {
    return this.activeFlows;
  }

  @Override
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchUnfinishedFlows()
      throws ExecutorManagerException {
    return this.activeFlows;
  }

  @Override
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchUnfinishedFlowsMetadata()
      throws ExecutorManagerException {
    return this.activeFlows.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> {
          final ExecutableFlow metadata = getExecutableFlowMetadata(e.getValue().getSecond());
          return new Pair<>(e.getValue().getFirst(), metadata);
        }));
  }

  private ExecutableFlow getExecutableFlowMetadata(
      final ExecutableFlow fullExFlow) {
    final Flow flow = new Flow(fullExFlow.getId());
    final Project project = new Project(fullExFlow.getProjectId(), null);
    project.setVersion(fullExFlow.getVersion());
    flow.setVersion(fullExFlow.getVersion());
    final ExecutableFlow metadata = new ExecutableFlow(project, flow);
    metadata.setExecutionId(fullExFlow.getExecutionId());
    metadata.setStatus(fullExFlow.getStatus());
    metadata.setSubmitTime(fullExFlow.getSubmitTime());
    metadata.setStartTime(fullExFlow.getStartTime());
    metadata.setEndTime(fullExFlow.getEndTime());
    metadata.setSubmitUser(fullExFlow.getSubmitUser());
    return metadata;
  }

  @Override
  public Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(final int execId) {
    return new Pair<>(null, null);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
      final int skip, final int num) throws ExecutorManagerException {
    return null;
  }

  @Override
  public void addActiveExecutableReference(final ExecutionReference ref)
      throws ExecutorManagerException {
    this.refs.put(ref.getExecId(), ref);
  }

  @Override
  public void removeActiveExecutableReference(final int execId)
      throws ExecutorManagerException {
    this.refs.remove(execId);
  }

  @Override
  public void uploadLogFile(final int execId, final String name, final int attempt,
      final File... files)
      throws ExecutorManagerException {
    for (final File file : files) {
      try {
        final String logs = FileUtils.readFileToString(file, "UTF-8");
        logger.info("Uploaded log for [" + name + "]:[" + execId + "]:\n" + logs);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void updateExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    final ExecutableFlow toUpdate = this.flows.get(flow.getExecutionId());

    toUpdate.applyUpdateObject(flow.toUpdateObject(0));
    this.flowUpdateCount++;
  }

  @Override
  public void uploadExecutableNode(final ExecutableNode node, final Props inputParams)
      throws ExecutorManagerException {
    // Clone the job node to mimick how it would be saved in DB.
    // If we would keep a handle to the original job node, we would also see any changes made after
    // this method was called. We must only store a snapshot of the current state.
    // Also to avoid modifying statuses of the original job nodes in this.updateExecutableNode()
    final ExecutableNode exNode = new ExecutableNode();
    exNode.fillExecutableFromMapObject(node.toObject());

    this.nodes.put(node.getId(), exNode);
    this.jobUpdateCount.put(node.getId(), 1);
  }

  @Override
  public void updateExecutableNode(final ExecutableNode node)
      throws ExecutorManagerException {
    final ExecutableNode foundNode = this.nodes.get(node.getId());
    foundNode.setEndTime(node.getEndTime());
    foundNode.setStartTime(node.getStartTime());
    foundNode.setStatus(node.getStatus());
    foundNode.setUpdateTime(node.getUpdateTime());

    Integer value = this.jobUpdateCount.get(node.getId());
    if (value == null) {
      throw new ExecutorManagerException("The node has not been uploaded");
    } else {
      this.jobUpdateCount.put(node.getId(), ++value);
    }

    this.flowUpdateCount++;
  }

  @Override
  public int fetchNumExecutableFlows(final int projectId, final String flowId)
      throws ExecutorManagerException {
    return 0;
  }

  @Override
  public int fetchNumExecutableFlows() throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return 0;
  }

  public Integer getNodeUpdateCount(final String jobId) {
    return this.jobUpdateCount.get(jobId);
  }

  @Override
  public ExecutableJobInfo fetchJobInfo(final int execId, final String jobId, final int attempt)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean updateExecutableReference(final int execId, final long updateTime)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return true;
  }

  @Override
  public LogData fetchLogs(final int execId, final String name, final int attempt,
      final int startByte,
      final int endByte) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int skip, final int num)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final String projectContains,
      final String flowContains, final String userNameContains, final int status,
      final long startData,
      final long endData, final int skip, final int num) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
      final long startTime) throws ExecutorManagerException {
    return new ArrayList<>();
  }

  @Override
  public List<ExecutableJobInfo> fetchJobHistory(final int projectId, final String jobId,
      final int skip, final int size) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int fetchNumExecutableNodes(final int projectId, final String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Props fetchExecutionJobInputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Props fetchExecutionJobOutputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Pair<Props, Props> fetchExecutionJobProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableJobInfo> fetchJobInfoAttempts(final int execId, final String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int removeExecutionLogsByTime(final long millis)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
      final int skip, final int num, final Status status) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Object> fetchAttachments(final int execId, final String name, final int attempt)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void uploadAttachmentFile(final ExecutableNode node, final File file)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    final List<Executor> activeExecutors = new ArrayList<>();
    for (final Executor executor : this.executors) {
      if (executor.isActive()) {
        activeExecutors.add(executor);
      }
    }
    return activeExecutors;
  }

  @Override
  public Executor fetchExecutor(final String host, final int port)
      throws ExecutorManagerException {
    for (final Executor executor : this.executors) {
      if (executor.getHost().equals(host) && executor.getPort() == port) {
        return executor;
      }
    }
    return null;
  }

  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    for (final Executor executor : this.executors) {
      if (executor.getId() == executorId) {
        return executor;
      }
    }
    return null;
  }

  @Override
  public Executor addExecutor(final String host, final int port, final boolean isActive)
      throws ExecutorManagerException {
    Executor executor = null;
    if (fetchExecutor(host, port) == null) {
      this.executorIdCounter++;
      executor = new Executor(this.executorIdCounter, host, port, isActive);
      this.executors.add(executor);
    }
    return executor;
  }

  @Override
  public void removeExecutor(final String host, final int port) throws ExecutorManagerException {
    final Executor executor = fetchExecutor(host, port);
    if (executor != null) {
      this.executorIdCounter--;
      this.executors.remove(executor);
    }
  }

  @Override
  public void postExecutorEvent(final Executor executor, final EventType type, final String user,
      final String message) throws ExecutorManagerException {
    final ExecutorLogEvent event =
        new ExecutorLogEvent(executor.getId(), user, new Date(), type, message);

    if (!this.executorEvents.containsKey(executor.getId())) {
      this.executorEvents.put(executor.getId(), new ArrayList<>());
    }

    this.executorEvents.get(executor.getId()).add(event);
  }

  @Override
  public List<ExecutorLogEvent> getExecutorEvents(final Executor executor, final int num,
      final int skip) throws ExecutorManagerException {
    if (!this.executorEvents.containsKey(executor.getId())) {
      final List<ExecutorLogEvent> events = this.executorEvents.get(executor.getId());
      return events.subList(skip, Math.min(num + skip - 1, events.size() - 1));
    }
    return null;
  }

  @Override
  public void updateExecutor(final Executor executor) throws ExecutorManagerException {
    final Executor oldExecutor = fetchExecutor(executor.getId());
    this.executors.remove(oldExecutor);
    this.executors.add(executor);
  }

  @Override
  public List<Executor> fetchAllExecutors() throws ExecutorManagerException {
    return this.executors;
  }

  @Override
  public void assignExecutor(final int executorId, final int execId)
      throws ExecutorManagerException {
    final ExecutionReference ref = this.refs.get(execId);
    ref.setExecutor(fetchExecutor(executorId));
    this.executionExecutorMapping.put(execId, executorId);
  }

  @Override
  public Executor fetchExecutorByExecutionId(final int execId) throws ExecutorManagerException {
    if (this.executionExecutorMapping.containsKey(execId)) {
      return fetchExecutor(this.executionExecutorMapping.get(execId));
    } else {
      throw new ExecutorManagerException(
          "Failed to find executor with execution : " + execId);
    }
  }

  @Override
  public List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows()
      throws ExecutorManagerException {
    final List<Pair<ExecutionReference, ExecutableFlow>> queuedFlows =
        new ArrayList<>();
    for (final int execId : this.refs.keySet()) {
      if (!this.executionExecutorMapping.containsKey(execId)) {
        queuedFlows.add(new Pair<>(this.refs
            .get(execId), this.flows.get(execId)));
      }
    }
    return queuedFlows;
  }

  @Override
  public void unassignExecutor(final int executionId) throws ExecutorManagerException {
    this.executionExecutorMapping.remove(executionId);
  }

  @Override
  public List<ExecutableFlow> fetchRecentlyFinishedFlows(final Duration maxAge)
      throws ExecutorManagerException {
    return new ArrayList<>();
  }

  @Override
  public int selectAndUpdateExecution(final int executorId, final boolean isActive)
      throws ExecutorManagerException {
    return 1;
  }

  @Override
  public void unsetExecutorIdForExecution(final int executionId) {
  }
}
