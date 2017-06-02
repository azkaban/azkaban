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
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockExecutorLoader implements ExecutorLoader {

  HashMap<Integer, Integer> executionExecutorMapping =
      new HashMap<>();
  HashMap<Integer, ExecutableFlow> flows =
      new HashMap<>();
  HashMap<String, ExecutableNode> nodes = new HashMap<>();
  HashMap<Integer, ExecutionReference> refs =
      new HashMap<>();
  int flowUpdateCount = 0;
  HashMap<String, Integer> jobUpdateCount = new HashMap<>();
  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows =
      new HashMap<>();
  List<Executor> executors = new ArrayList<>();
  int executorIdCounter = 0;
  Map<Integer, ArrayList<ExecutorLogEvent>> executorEvents =
      new HashMap<>();

  @Override
  public void uploadExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    this.flows.put(flow.getExecutionId(), flow);
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
  public Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(final int execId)
      throws ExecutorManagerException {
    return this.activeFlows.get(execId);
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

  public boolean hasActiveExecutableReference(final int execId) {
    return this.refs.containsKey(execId);
  }

  @Override
  public void uploadLogFile(final int execId, final String name, final int attempt,
      final File... files)
      throws ExecutorManagerException {

  }

  @Override
  public void updateExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    final ExecutableFlow toUpdate = this.flows.get(flow.getExecutionId());

    toUpdate.applyUpdateObject((Map<String, Object>) flow.toUpdateObject(0));
    this.flowUpdateCount++;
  }

  @Override
  public void uploadExecutableNode(final ExecutableNode node, final Props inputParams)
      throws ExecutorManagerException {
    final ExecutableNode exNode = new ExecutableNode();
    exNode.fillExecutableFromMapObject(node.toObject());

    this.nodes.put(node.getId(), exNode);
    this.jobUpdateCount.put(node.getId(), 1);
  }

  @Override
  public void updateExecutableNode(final ExecutableNode node)
      throws ExecutorManagerException {
    if (!this.nodes.containsKey(node.getId())) {
      uploadExecutableNode(node, new Props());
    }
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

  public int getFlowUpdateCount() {
    return this.flowUpdateCount;
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
  public Executor addExecutor(final String host, final int port)
      throws ExecutorManagerException {
    Executor executor = null;
    if (fetchExecutor(host, port) == null) {
      this.executorIdCounter++;
      executor = new Executor(this.executorIdCounter, host, port, true);
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
}
