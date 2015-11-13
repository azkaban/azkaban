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

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;

public class MockExecutorLoader implements ExecutorLoader {

  HashMap<Integer, Integer> executionExecutorMapping =
      new HashMap<Integer, Integer>();
  HashMap<Integer, ExecutableFlow> flows =
      new HashMap<Integer, ExecutableFlow>();
  HashMap<String, ExecutableNode> nodes = new HashMap<String, ExecutableNode>();
  HashMap<Integer, ExecutionReference> refs =
      new HashMap<Integer, ExecutionReference>();
  int flowUpdateCount = 0;
  HashMap<String, Integer> jobUpdateCount = new HashMap<String, Integer>();
  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows =
      new HashMap<Integer, Pair<ExecutionReference, ExecutableFlow>>();
  List<Executor> executors = new ArrayList<Executor>();
  int executorIdCounter = 0;
  Map<Integer, ArrayList<ExecutorLogEvent>> executorEvents =
    new HashMap<Integer, ArrayList<ExecutorLogEvent>>();

  @Override
  public void uploadExecutableFlow(ExecutableFlow flow)
      throws ExecutorManagerException {
    flows.put(flow.getExecutionId(), flow);
    flowUpdateCount++;
  }

  @Override
  public ExecutableFlow fetchExecutableFlow(int execId)
      throws ExecutorManagerException {
    ExecutableFlow flow = flows.get(execId);
    return ExecutableFlow.createExecutableFlowFromObject(flow.toObject());
  }

  @Override
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {
    return activeFlows;
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId,
      int skip, int num) throws ExecutorManagerException {
    return null;
  }

  @Override
  public void addActiveExecutableReference(ExecutionReference ref)
      throws ExecutorManagerException {
    refs.put(ref.getExecId(), ref);
  }

  @Override
  public void removeActiveExecutableReference(int execId)
      throws ExecutorManagerException {
    refs.remove(execId);
  }

  public boolean hasActiveExecutableReference(int execId) {
    return refs.containsKey(execId);
  }

  @Override
  public void uploadLogFile(int execId, String name, int attempt, File... files)
      throws ExecutorManagerException {

  }

  @Override
  public void updateExecutableFlow(ExecutableFlow flow)
      throws ExecutorManagerException {
    ExecutableFlow toUpdate = flows.get(flow.getExecutionId());

    toUpdate.applyUpdateObject((Map<String, Object>) flow.toUpdateObject(0));
    flowUpdateCount++;
  }

  @Override
  public void uploadExecutableNode(ExecutableNode node, Props inputParams)
      throws ExecutorManagerException {
    ExecutableNode exNode = new ExecutableNode();
    exNode.fillExecutableFromMapObject(node.toObject());

    nodes.put(node.getId(), exNode);
    jobUpdateCount.put(node.getId(), 1);
  }

  @Override
  public void updateExecutableNode(ExecutableNode node)
      throws ExecutorManagerException {
    ExecutableNode foundNode = nodes.get(node.getId());
    foundNode.setEndTime(node.getEndTime());
    foundNode.setStartTime(node.getStartTime());
    foundNode.setStatus(node.getStatus());
    foundNode.setUpdateTime(node.getUpdateTime());

    Integer value = jobUpdateCount.get(node.getId());
    if (value == null) {
      throw new ExecutorManagerException("The node has not been uploaded");
    } else {
      jobUpdateCount.put(node.getId(), ++value);
    }

    flowUpdateCount++;
  }

  @Override
  public int fetchNumExecutableFlows(int projectId, String flowId)
      throws ExecutorManagerException {
    return 0;
  }

  @Override
  public int fetchNumExecutableFlows() throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return 0;
  }

  public int getFlowUpdateCount() {
    return flowUpdateCount;
  }

  public Integer getNodeUpdateCount(String jobId) {
    return jobUpdateCount.get(jobId);
  }

  @Override
  public ExecutableJobInfo fetchJobInfo(int execId, String jobId, int attempt)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean updateExecutableReference(int execId, long updateTime)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return true;
  }

  @Override
  public LogData fetchLogs(int execId, String name, int attempt, int startByte,
      int endByte) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(int skip, int num)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(String projectContains,
      String flowContains, String userNameContains, int status, long startData,
      long endData, int skip, int num) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableJobInfo> fetchJobHistory(int projectId, String jobId,
      int skip, int size) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int fetchNumExecutableNodes(int projectId, String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Props fetchExecutionJobInputProps(int execId, String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Props fetchExecutionJobOutputProps(int execId, String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Pair<Props, Props> fetchExecutionJobProps(int execId, String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableJobInfo> fetchJobInfoAttempts(int execId, String jobId)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int removeExecutionLogsByTime(long millis)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId,
      int skip, int num, Status status) throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Object> fetchAttachments(int execId, String name, int attempt)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void uploadAttachmentFile(ExecutableNode node, File file)
      throws ExecutorManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    List<Executor> activeExecutors = new ArrayList<Executor>();
    for (Executor executor : executors) {
      if (executor.isActive()) {
        activeExecutors.add(executor);
      }
    }
    return activeExecutors;
  }

  @Override
  public Executor fetchExecutor(String host, int port)
    throws ExecutorManagerException {
    for (Executor executor : executors) {
      if (executor.getHost().equals(host) && executor.getPort() == port) {
        return executor;
      }
    }
    return null;
  }

  @Override
  public Executor fetchExecutor(int executorId) throws ExecutorManagerException {
    for (Executor executor : executors) {
      if (executor.getId() == executorId) {
        return executor;
      }
    }
    return null;
  }

  @Override
  public Executor addExecutor(String host, int port)
    throws ExecutorManagerException {
    Executor executor = null;
    if (fetchExecutor(host, port) == null) {
      executorIdCounter++;
      executor = new Executor(executorIdCounter, host, port, true);
      executors.add(executor);
    }
    return executor;
  }

  @Override
  public void postExecutorEvent(Executor executor, EventType type, String user,
    String message) throws ExecutorManagerException {
    ExecutorLogEvent event =
      new ExecutorLogEvent(executor.getId(), user, new Date(), type, message);

    if (!executorEvents.containsKey(executor.getId())) {
      executorEvents.put(executor.getId(), new ArrayList<ExecutorLogEvent>());
    }

    executorEvents.get(executor.getId()).add(event);
  }

  @Override
  public List<ExecutorLogEvent> getExecutorEvents(Executor executor, int num,
    int skip) throws ExecutorManagerException {
    if (!executorEvents.containsKey(executor.getId())) {
      List<ExecutorLogEvent> events = executorEvents.get(executor.getId());
      return events.subList(skip, Math.min(num + skip - 1, events.size() - 1));
    }
    return null;
  }

  @Override
  public void updateExecutor(Executor executor) throws ExecutorManagerException {
    Executor oldExecutor = fetchExecutor(executor.getId());
    executors.remove(oldExecutor);
    executors.add(executor);
  }

  @Override
  public List<Executor> fetchAllExecutors() throws ExecutorManagerException {
    return executors;
  }

  @Override
  public void assignExecutor(int executorId, int execId)
    throws ExecutorManagerException {
    ExecutionReference ref = refs.get(execId);
    ref.setExecutor(fetchExecutor(executorId));
    executionExecutorMapping.put(execId, executorId);
  }

  @Override
  public Executor fetchExecutorByExecutionId(int execId) throws ExecutorManagerException {
    if (executionExecutorMapping.containsKey(execId)) {
      return fetchExecutor(executionExecutorMapping.get(execId));
    } else {
      throw new ExecutorManagerException(
        "Failed to find executor with execution : " + execId);
    }
  }

  @Override
  public List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows()
    throws ExecutorManagerException {
    List<Pair<ExecutionReference, ExecutableFlow>> queuedFlows =
      new ArrayList<Pair<ExecutionReference, ExecutableFlow>>();
    for (int execId : refs.keySet()) {
      if (!executionExecutorMapping.containsKey(execId)) {
        queuedFlows.add(new Pair<ExecutionReference, ExecutableFlow>(refs
          .get(execId), flows.get(execId)));
      }
    }
    return queuedFlows;
  }

  @Override
  public void unassignExecutor(int executionId) throws ExecutorManagerException {
    executionExecutorMapping.remove(executionId);
  }
}
