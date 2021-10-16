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

import azkaban.DispatchMethod;
import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used in unit tests to mock the "DB layer" (the real implementation is JdbcExecutorLoader).
 * Captures status updates of jobs and flows (in memory) so that they can be checked in tests.
 */
public class MockExecutorLoader implements ExecutorLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockExecutorLoader.class);

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
  public void uploadExecutableFlow(final ExecutableFlow flow) throws ExecutorManagerException {
    // Clone the flow node to mimick how it would be saved in DB.
    // If we would keep a handle to the original flow node, we would also see any changes made after
    // this method was called. We must only store a snapshot of the current state.
    // Also to avoid modifying statuses of the original job nodes in this.updateExecutableFlow()
    final ExecutableFlow exFlow = ExecutableFlow.createExecutableFlow(flow.toObject(),
        flow.getStatus());
    this.flows.put(flow.getExecutionId(), exFlow);
    this.flowUpdateCount++;
  }

  @Override
  public ExecutableFlow fetchExecutableFlow(final int execId) throws ExecutorManagerException {
    final ExecutableFlow flow = this.flows.get(execId);
    return ExecutableFlow.createExecutableFlow(flow.toObject(), flow.getStatus());
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
        LOGGER.info("Uploaded log for [" + name + "]:[" + execId + "]:\n" + logs);
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
  public int removeExecutionLogsByTime(final long millis, final int recordCleanupLimit)
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
    return fetchQueuedFlows(Status.PREPARING);
  }

  @Override
  public List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows(Status status)
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
  public List<ExecutableFlow> fetchStaleFlowsForStatus(final Status status,
      final ImmutableMap<Status, Pair<Duration, String>> validityMap)
      throws ExecutorManagerException {
    throw new ExecutorManagerException("Method Not Implemented!");
  }

  @Override
  // TODO(anish-mal) To be used in a future unit test, once System calls to obtain
  // current time have been replaced by Clocks. Clocks are needed in order to write
  // unit tests for duration based features. Without it, the tests end up being flaky.
  public List<ExecutableFlow> fetchAgedQueuedFlows(final Duration minAge)
      throws ExecutorManagerException {
    final List<ExecutableFlow> agedQueuedFlows = new ArrayList<>();

    long timeThreshoold = System.currentTimeMillis() - minAge.toMillis();
    for (final int execId : this.refs.keySet()) {
      if (!this.executionExecutorMapping.containsKey(execId)) {
        ExecutableFlow agedFlow = this.flows.get(execId);
        if (agedFlow.getSubmitTime() < timeThreshoold) {
          agedQueuedFlows.add(agedFlow);
        }
      }
    }
    return agedQueuedFlows;
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
  public int selectAndUpdateExecution(final int executorId, final boolean isActive,
      final DispatchMethod dispatchMethod)
      throws ExecutorManagerException {
    return 1;
  }

  @Override
  public int selectAndUpdateExecutionWithLocking(final int executorId, final boolean isActive,
      final DispatchMethod dispatchMethod)
      throws ExecutorManagerException {
    return 1;
  }

  @Override
  public Set<Integer> selectAndUpdateExecutionWithLocking(final boolean batchEnabled,
      final int limit,
      final Status updatedStatus,
      final DispatchMethod dispatchMethod) throws ExecutorManagerException {
    final Set<Integer> executions = new HashSet<>();
    executions.add(1);
    return executions;
  }

  @Override
  public ExecutableRampMap fetchExecutableRampMap() throws ExecutorManagerException {
    ExecutableRampMap map = ExecutableRampMap.createInstance();
    map.add("rampId",
        ExecutableRamp.builder("dali", "RampPolicy")
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(5)
                .setMaxFailureToRampDown(10)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(0)
                .setEndTime(0)
                .setLastUpdatedTime(0)
                .setNumOfTrail(0)
                .setNumOfSuccess(0)
                .setNumOfFailure(0)
                .setNumOfIgnored(0)
                .setPaused(false)
                .setRampStage(0)
                .setActive(true)
                .build())
            .build()
    );

    return map;
  }

  @Override
  public ExecutableRampItemsMap fetchExecutableRampItemsMap() throws ExecutorManagerException {
    ExecutableRampItemsMap map = ExecutableRampItemsMap.createInstance();
    map.add("rampId", "dependencyId", "defaultValue");
    return map;
  }

  @Override
  public ExecutableRampDependencyMap fetchExecutableRampDependencyMap() throws ExecutorManagerException {
    ExecutableRampDependencyMap map = ExecutableRampDependencyMap.createInstance();
    map.add("dependency", "defaultValue", "pig,hive,spark");
    return map;
  }

  @Override
  public ExecutableRampExceptionalFlowItemsMap fetchExecutableRampExceptionalFlowItemsMap() throws ExecutorManagerException {
    ExecutableRampExceptionalFlowItemsMap map = ExecutableRampExceptionalFlowItemsMap.createInstance();
    map.add("rampId", "flowId", ExecutableRampStatus.SELECTED, 15500000L);
    return map;
  }

  @Override
  public ExecutableRampExceptionalJobItemsMap fetchExecutableRampExceptionalJobItemsMap() throws ExecutorManagerException {
    ExecutableRampExceptionalJobItemsMap map = ExecutableRampExceptionalJobItemsMap.createInstance();
    map.add("rampId", "flowId", "jobId", ExecutableRampStatus.UNSELECTED, 15000000L);
    return map;
  }

  @Override
  public void updateExecutedRampFlows(final String ramp, ExecutableRampExceptionalItems executableRampExceptionalItems)
      throws ExecutorManagerException {

  }

  @Override
  public void updateExecutableRamp(ExecutableRamp executableRamp) throws ExecutorManagerException {

  }

  @Override
  public int updateVersionSetId(int executionId, int versionSetId) throws ExecutorManagerException {
    return 0;
  }

  @Override
  public Map<String, String> doRampActions(List<Map<String, Object>> rampActionsMap) throws ExecutorManagerException {
    return null;
  }

  @Override
  public void unsetExecutorIdForExecution(final int executionId) {
  }
}
