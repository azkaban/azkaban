/*
 * Copyright 2018 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.executor;

import azkaban.DispatchMethod;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controls flow executions on web server. This module implements the polling model in the new AZ
 * dispatching design. It's injected only when azkaban.execution.dispatch.method is configured to
 * POLL. It will ultimately replace ExecutorManager in the future.
 */
@Singleton
public class ExecutionController extends AbstractExecutorManagerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(ExecutionController.class);
  private final AlerterHolder alerterHolder;
  private final ExecutorHealthChecker executorHealthChecker;


  @Inject
  ExecutionController(final Props azkProps, final ExecutorLoader executorLoader,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway, final AlerterHolder alerterHolder, final
  ExecutorHealthChecker executorHealthChecker) {
    super(azkProps, executorLoader, commonMetrics, apiGateway);
    this.alerterHolder = alerterHolder;
    this.executorHealthChecker = executorHealthChecker;
  }

  @Override
  public void setupExecutors() {
    // Todo: deprecate this method
  }

  @Override
  public void disableQueueProcessorThread() {
    // Todo: deprecate this method
  }

  @Override
  public void enableQueueProcessorThread() {
    // Todo: deprecate this method
  }

  @Override
  public State getExecutorManagerThreadState() {
    // Todo: deprecate this method
    return State.RUNNABLE;
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    // Todo: deprecate this method
    return true;
  }

  @Override
  public long getLastExecutorManagerThreadCheckTime() {
    // Todo: deprecate this method
    return 1L;
  }

  @Override
  public Collection<Executor> getAllActiveExecutors() {
    List<Executor> executors = new ArrayList<>();
    try {
      executors = this.executorLoader.fetchActiveExecutors();
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get all active executors.", e);
    }
    return executors;
  }

  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    return this.executorLoader.fetchExecutor(executorId);
  }

  @Override
  public Set<String> getPrimaryServerHosts() {
    final HashSet<String> ports = new HashSet<>();
    try {
      for (final Executor executor : this.executorLoader.fetchActiveExecutors()) {
        ports.add(executor.getHost() + ":" + executor.getPort());
      }
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get primary server hosts.", e);
    }
    return ports;
  }

  @Override
  public Set<String> getAllActiveExecutorServerHosts() {
    final Set<String> ports = getPrimaryServerHosts();
    // include executor which were initially active and still has flows running
    try {
      for (final Pair<ExecutionReference, ExecutableFlow> running : this.executorLoader
          .fetchActiveFlows().values()) {
        final ExecutionReference ref = running.getFirst();
        if (ref.getExecutor().isPresent()) {
          final Executor executor = ref.getExecutor().get();
          ports.add(executor.getHost() + ":" + executor.getPort());
        }
      }
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get all active executor server hosts.", e);
    }
    return ports;
  }

  /**
   * Gets a list of all the unfinished (both dispatched and non-dispatched) executions for a given
   * project and flow {@inheritDoc}.
   *
   * @see azkaban.executor.ExecutorManagerAdapter#getRunningFlows(int, java.lang.String)
   */
  @Override
  public List<Integer> getRunningFlows(final int projectId, final String flowId) {
    final List<Integer> executionIds = new ArrayList<>();
    try {
      executionIds.addAll(ExecutorUtils.getRunningFlowsHelper(projectId, flowId,
          this.executorLoader.fetchUnfinishedFlows().values()));
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get running flows for project " + projectId + ", flow "
          + flowId, e);
    }
    return executionIds;
  }

  @Override
  public List<Pair<ExecutableFlow, Optional<Executor>>> getActiveFlowsWithExecutor() {
    final List<Pair<ExecutableFlow, Optional<Executor>>> flows = new ArrayList<>();
    try {
      getActiveFlowsWithExecutorHelper(flows, this.executorLoader.fetchUnfinishedFlows().values());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get active flows with executor.", e);
    }
    return flows;
  }

  /* Helper method for getActiveFlowsWithExecutor */
  private void getActiveFlowsWithExecutorHelper(
      final List<Pair<ExecutableFlow, Optional<Executor>>> flows,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      flows.add(new Pair<>(ref.getSecond(), ref
          .getFirst().getExecutor()));
    }
  }

  /**
   * Checks whether the given flow has an active (running, non-dispatched) execution from database.
   * {@inheritDoc}
   */
  @Override
  public boolean isFlowRunning(final int projectId, final String flowId) {
    boolean isRunning = false;
    try {
      isRunning = isFlowRunningHelper(projectId, flowId,
          this.executorLoader.fetchUnfinishedFlows().values());

    } catch (final ExecutorManagerException e) {
      logger.error(
          "Failed to check if the flow is running for project " + projectId + ", flow " + flowId,
          e);
    }
    return isRunning;
  }

  /**
   * When a flow is submitted, insert a new execution into the database queue. {@inheritDoc}
   */
  @Override
  public String submitExecutableFlow(final ExecutableFlow exflow, final String userId)
      throws ExecutorManagerException {
    if (exflow.isLocked()) {
      // Skip execution for locked flows.
      final String message = String.format("Flow %s for project %s is locked.", exflow.getId(),
          exflow.getProjectName());
      logger.info(message);
      return message;
    }

    final String exFlowKey = exflow.getProjectName() + "." + exflow.getId() + ".submitFlow";
    // Use project and flow name to prevent race condition when same flow is submitted by API and
    // schedule at the same time
    // causing two same flow submission entering this piece.
    synchronized (exFlowKey.intern()) {
      final String flowId = exflow.getFlowId();
      logger.info("Submitting execution flow " + flowId + " by " + userId);

      String message = uploadExecutableFlow(exflow, userId, flowId, "");

      this.commonMetrics.markSubmitFlowSuccess();
      message += "Execution queued successfully with exec id " + exflow.getExecutionId();
      return message;
    }
  }

  /* Search a running flow in a collection */
  private boolean isFlowRunningHelper(final int projectId, final String flowId,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    for (final Pair<ExecutionReference, ExecutableFlow> ref : collection) {
      if (ref.getSecond().getProjectId() == projectId
          && ref.getSecond().getFlowId().equals(flowId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get all running (unfinished) flows from database. {@inheritDoc}
   */
  @Override
  public List<ExecutableFlow> getRunningFlows() {
    final ArrayList<ExecutableFlow> flows = new ArrayList<>();
    try {
      getFlowsHelper(flows, this.executorLoader.fetchUnfinishedFlows().values());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get running flows.", e);
    }
    return flows;
  }

  /**
   * Helper method to get all flows from collection.
   */
  private void getFlowsHelper(final ArrayList<ExecutableFlow> flows,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    collection.forEach(ref -> flows.add(ref.getSecond()));
  }

  /**
   * Get execution ids of all running (unfinished) flows from database.
   */
  public List<Integer> getRunningFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    try {
      getExecutionIdsHelper(allIds, this.executorLoader.fetchUnfinishedFlows().values());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get running flow ids.", e);
    }
    return allIds;
  }

  /**
   * Get execution ids of all non-dispatched flows from database.
   */
  public List<Integer> getQueuedFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    try {
      getExecutionIdsHelper(allIds, this.executorLoader.fetchQueuedFlows());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get queued flow ids.", e);
    }
    return allIds;
  }

  /* Helper method to get all execution ids from collection in sorted order. */
  private void getExecutionIdsHelper(final List<Integer> allIds,
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection) {
    collection.forEach(ref -> allIds.add(ref.getSecond().getExecutionId()));
    Collections.sort(allIds);
  }

  /**
   * Get the number of non-dispatched flows from database. {@inheritDoc}
   */
  @Override
  public long getQueuedFlowSize() {
    long size = 0L;
    // TODO(anish-mal) FetchQueuedExecutableFlows does a lot of processing that is redundant, since
    // all we care about is the count. Write a new class that's more performant and can be used for
    // metrics. this.executorLoader.fetchQueuedFlows internally calls FetchQueuedExecutableFlows.
    try {
      size = this.executorLoader.fetchQueuedFlows().size();
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get queued flow size.", e);
    }
    return size;
  }

  @Override
  public DispatchMethod getDispatchMethod() {
    return DispatchMethod.POLL;
  }

  @Override
  public LogData getExecutableFlowLog(final ExecutableFlow exFlow, final int offset,
      final int length) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair = this.executorLoader
        .fetchActiveFlowByExecId(exFlow.getExecutionId());
    return getFlowLogData(exFlow, offset, length, pair);
  }

  @Override
  public LogData getExecutionJobLog(final ExecutableFlow exFlow, final String jobId,
      final int offset, final int length, final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair = this.executorLoader
        .fetchActiveFlowByExecId(exFlow.getExecutionId());
    return getJobLogData(exFlow, jobId, offset, length, attempt, pair);
  }

  @Override
  public List<Object> getExecutionJobStats(final ExecutableFlow exFlow, final String jobId,
      final int attempt) throws ExecutorManagerException {
    final Pair<ExecutionReference, ExecutableFlow> pair =
        this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
    return getExecutionJobStats(exFlow, jobId, attempt, pair);
  }

  /**
   * If a flow is already dispatched to an executor, cancel by calling Executor. Else if it's still
   * queued in DB, remove it from DB queue and finalize. {@inheritDoc}
   */
  @Override
  public void cancelFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows = this.executorLoader
          .fetchUnfinishedFlows();
      if (unfinishedFlows.containsKey(exFlow.getExecutionId())) {
        final Pair<ExecutionReference, ExecutableFlow> pair = unfinishedFlows
            .get(exFlow.getExecutionId());
        if (pair.getFirst().getExecutor().isPresent()) {
          // Flow is already dispatched to an executor, so call that executor to cancel the flow.
          this.apiGateway
              .callWithReferenceByUser(pair.getFirst(), ConnectorParams.CANCEL_ACTION, userId);
        } else {
          // Flow is still queued, need to finalize it and update the status in DB.
          ExecutionControllerUtils.finalizeFlow(this.executorLoader, this.alerterHolder, exFlow,
              "Cancelled before dispatching to executor", null);
        }
      } else {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
    }
  }

  @Override
  public void resumeFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
      if (pair == null) {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
      this.apiGateway
          .callWithReferenceByUser(pair.getFirst(), ConnectorParams.RESUME_ACTION, userId);
    }
  }

  @Override
  public void pauseFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
      if (pair == null) {
        throw new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId()
            + " isn't running.");
      }
      this.apiGateway
          .callWithReferenceByUser(pair.getFirst(), ConnectorParams.PAUSE_ACTION, userId);
    }
  }

  @Override
  public void retryFailures(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_FAILURES, userId);
  }

  private void modifyExecutingJobs(final ExecutableFlow exFlow,
      final String command, final String userId, final String... jobIds)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
      modifyExecutingJobs(exFlow, command, userId, pair, jobIds);
    }
  }

  @Override
  public void start() {
    this.executorHealthChecker.start();
  }

  @Override
  public void shutdown() {
    this.executorHealthChecker.shutdown();
  }

}
