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

import static azkaban.Constants.LogConstants.NEARLINE_LOGS;

import azkaban.DispatchMethod;
import azkaban.event.EventListener;
import azkaban.logs.ExecutionLogsLoader;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.project.ProjectManager;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collection;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
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
  private final ExecutorHealthChecker executorHealthChecker;


  @Inject
  public ExecutionController(final Props azkProps,
      final ProjectManager projectManager, final ExecutorLoader executorLoader,
      @Named(NEARLINE_LOGS) final ExecutionLogsLoader executionLogsLoader,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway, final AlerterHolder alerterHolder, final
  ExecutorHealthChecker executorHealthChecker, final EventListener eventListener,
      final ContainerizationMetrics containerizationMetrics) {
    super(azkProps, projectManager, executorLoader, executionLogsLoader, commonMetrics, apiGateway,
        alerterHolder, eventListener, containerizationMetrics);
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
          .fetchActiveFlows(DispatchMethod.POLL).values()) {
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

  @SuppressWarnings("unchecked")
  private Map<String, Object> modifyExecutingJobs(final ExecutableFlow exFlow,
      final String command, final String userId, final String... jobIds)
      throws ExecutorManagerException {
    synchronized (exFlow) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.executorLoader.fetchActiveFlowByExecId(exFlow.getExecutionId());
      return modifyExecutingJobs(exFlow, command, userId, pair, jobIds);
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
