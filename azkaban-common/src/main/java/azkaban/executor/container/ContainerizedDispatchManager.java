/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.executor.container;

import static azkaban.Constants.LogConstants.NEARLINE_LOGS;

import azkaban.Constants;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.Constants.FlowParameters;
import azkaban.DispatchMethod;
import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventListener;
import azkaban.executor.AbstractExecutorManagerAdapter;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ConnectorParams;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionReference;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorApiGateway;
import azkaban.executor.ExecutorHealthChecker;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.logs.ExecutionLogsLoader;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.project.ProjectManager;
import azkaban.spi.EventType;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to dispatch execution on Containerized infrastructure. Each execution will be
 * dispatched in single container. This way, it will bring isolation between all the executions.
 * When the flow will be executed or triggered by schedule, it will be added in a queue maintained
 * in database. The state of execution will be READY in case of Containerized dispatch. When the
 * flow will be picked up by @{@link QueueProcessorThread} to dispatch it to containerized
 * infrastructure, it will be marked as DISPATCHING. Once the container creation request is
 * sent, it will be marked as PREPARING. When a flow will be ready to run on container, it will
 * be marked as RUNNING. In case of failure in dispatch, it will move back to READY state in queue.
 */
@Singleton
public class ContainerizedDispatchManager extends AbstractExecutorManagerAdapter {
  private final ContainerizedImpl containerizedImpl;
  private QueueProcessorThread queueProcessor;
  private final RateLimiter rateLimiter;
  private static final Logger logger = LoggerFactory.getLogger(ContainerizedDispatchManager.class);
  private final ContainerJobTypeCriteria containerJobTypeCriteria;
  private final ContainerRampUpCriteria containerRampUpCriteria;
  private final ContainerProxyUserCriteria containerProxyUserCriteria;
  private final Optional<ContainerizedWatch> containerizedWatch;
  private final Optional<ExecutorHealthChecker> executorHealthChecker;

  @Inject
  public ContainerizedDispatchManager(
      final Props azkProps,
      final ProjectManager projectManager,
      final ExecutorLoader executorLoader,
      @Named(NEARLINE_LOGS) final ExecutionLogsLoader executionLogsLoader,
      final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway,
      final ContainerizedImpl containerizedImpl,
      final AlerterHolder alerterHolder,
      final ContainerizedWatch containerizedWatch,
      final EventListener eventListener,
      final ContainerizationMetrics containerizationMetrics,
      @Nullable final ExecutorHealthChecker executorHealthChecker)
      throws ExecutorManagerException {
    super(azkProps, projectManager, executorLoader, executionLogsLoader, commonMetrics, apiGateway,
        alerterHolder, eventListener, containerizationMetrics);
    rateLimiter =
        RateLimiter.create(azkProps
            .getInt(ContainerizedDispatchManagerProperties.CONTAINERIZED_CREATION_RATE_LIMIT, 20));
    this.containerizedImpl = containerizedImpl;
    this.containerizedWatch = Optional.ofNullable(containerizedWatch);
    this.containerJobTypeCriteria = new ContainerJobTypeCriteria(azkProps);
    this.containerRampUpCriteria = new ContainerRampUpCriteria(azkProps);
    this.containerProxyUserCriteria = new ContainerProxyUserCriteria(azkProps);
    this.executorHealthChecker = Optional.ofNullable(executorHealthChecker);
  }

  public ContainerJobTypeCriteria getContainerJobTypeCriteria() {
    return this.containerJobTypeCriteria;
  }

  public ContainerRampUpCriteria getContainerRampUpCriteria() {
    return this.containerRampUpCriteria;
  }

  public ContainerProxyUserCriteria getContainerProxyUserCriteria(){
    return this.containerProxyUserCriteria;
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
   * Get queued flow ids from database. The status for queued flows is READY for containerization.
   *
   * @return
   */
  public List<Integer> getQueuedFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    try {
      getExecutionIdsHelper(allIds, this.executorLoader.fetchQueuedFlows(Status.READY));
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get queued flow ids.", e);
    }
    return allIds;
  }

  /**
   * Get size of queued flows. The status for queued flows is READY for containerization.
   *
   * @return
   */
  @Override
  public long getQueuedFlowSize() {
    return getQueuedFlowIds().size();
  }

  /**
   * Get dispatch method enum for this class.
   *
   * @return
   */
  @Override
  public DispatchMethod getDispatchMethod() {
    return this.containerRampUpCriteria.getDispatchMethod();
  }

  /**
   * This method is used to get dispatch method for a flow based on multiple criteria mentioned
   * below.
   * a) If @Constants.FlowParameters.FLOW_PARAM_DISPATCH_EXECUTION_TO_CONTAINER is set to true
   * then dispatch method should be containerized
   * b) If @ExecutionOptions.USE_EXECUTOR is set then dispatch method should be POLL.
   * c) If based on deterministic ramp up, dispatch method is anything other than containerized
   * then that should be returned.
   * d) If deterministic ramp up chooses dispatch method as containerized then validate all the
   * jobtypes in a flow are in allow list of job criteria. Return containerized if all the
   * jobtypes for a flow are allowed. Else, return POLL.
   * @param flow
   * @return
   */
  @Override
  public DispatchMethod getDispatchMethod(final ExecutableFlow flow) {
    ExecutionOptions executionOptions = flow.getExecutionOptions();
    if (executionOptions != null) {
      final Map<String, String> flowParam = executionOptions.getFlowParameters();
      if (flowParam != null && !flowParam.isEmpty()) {
        if (flowParam
            .containsKey(Constants.FlowParameters.FLOW_PARAM_DISPATCH_EXECUTION_TO_CONTAINER)) {
          logger.info(FlowParameters.FLOW_PARAM_DISPATCH_EXECUTION_TO_CONTAINER + " flow param is"
              + " specified for " + flow.getFlowName() + ". It's value is set to "
              + flowParam
              .get(Constants.FlowParameters.FLOW_PARAM_DISPATCH_EXECUTION_TO_CONTAINER));
          if (Boolean.valueOf(flowParam
              .get(Constants.FlowParameters.FLOW_PARAM_DISPATCH_EXECUTION_TO_CONTAINER))) {
            return DispatchMethod.CONTAINERIZED;
          } else {
            return DispatchMethod.POLL;
          }
        }
        if (flowParam.containsKey(ExecutionOptions.USE_EXECUTOR)) {
          logger.info(ExecutionOptions.USE_EXECUTOR + " flow param is"
              + " specified for " + flow.getFlowName() + ". It's value is set to "
              + flowParam.get(ExecutionOptions.USE_EXECUTOR));
          return DispatchMethod.POLL;
        }
      }
    }

    DispatchMethod dispatchMethod = this.containerRampUpCriteria.getDispatchMethod(flow);
    logger.info("Dispatch method by proxy user criteria is",
        dispatchMethod.name() + " for " + flow.getFlowName() + " flow.");
    if (dispatchMethod != DispatchMethod.CONTAINERIZED) {
      return dispatchMethod;
    }
    dispatchMethod = this.containerJobTypeCriteria.getDispatchMethod(flow);
    logger.info("Dispatch method by job type criteria is",
        dispatchMethod.name() + " for " + flow.getFlowName() + " flow.");
    if (dispatchMethod != DispatchMethod.CONTAINERIZED) {
      return dispatchMethod;
    }
    return this.containerProxyUserCriteria.getDispatchMethod(flow);
  }

  /**
   * This is responsible for starting processing loops for the containerized dispatch.
   * Currently this includes:
   * (1) This method is used to start queue processor thread. The queue processor thread will pick
   *   execution from queue maintained in database and create a container for it.
   * (2) Starting container event watch, if present.
   */
  @Override
  public void start() {
    this.queueProcessor = setupQueueProcessor();
    this.queueProcessor.start();
    startWatch();
    this.executorHealthChecker.ifPresent(ehc -> ehc.start());
  }

  // Start the event watch if configured.
  private void startWatch() {
    if (!containerizedWatch.isPresent()) {
      logger.info("Containerized watch was not provided and will not be started");
      return;
    }
    if (this.azkProps.getBoolean(ContainerizedDispatchManagerProperties.KUBERNETES_WATCH_ENABLED,
        false)) {
      logger.info("Starting containerized watch.");
      this.containerizedWatch.get().launchWatch();
    } else {
      logger.info("Containerized watch was not started as the config {} is not true.",
          ContainerizedDispatchManagerProperties.KUBERNETES_WATCH_ENABLED);
    }
  }

  private QueueProcessorThread setupQueueProcessor() {
    return new QueueProcessorThread(
        this.azkProps, this.executorLoader);
  }

  /**
   * Get the status for executions maintained in queue. For other dispatch implementations, the
   * status for executions in queue is PREPARING. But for containerized dispatch method, the status
   * will be READY.
   *
   * @return
   */
  @Override
  public Status getStartStatus() {
    return Status.READY;
  }

  /**
   * @param execFlow {@link ExecutableFlow} containing all the information for a flow execution
   *                                       including dispatch method which will be utilized here.
   * @return READY if the dispatchMethod is CONTAINERIZED otherwise PREPARING.
   */
  @Override
  public Status getStartStatus(ExecutableFlow execFlow) {
    if (execFlow.getDispatchMethod() == DispatchMethod.CONTAINERIZED) {
      return Status.READY;
    } else {
      return Status.PREPARING;
    }
  }

  /**
   * This method will shutdown the queue processor thread. It won't pick up executions from queue
   * for dispatch after this. This method will be called when webserver will shutdown.
   */
  @Override
  public void shutdown() {
    logger.info("Shutting down queue processor thread for containerized dispatch implementation.");
    if (null != this.queueProcessor) {
      this.queueProcessor.shutdown();
    }

    if (this.azkProps.getBoolean(ContainerizedDispatchManagerProperties.KUBERNETES_WATCH_ENABLED,
        false) && containerizedWatch.isPresent()) {
      logger.info("Shutting down containerized watch");
      containerizedWatch.get().requestShutdown();
    }

    this.executorHealthChecker.ifPresent(ehc -> ehc.shutdown());
  }

  /**
   * This method is used to enable queue processor thread. It will resume dispatching executions.
   * Due to any maintenance of containerized infrastructure, if queue processor was disabled then it
   * can enabled again using this method.
   */
  @Override
  public void enableQueueProcessorThread() {
    this.queueProcessor.setActive(true);
  }

  /**
   * This method is used to disable queue processor thread. It will stop dispatching executions. In
   * case of maintenance of containerized infrastructure, this method can be used to disable queue
   * processor. It will disable dispatch of executions in containers.
   */
  @Override
  public void disableQueueProcessorThread() {
    this.queueProcessor.setActive(false);
  }

  public State getQueueProcessorThreadState() {
    return this.queueProcessor.getState();
  }

  public boolean isQueueProcessorThreadActive() {
    return this.queueProcessor.isActive();
  }

  /**
   * This QueueProcessorThread will fetch executions from database in batch/single execution at a
   * time and dispatch it in container.
   */
  private class QueueProcessorThread extends Thread {

    private final Props azkProps;
    private volatile boolean shutdown = false;
    private volatile boolean isActive = true;
    private ExecutorService executorService;
    private ExecutorLoader executorLoader;
    private boolean executionsBatchProcessingEnabled;
    private int executionsBatchSize;

    public QueueProcessorThread(final Props azkProps, final ExecutorLoader executorLoader) {
      this.azkProps = azkProps;
      this.executorLoader = executorLoader;
      setActive(
          this.azkProps.getBoolean(Constants.ConfigurationKeys.QUEUEPROCESSING_ENABLED, true));
      this.executionsBatchProcessingEnabled = azkProps
          .getBoolean(ContainerizedDispatchManagerProperties.CONTAINERIZED_EXECUTION_BATCH_ENABLED,
              false);
      this.executionsBatchSize =
          azkProps
              .getInt(ContainerizedDispatchManagerProperties.CONTAINERIZED_EXECUTION_BATCH_SIZE,
                  10);
      this.executorService = Executors.newFixedThreadPool(azkProps.getInt(
          ContainerizedDispatchManagerProperties.CONTAINERIZED_EXECUTION_PROCESSING_THREAD_POOL_SIZE,
          10));
      this.setName("Containerized-QueueProcessor-Thread");
    }

    @Override
    public void run() {
      // Loops till QueueProcessorThread is shutdown
      while (!this.shutdown) {
        try {
          // Start processing queue if active, otherwise wait for sometime
          if (this.isActive) {
            processQueuedFlows();
          }
        } catch (final Exception e) {
          ContainerizedDispatchManager.logger.error(
              "QueueProcessorThread Interrupted. Probably to shut down.", e);
        }
      }
    }

    /**
     * This method is responsible for dispatching the executions in queue in READY state. It will
     * fetch single execution or in batch based on the property. The batch size can also be defined
     * in property.
     *
     * @throws ExecutorManagerException
     */
    private void processQueuedFlows() throws ExecutorManagerException {
      final Set<Integer> executionIds =
          executorLoader.selectAndUpdateExecutionWithLocking(this.executionsBatchProcessingEnabled,
              this.executionsBatchSize,
              Status.DISPATCHING, DispatchMethod.CONTAINERIZED);

      for (final int executionId : executionIds) {
        rateLimiter.acquire();
        logger.info("Starting dispatch for {} execution.", executionId);
        Runnable worker = new ExecutionDispatcher(executionId);
        // Fetch an executable flow based on execution id and report an dispatching event
        final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(executionId);
        ContainerizedDispatchManager.this.fireEventListeners(Event.create(flow,
            EventType.FLOW_STATUS_CHANGED,
            new EventData(flow)));

        executorService.execute(worker);
      }
    }

    public boolean isActive() {
      return this.isActive;
    }

    public void setActive(final boolean isActive) {
      this.isActive = isActive;
      ContainerizedDispatchManager.logger
          .info("QueueProcessorThread turned " + this.isActive);
    }

    /**
     * When queue process is shutting down, executor service for dispatch needs shutdown too.
     */
    public void shutdown() {
      this.shutdown = true;
      this.executorService.shutdown();
      this.interrupt();
    }
  }

  @VisibleForTesting
  public ExecutionDispatcher getExecutionDispatcher(int execId) {
    return new ExecutionDispatcher(execId);
  }

  /**
   * This class is a worker class to dispatch execution in container.
   */
  private class ExecutionDispatcher implements Runnable {

    private final int executionId;

    ExecutionDispatcher(final int executionId) {
      this.executionId = executionId;
    }

    @Override
    public void run() {
      try {
        logger.info("Creating a container for {}", executionId);
        long startTime = System.currentTimeMillis();
        // Create a container for execution id. The container creation will throw exception if it
        //is not able read template files, unable to parse files, unable to replace dynamic
        //variables etc.
        ContainerizedDispatchManager.this.containerizedImpl.createContainer(executionId);
        logger.info("Time taken to dispatch a container for {} is {} seconds", executionId,
            (System.currentTimeMillis() - startTime) / 1000);
      } catch (ExecutorManagerException e) {
        logger.info("Unable to dispatch container in Kubernetes for : {}", executionId);
        logger.info("Reason for dispatch failure: {}", e.getMessage());
        // Finalize the flow if the dispatch failed.
        try {
          ContainerizedDispatchManager.this.containerizationMetrics.markContainerDispatchFail();
          ExecutableFlow execFlow =
              ContainerizedDispatchManager.this.executorLoader.fetchExecutableFlow(executionId);
          Status originalStatus = execFlow.getStatus();
          ExecutionControllerUtils.finalizeFlow(ContainerizedDispatchManager.this,
              ContainerizedDispatchManager.this.projectManager,
              ContainerizedDispatchManager.this.executorLoader,
              ContainerizedDispatchManager.this.alerterHolder, execFlow, "Failed to dispatch", e,
              Status.FAILED);
          logger.info("Finalizing the flow execution ", executionId);
          ExecutionControllerUtils.restartFlow(execFlow, originalStatus);
        } catch (ExecutorManagerException executorManagerException) {
          logger.error("Unable to update execution status to FAILED for : {}", executionId);
        } catch (RuntimeException re) {
          logger.error("Unexpected RuntimeException in ExecutionDispatcher", re);
        }
      }
    }
  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public void resumeFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public void pauseFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public void retryFailures(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
    synchronized (exFlow) {
      logger.info("Retrying failures for execId: {}", exFlow.getExecutionId());
      final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows = this.executorLoader
          .fetchUnfinishedFlows();
      if (unfinishedFlows.containsKey(exFlow.getExecutionId())) {
        final Pair<ExecutionReference, ExecutableFlow> pair = unfinishedFlows
            .get(exFlow.getExecutionId());
        // Note that ExecutionReference may have the 'executor' as null. ApiGateway call is expected
        // to handle this scenario.
        this.apiGateway.callWithReferenceByUser(pair.getFirst(),
            ConnectorParams.MODIFY_EXECUTION_ACTION, userId,
            new Pair<>(
                ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE,
                ConnectorParams.MODIFY_RETRY_FAILURES));
      } else {
        final ExecutorManagerException eme = new ExecutorManagerException("Execution "
            + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId() + " isn't running.");
        logger.warn("Exception while retrying flow failures. ", eme);
        throw eme;
      }
    }
  }

  //TODO: BDP-2567 Add container stats information
  @Override
  public Map<String, Object> callExecutorStats(int executorId, String action,
      Pair<String, String>... param) throws IOException, ExecutorManagerException {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  //TODO: BDP-2567 Add way to call jmx endpoint for flow container
  @Override
  public Map<String, Object> callExecutorJMX(String hostPort, String action, String mBean)
      throws IOException {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public Set<String> getAllActiveExecutorServerHosts() {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public State getExecutorManagerThreadState() {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public long getLastExecutorManagerThreadCheckTime() {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public Set<? extends String> getPrimaryServerHosts() {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public Collection<Executor> getAllActiveExecutors() {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public Executor fetchExecutor(int executorId) throws ExecutorManagerException {
    throw new UnsupportedOperationException("Unsupported Method");
  }

  @Override
  public void setupExecutors() throws ExecutorManagerException {
    throw new UnsupportedOperationException("Unsupported Method");
  }
}
