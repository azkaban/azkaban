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

import azkaban.Constants;
import azkaban.DispatchMethod;
import azkaban.executor.AbstractExecutorManagerAdapter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorApiGateway;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.IOException;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ContainerizedExecutionManager extends AbstractExecutorManagerAdapter {

  private ContainerizedImpl containerizedImpl;
  private QueueProcessorThread queueProcessor;
  private static final Logger logger = LoggerFactory.getLogger(ContainerizedExecutionManager.class);

  @Inject
  public ContainerizedExecutionManager(final Props azkProps, final ExecutorLoader executorLoader,
      final CommonMetrics commonMetrics, final ExecutorApiGateway apiGateway) throws ExecutorManagerException{
    super(azkProps, executorLoader, commonMetrics, apiGateway);
    String containerizedImplProperty =
        azkProps.getString(Constants.ContainerizationProperties.CONTAINERIZATION_IMPL_TYPE,
            ContainerizedImplType.KUBERNETES.name())
            .toUpperCase();
    try {
      this.containerizedImpl = ContainerizedImplFactory.getContainerizedImpl(azkProps,
          ContainerizedImplType.valueOf(containerizedImplProperty));
    } catch (ExecutorManagerException e) {
      logger.error("Could not create containerized implementation class for {}",
          containerizedImplProperty);
      throw new ExecutorManagerException("Unable to create containerized implementation.");
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
   * Get execution ids of all running (unfinished) flows from database.
   */
  public List<Integer> getRunningFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    try {
      getExecutionIdsHelper(allIds, this.executorLoader.fetchUnfinishedFlows().values());
    } catch (final ExecutorManagerException e) {
      this.logger.error("Failed to get running flow ids.", e);
    }
    return allIds;
  }

  public List<Integer> getQueuedFlowIds() {
    final List<Integer> allIds = new ArrayList<>();
    try {
      getExecutionIdsHelper(allIds, this.executorLoader.fetchQueuedFlows(Status.READY));
    } catch (final ExecutorManagerException e) {
      this.logger.error("Failed to get queued flow ids.", e);
    }
    return allIds;
  }

  @Override
  public long getQueuedFlowSize() {
    return getQueuedFlowIds().size();
  }

  @Override
  public DispatchMethod getDispatchMethod() {
    return DispatchMethod.CONTAINERIZED;
  }

  @Override
  public void start() {
    this.queueProcessor = setupQueueProcessor();
    this.queueProcessor.start();
  }

  private QueueProcessorThread setupQueueProcessor() {
    return new QueueProcessorThread(
        this.azkProps, this.executorLoader);
  }

  @Override
  public Status getStartStatus() {
    return Status.READY;
  }

  @Override
  public void shutdown() {
    if (null != this.queueProcessor) {
      this.queueProcessor.shutdown();
    }
  }

  @Override
  public void enableQueueProcessorThread() {
    this.queueProcessor.setActive(true);
  }

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
   * This QueueProcessorThread will fetch executions from database in batch and dispatch it in
   * container.
   */
  private class QueueProcessorThread extends Thread {

    private final long queueProcessorWaitInMS;
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
      this.queueProcessorWaitInMS =
          this.azkProps.getLong(Constants.ConfigurationKeys.QUEUE_PROCESSOR_WAIT_IN_MS, 1000);
      this.executionsBatchProcessingEnabled = azkProps
          .getBoolean(Constants.ContainerizationProperties.CONTAINERIZATION_EXECUTION_BATCH_ENABLED,
              false);
      this.executionsBatchSize =
          azkProps
              .getInt(Constants.ContainerizationProperties.CONTAINERIZATION_EXECUTION_BATCH_SIZE,
                  10);

      this.executorService = Executors.newFixedThreadPool(azkProps.getInt(
          Constants.ContainerizationProperties.CONTAINERIZATION_EXECUTION_PROCESSING_THREAD_SIZE,
          10));
      this.setName("Containerized-QueueProcessor-Thread");
    }

    @Override
    public void run() {
      // Loops till QueueProcessorThread is shutdown
      while (!this.shutdown) {
        synchronized (this) {
          try {
            // start processing queue if active, otherwise wait for sometime
            if (this.isActive) {
              processQueuedFlows();
            }
            wait(queueProcessorWaitInMS);
          } catch (final Exception e) {
            ContainerizedExecutionManager.logger.error(
                "QueueProcessorThread Interrupted. Probably to shut down.", e);
          }
        }
      }
    }

    /* Method responsible for processing the non-dispatched flows */
    private void processQueuedFlows() throws ExecutorManagerException {
      Set<Integer> executionIds =
          executorLoader.selectAndUpdateExecutionWithLocking(this.executionsBatchProcessingEnabled, this.executionsBatchSize,
              Status.DISPATCHING);

      for (int executionId : executionIds) {
        Runnable worker = new ExecutionDispatcher(executionId);
        executorService.execute(worker);
      }
    }

    public boolean isActive() {
      return this.isActive;
    }

    public void setActive(final boolean isActive) {
      this.isActive = isActive;
      ContainerizedExecutionManager.logger
          .info("QueueProcessorThread active turned " + this.isActive);
    }

    public void shutdown() {
      this.shutdown = true;
      this.executorService.shutdown();
      this.interrupt();
    }
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
        ContainerizedExecutionManager.this.containerizedImpl.createContainer(executionId);
      } catch (ExecutorManagerException e) {
        logger.info("Unable to dispatch container in Kubernetes for : {}", executionId);
        final ExecutableFlow dsFlow;
        try {
          dsFlow =
              ContainerizedExecutionManager.this.executorLoader.fetchExecutableFlow(executionId);
          dsFlow.setStatus(Status.READY);
          dsFlow.setUpdateTime(System.currentTimeMillis());
          ContainerizedExecutionManager.this.executorLoader.updateExecutableFlow(dsFlow);
        } catch (ExecutorManagerException executorManagerException) {
          logger.error("Unable to update execution status to READY for : {}", executionId);
        }
      }
    }
  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public LogData getExecutableFlowLog(ExecutableFlow exFlow, int offset, int length)
      throws ExecutorManagerException {
    return null;
  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public LogData getExecutionJobLog(ExecutableFlow exFlow, String jobId, int offset, int length,
      int attempt) throws ExecutorManagerException {
    return null;
  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public List<Object> getExecutionJobStats(ExecutableFlow exflow, String jobId, int attempt)
      throws ExecutorManagerException {
    return null;
  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public void cancelFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {

  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public void resumeFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {

  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public void pauseFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {

  }

  //TODO: BDP-3642 Add a way to call Flow container APIs using apiGateway
  @Override
  public void retryFailures(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {

  }

  //TODO: BDP-2567 Add container stats information
  @Override
  public Map<String, Object> callExecutorStats(int executorId, String action,
      Pair<String, String>... param) throws IOException, ExecutorManagerException {
    throw new UnsupportedOperationException("Invalid Method");
  }

  //TODO: BDP-2567 Add way to call jmx endpoint for flow container
  @Override
  public Map<String, Object> callExecutorJMX(String hostPort, String action, String mBean)
      throws IOException {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public Map<String, String> doRampActions(List<Map<String, Object>> rampAction)
      throws ExecutorManagerException {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public Set<String> getAllActiveExecutorServerHosts() {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public State getExecutorManagerThreadState() {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public long getLastExecutorManagerThreadCheckTime() {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public Set<? extends String> getPrimaryServerHosts() {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public Collection<Executor> getAllActiveExecutors() {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public Executor fetchExecutor(int executorId) throws ExecutorManagerException {
    throw new UnsupportedOperationException("Invalid Method");
  }

  @Override
  public void setupExecutors() throws ExecutorManagerException {
    throw new UnsupportedOperationException("Invalid Method");
  }
}
