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
import azkaban.Constants.ContainerizedExecutionManagerProperties;
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
import com.google.common.util.concurrent.RateLimiter;
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

/**
 * This class is used to dispatch execution on Containerized infrastructure. Each execution will
 * be dispatched in single container. This way, it will bring isolation between all the
 * executions.
 */
@Singleton
public class ContainerizedDispatchImpl extends AbstractExecutorManagerAdapter {

  private final ContainerizedImpl containerizedImpl;
  private QueueProcessorThread queueProcessor;
  private final RateLimiter rateLimiter;
  private static final Logger logger = LoggerFactory.getLogger(ContainerizedDispatchImpl.class);

  @Inject
  public ContainerizedDispatchImpl(final Props azkProps, final ExecutorLoader executorLoader,
      final CommonMetrics commonMetrics, final ExecutorApiGateway apiGateway,
      final ContainerizedImpl containerizedImpl) throws ExecutorManagerException{
    super(azkProps, executorLoader, commonMetrics, apiGateway);
    rateLimiter =
        RateLimiter.create(azkProps.getInt(ContainerizedExecutionManagerProperties.CONTAINERIZED_CREATION_RATE_LIMIT, 20));
    this.containerizedImpl = containerizedImpl;
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
   * Get queued flow ids from database. The status for queued flows is READY for
   * containerization.
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
   * @return
   */
  @Override
  public long getQueuedFlowSize() {
    return getQueuedFlowIds().size();
  }

  /**
   * Get dispatch method enum for this class.
   * @return
   */
  @Override
  public DispatchMethod getDispatchMethod() {
    return DispatchMethod.CONTAINERIZED;
  }

  /**
   * This method is used to start queue processor thread. The queue processor thread will pick
   * execution from queue maintained in database and create a container for it.
   */
  @Override
  public void start() {
    this.queueProcessor = setupQueueProcessor();
    this.queueProcessor.start();
  }

  private QueueProcessorThread setupQueueProcessor() {
    return new QueueProcessorThread(
        this.azkProps, this.executorLoader);
  }

  /**
   * Get the status for executions maintained in queue. For other dispatch implementations, the
   * status for executions in queue is PREPARING. But for containerized dispatch method, the status
   * will be READY.
   * @return
   */
  @Override
  public Status getStartStatus() {
    return Status.READY;
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
  }

  /**
   * This method is used to enable queue processor thread. It will resume dispatching executions.
   * Due to any maintenance of containerized infrastructure, if queue processor was disabled then
   * it can enabled again using this method.
   */
  @Override
  public void enableQueueProcessorThread() {
    this.queueProcessor.setActive(true);
  }

  /**
   * This method is used to disable queue processor thread. It will stop dispatching executions.
   * In case of maintenance of containerized infrastructure, this method can be used to disable
   * queue processor. It will disable dispatch of executions in containers.
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
          .getBoolean(ContainerizedExecutionManagerProperties.CONTAINERIZED_EXECUTION_BATCH_ENABLED,
              false);
      this.executionsBatchSize =
          azkProps
              .getInt(ContainerizedExecutionManagerProperties.CONTAINERIZED_EXECUTION_BATCH_SIZE,
                  10);
      this.executorService = Executors.newFixedThreadPool(azkProps.getInt(
          ContainerizedExecutionManagerProperties.CONTAINERIZED_EXECUTION_PROCESSING_THREAD_POOL_SIZE,
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
          ContainerizedDispatchImpl.logger.error(
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
              Status.DISPATCHING);

      for (final int executionId : executionIds) {
        rateLimiter.acquire();
        logger.info("Starting dispatch for {} execution.", executionId);
        Runnable worker = new ExecutionDispatcher(executionId);
        executorService.execute(worker);
      }
    }

    public boolean isActive() {
      return this.isActive;
    }

    public void setActive(final boolean isActive) {
      this.isActive = isActive;
      ContainerizedDispatchImpl.logger
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
        ContainerizedDispatchImpl.this.containerizedImpl.createContainer(executionId);
        logger.info("Time taken to dispatch a container for {} is {}", executionId,
            (System.currentTimeMillis() - startTime) / 1000);
      } catch (ExecutorManagerException e) {
        logger.info("Unable to dispatch container in Kubernetes for : {}", executionId);
        logger.info("Reason for dispatch failure: {}", e.getMessage());
        // Reset the status of an execution to READY if dispatched failed. It will be picked up
        // again from queue.
        final ExecutableFlow dsFlow;
        try {
          dsFlow =
              ContainerizedDispatchImpl.this.executorLoader.fetchExecutableFlow(executionId);
          dsFlow.setStatus(Status.READY);
          dsFlow.setUpdateTime(System.currentTimeMillis());
          ContainerizedDispatchImpl.this.executorLoader.updateExecutableFlow(dsFlow);
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
