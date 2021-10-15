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

import static azkaban.Constants.ContainerizedDispatchManagerProperties;

import azkaban.Constants.FlowParameters;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.utils.Props;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provides the ability to cleanup execution containers which exceed the maximum allowed
 * duration for a flow. Following extensions will be considered in future. (1) Convert this to a
 * general background process for any other containerization related periodic tasks. (2) Add ability
 * to track containers which may not have been deleted despite prior attempts. (3) It's assumed
 * final status updates to the db are handled by other parts, such as flow-container. That can also
 * be added here as a failsafe if needed.
 */
@Singleton
public class ContainerCleanupManager {

  private static final Logger logger = LoggerFactory.getLogger(ContainerCleanupManager.class);
  private static final Duration DEFAULT_STALE_CONTAINER_CLEANUP_INTERVAL = Duration.ofMinutes(10);
  private final long cleanupIntervalMin;
  private final ScheduledExecutorService cleanupService;
  private final ExecutorLoader executorLoader;
  private final ContainerizedImpl containerizedImpl;
  private final ContainerizedDispatchManager containerizedDispatchManager;

  @Inject
  public ContainerCleanupManager(final Props azkProps, final ExecutorLoader executorLoader,
      final ContainerizedImpl containerizedImpl,
      final ContainerizedDispatchManager containerizedDispatchManager) {
    this.cleanupIntervalMin = azkProps
        .getLong(
            ContainerizedDispatchManagerProperties.CONTAINERIZED_STALE_EXECUTION_CLEANUP_INTERVAL_MIN,
            DEFAULT_STALE_CONTAINER_CLEANUP_INTERVAL.toMinutes());
    this.cleanupService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("azk-container-cleanup").build());
    this.executorLoader = executorLoader;
    this.containerizedImpl = containerizedImpl;
    this.containerizedDispatchManager = containerizedDispatchManager;
  }

  public void cleanUpStaleFlows() {
    cleanUpStaleFlows(Status.DISPATCHING);
    cleanUpStaleFlows(Status.PREPARING);
    cleanUpStaleFlows(Status.RUNNING);
    cleanUpStaleFlows(Status.PAUSED);
    cleanUpStaleFlows(Status.KILLING);
    cleanUpStaleFlows(Status.EXECUTION_STOPPED);
    cleanUpStaleFlows(Status.FAILED_FINISHING);
  }

  /**
   * Try cleaning the stale flows for a given status. This will try to cancel the flow, if
   * unreachable, flow will be finalized. Pod Container will be deleted.
   *
   * @param status
   */
  public void cleanUpStaleFlows(final Status status) {
    List<ExecutableFlow> staleFlows;
    try {
      staleFlows = this.executorLoader.fetchStaleFlowsForStatus(status);
    } catch (final Exception e) {
      logger.error("Exception occurred while fetching stale flows during clean up." + e);
      return;
    }
    for (final ExecutableFlow flow : staleFlows) {
      if (shouldIgnore(flow, status)) {
        continue;
      }
      logger.info("Cleaning up stale flow " + flow.getExecutionId() + " in state " + status.name());
      String cancelMessage = cancelFlowQuietly(flow);
      deleteContainerQuietly(flow.getExecutionId());
      uploadMessageQuietly(flow.getExecutionId(), cancelMessage);
    }
  }

  private void uploadMessageQuietly(final int execId, final String message) {
    if (StringUtils.isBlank(message)) {
      return;
    }
    try {
      File tempFile = File.createTempFile("cleanup-" + execId + "-", ".tmp");
      uploadMessageQuietly(execId, message, tempFile);
      tempFile.delete();
    } catch (IOException ie) {
      logger.error("IOException while uploading cleanup logs.", ie);
    } catch (RuntimeException re) {
      logger.error("Unexpected RuntimeException while uploading cleanup logs.", re);
    }
  }

  private void uploadMessageQuietly(final int execId, final String message, final File tempFile) {
    try {
      Files.write(tempFile.toPath(), message.getBytes(StandardCharsets.UTF_8));
      this.executorLoader.uploadLogFile(execId, "", 0, tempFile);
    } catch (ExecutorManagerException | IOException eme) {
      logger.error("Exception while uploading cleanup logs.", eme);
    } catch (RuntimeException re) {
      logger.error("Unexpected RuntimeException while uploading cleanup logs.", re);
    }
  }

  /**
   * Handles special cases. If the flow is in PREPARING STATE and enable.dev.pod=true then ignore
   * executions from the last 2 days.
   *
   * @param flow
   * @param status
   * @return
   */
  private boolean shouldIgnore(final ExecutableFlow flow, final Status status) {
    if (status != Status.PREPARING) {
      return false;
    }
    final ExecutionOptions executionOptions = flow.getExecutionOptions();
    if (null == executionOptions) {
      return false;
    }
    boolean isDevPod = Boolean.parseBoolean(
        executionOptions.getFlowParameters().get(FlowParameters.FLOW_PARAM_ENABLE_DEV_POD));
    if (!isDevPod) {
      return false;
    }
    return flow.getSubmitTime() > System.currentTimeMillis() - Duration.ofDays(2).toMillis();
  }

  /**
   * Try to quietly cancel the flow. Cancel flow tries to gracefully kill the executions if they are
   * reachable, otherwise, flow will be finalized.
   *
   * @param flow
   * @return the errorMessage
   */
  private String cancelFlowQuietly(ExecutableFlow flow) {
    final StringBuilder builder = new StringBuilder();
    builder.append("Cleaning up stale flow with status: " + flow.getStatus() + "\n");
    try {
      this.containerizedDispatchManager.cancelFlow(flow, flow.getSubmitUser());
      return StringUtils.EMPTY;
    } catch (ExecutorManagerException eme) {
      String msg = "ExecutorManagerException while cancelling flow.";
      logger.error(msg, eme);
      builder.append(msg + "\n");
      builder.append(ExceptionUtils.getStackTrace(eme) + "\n");
    } catch (RuntimeException re) {
      String msg = "Unexpected RuntimeException while finalizing flow during clean up.";
      logger.error(msg, re);
      builder.append(msg + "\n");
      builder.append(ExceptionUtils.getStackTrace(re) + "\n");
    }
    return builder.toString();
  }

  // Deletes the container specified by executionId while logging and consuming any exceptions.
  // Note that while this method is not async it's still expected to return 'quickly'. This is true
  // for Kubernetes as it's declarative API will only submit the request for deleting container
  // resources. In future we can consider making this async to eliminate any chance of the cleanup
  // thread getting blocked.
  private void deleteContainerQuietly(final int executionId) {
    try {
      this.containerizedImpl.deleteContainer(executionId);
    } catch (final ExecutorManagerException eme) {
      logger.warn("ExecutorManagerException while deleting container.", eme);
    } catch (final RuntimeException re) {
      logger.error("Unexpected RuntimeException while deleting container.", re);
    }
  }

  /**
   * Start periodic deletions of per-container resources for stale executions.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    logger.info("Start container cleanup service");
    this.cleanupService.scheduleAtFixedRate(this::cleanUpStaleFlows, 0L,
        this.cleanupIntervalMin, TimeUnit.MINUTES);
  }

  /**
   * Stop the periodic container cleanups.
   */
  public void shutdown() {
    logger.info("Shutdown container cleanup service");
    this.cleanupService.shutdown();
    try {
      if (!this.cleanupService.awaitTermination(30, TimeUnit.SECONDS)) {
        this.cleanupService.shutdownNow();
      }
    } catch (final InterruptedException ex) {
      this.cleanupService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
