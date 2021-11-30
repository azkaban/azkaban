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

import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.utils.Props;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
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
  private static final Duration DEFAULT_STALE_CONTAINER_AGE_MINS =
      Duration.ofMinutes(10 * 24 * 60); // 10 days
  private final long cleanupIntervalMin;
  private final long staleContainerAgeMins;
  private final ScheduledExecutorService cleanupService;
  private final ExecutorLoader executorLoader;
  private final ContainerizedImpl containerizedImpl;

  @Inject
  public ContainerCleanupManager(final Props azkProps, final ExecutorLoader executorLoader,
      final ContainerizedImpl containerizedImpl) {
    this.cleanupIntervalMin = azkProps
        .getLong(
            ContainerizedDispatchManagerProperties.CONTAINERIZED_STALE_EXECUTION_CLEANUP_INTERVAL_MIN,
            DEFAULT_STALE_CONTAINER_CLEANUP_INTERVAL.toMinutes());
    this.staleContainerAgeMins = azkProps.getLong(ConfigurationKeys.AZKABAN_MAX_FLOW_RUNNING_MINS,
        DEFAULT_STALE_CONTAINER_AGE_MINS.toMinutes());
    this.cleanupService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("azk-container-cleanup").build());
    this.executorLoader = executorLoader;
    this.containerizedImpl = containerizedImpl;
  }

  /**
   * Execute container-provider specific APIs for all 'stale' containers. A container is considered
   * 'stale' if it was launched {@code staleContainerAgeMins} ago and the corresponding execution is
   * not yet in a final state.
   * <p>
   * It's important that this method does not throw exceptions as that will interrupt the scheduling
   * of {@code cleanupService}.
   */
  public void terminateStaleContainers() {
    try {
      final List<ExecutableFlow> staleFlows = this.executorLoader
          .fetchStaleFlows(Duration.ofMinutes(this.staleContainerAgeMins));
      for (final ExecutableFlow flow : staleFlows) {
        deleteContainerQuietly(flow.getExecutionId());
      }
    } catch (final Exception e) {
      logger.error("Unexpected exception during container cleanup." + e);
    }
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
    this.cleanupService.scheduleAtFixedRate(this::terminateStaleContainers, 0L,
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
