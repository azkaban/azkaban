/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.executor.container.watch;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import azkaban.metrics.ContainerMetrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;

/**
 * Provides callback implementation of {@link AzPodStatusListener} for
 * updating container metrics based on pod status
 */
@Singleton
public class ContainerStatusMetricsHandlerListener implements AzPodStatusListener{

  private static final Logger logger =
      Logger.getLogger(ContainerStatusMetricsHandlerListener.class);
  public static final int DEFAULT_EVENT_CACHE_MAX_ENTRIES = 50000;
  public static final int SHUTDOWN_TERMINATION_TIMEOUT_SECONDS = 5;

  private final ContainerMetrics containerMetrics;
  private final Cache<String, AzPodStatus> podStatusCache;

  private final ExecutorService executor;

  // Convenience member for referring to the Cache through ConcurrentMap interface.
  // Since cached data is key-value pair of pod name and pod status from event metadata,
  // each data size is expected to be no more than 100 bytes, maximum cache size will
  // be 100 bytes * DEFAULT_EVENT_CACHE_MAX_ENTRIES ~ 5 mb

  @Inject
  public ContainerStatusMetricsHandlerListener(final ContainerMetrics containerMetrics) {
    this.containerMetrics = containerMetrics;

    this.executor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("azk-container-metrics-pool-%d").build());

    this.podStatusCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_EVENT_CACHE_MAX_ENTRIES)
        .recordStats()
        .build();
  }

  /**
   * Checks whether the {@link AzPodStatus} of a new event is the same as the one already cached.
   *
   * @param event pod watch event
   * @return true if the status is different from cached value, false otherwise
   */
  protected boolean isUpdatedPodStatusDistinct(final AzPodStatusMetadata event) {
    AzPodStatus currentStatus = AzPodStatus.AZ_POD_UNSET;
    currentStatus = podStatusCache.getIfPresent(event.getPodName());

    boolean shouldSkip = currentStatus == event.getAzPodStatus();
    if (shouldSkip) {
      logger.info(format("Event pod status is same as current %s, for pod %s."
          +" Any updates will be skipped.", currentStatus, event.getPodName()));
    }
    return !shouldSkip;
  }

  // Update the cache with the given event.
  protected void updatePodStatus(final AzPodStatusMetadata event) {
    podStatusCache.put(event.getPodName(), event.getAzPodStatus());
    logger.debug(format("Updated status to %s, for pod %s", event.getAzPodStatus(), event.getPodName()));
  }

  /**
   * Validate and process event data to emit AzPodStatus metrics
   * @param event
   */
  private void validateAndProcess(final AzPodStatusMetadata event) {
    requireNonNull(event, "event must be non-null");
    if (!event.getFlowPodMetadata().isPresent() || !isUpdatedPodStatusDistinct(event)) {
      return;
    }
    // Update AzPodStatus metrics for the flow-pod respectively
    switch (event.getAzPodStatus()) {
      case AZ_POD_REQUESTED:
        containerMetrics.markPodRequested();
        break;
      case AZ_POD_SCHEDULED:
        containerMetrics.markPodScheduled();
        break;
      case AZ_POD_INIT_CONTAINERS_RUNNING:
        containerMetrics.markContainerRunning();
        break;
      case AZ_POD_APP_CONTAINERS_STARTING:
        containerMetrics.markAppContainerStarting();
        break;
      case AZ_POD_READY:
        containerMetrics.markPodReady();
        break;
      case AZ_POD_COMPLETED:
        containerMetrics.markPodCompleted();
        break;
      case AZ_POD_INIT_FAILURE:
        containerMetrics.markPodInitFailure();
        break;
      case AZ_POD_APP_FAILURE:
        containerMetrics.markPodAppFailure();
        break;
      default:
        logger.error(String.format("Current flow pod status %s is not for metric emitting",
            event.getAzPodStatus()));
    }
    updatePodStatus(event);
  }

  @Override
  public void onPodRequested(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  @Override
  public void onPodScheduled(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  @Override
  public void onPodInitContainersRunning(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  @Override
  public void onPodAppContainersStarting(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  @Override
  public void onPodReady(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  @Override
  public void onPodCompleted(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  @Override
  public void onPodInitFailure(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  @Override
  public void onPodAppFailure(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  @Override
  public void onPodUnexpected(final AzPodStatusMetadata event) {
    executor.execute(()->validateAndProcess(event));
  }

  public void shutdown() {
    this.executor.shutdown();
    try {
      this.executor.awaitTermination(SHUTDOWN_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Executor service shutdown for container status metrics handler listener was "
              + "interrupted.",
          e);
    }
  }
}
