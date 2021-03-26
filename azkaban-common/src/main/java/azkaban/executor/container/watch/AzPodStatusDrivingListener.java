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

import static java.util.Objects.requireNonNull;

import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives execution {@link AzPodStatusListener} interface callbacks.
 * Multiple listeners can be registered with a single instance of this class.
 * Events received by this directly map the states in {@link AzPodStatus} and for every event all
 * the callbacks registered for the particular event type are 'invoked'
 *
 * Invocation of the callbacks is done via pool of threads different from the one invoking this
 * {@code onEvent} interface method of this class. This is done as a typical implementation of the
 * {@link AzPodStatusListener} interface can include several blocking calls, for example for
 * database status updates.
 */
@Singleton
public class AzPodStatusDrivingListener implements RawPodWatchEventListener {
  private static final Logger logger = LoggerFactory.getLogger(AzPodStatusDrivingListener.class);

  private static final int DEFAULT_THREAD_POOL_SIZE = 4;
  public static final int SHUTDOWN_TERMINATION_TIMEOUT_SECONDS = 5;
  private final int threadPoolSize;
  private final ExecutorService executor;
  private final ImmutableMap<AzPodStatus, List<Consumer<AzPodStatusMetadata>>> listenerMap;

  /**
   * Create a new instance of {@link AzPodStatusDrivingListener}.
   */
  @Inject
  public AzPodStatusDrivingListener(Props azkProps) {
    requireNonNull(azkProps, "azkaban properties must not be null");
    this.threadPoolSize = azkProps.getInt(
        ContainerizedDispatchManagerProperties.KUBERNETES_WATCH_DRIVER_THREAD_POOL_SIZE,
        DEFAULT_THREAD_POOL_SIZE);

    this.executor = Executors.newFixedThreadPool(this.threadPoolSize,
        new ThreadFactoryBuilder().setNameFormat("azk-watch-pool-%d").build());

    ImmutableMap.Builder listenerMapBuilder = ImmutableMap.builder()
        .put(AzPodStatus.AZ_POD_REQUESTED, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_SCHEDULED, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_READY, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_COMPLETED, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_INIT_FAILURE, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_APP_FAILURE, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_UNEXPECTED, new ArrayList<>());
    listenerMap = listenerMapBuilder.build();
  }

  /**
   * Register a new {@link AzPodStatusListener}.
   * Note that the callbacks are submitted to the {@link ExecutorService} in the order interfaces
   * were registered, but that does not make any guarantees about the sequence in which all the
   * callbacks for a given event are invoked.
   *
   * @param listener
   */
  public void registerAzPodStatusListener(AzPodStatusListener listener) {
    requireNonNull(listener, "listener must not be null");
    listenerMap.get(AzPodStatus.AZ_POD_REQUESTED).add(listener::onPodRequested);
    listenerMap.get(AzPodStatus.AZ_POD_SCHEDULED).add(listener::onPodScheduled);
    listenerMap.get(AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING).add(listener::onPodInitContainersRunning);
    listenerMap.get(AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING).add(listener::onPodAppContainersStarting);
    listenerMap.get(AzPodStatus.AZ_POD_READY).add(listener::onPodReady);
    listenerMap.get(AzPodStatus.AZ_POD_COMPLETED).add(listener::onPodCompleted);
    listenerMap.get(AzPodStatus.AZ_POD_INIT_FAILURE).add(listener::onPodInitFailure);
    listenerMap.get(AzPodStatus.AZ_POD_APP_FAILURE).add(listener::onPodAppFailure);
    listenerMap.get(AzPodStatus.AZ_POD_UNEXPECTED).add(listener::onPodUnexpected);

  }

  /**
   * Shutdown the driver, including the {@link ExecutorService} with a timeout.
   */
  public void shutdown() {
    executor.shutdown();
    try {
      executor.awaitTermination(SHUTDOWN_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Executor service shutdown was interrupted.", e);
    }
  }

  /**
   * Process all the callbacks for a given event. Lambdas corresponding to the callback method from
   * every registered {@link AzPodStatusListener} are scheduled for execution on the {@code
   * executor}
   *
   * @param podStatusMetadata
   */
  private void deliverCallbacksForEvent(AzPodStatusMetadata podStatusMetadata) {
    listenerMap.get(podStatusMetadata.getAzPodStatus()).stream()
        .forEach(callback -> executor.execute(() -> callback.accept(podStatusMetadata)));
  }

  @Override
  public void onEvent(Watch.Response<V1Pod> watchEvent) {
    // Technically, logging the pod watch event can also be performed as part of a callback.
    // For now the logging is inline to ensure the the event is logged before any corresponding
    // callbacks and are invoked.
    try {
      AzPodStatusMetadata azPodStatusMetadata = AzPodStatusExtractor.getAzPodStatusFromEvent(watchEvent);
      logPodWatchEvent(azPodStatusMetadata);
      deliverCallbacksForEvent(azPodStatusMetadata);
    } catch (Exception e) {
      logger.error("Unexepcted exception while processing pod watch event.", e);
    }
  }

  private static void logPodWatchEvent(AzPodStatusMetadata event) {
    // There could be value in logging the entire 'raw' event for post-mortem of any issues.
    // We should consider logging the event in a separate log file as a json object.
    logger.info(String.format("Event for pod %s : %s", event.getPodName(),
        event.getAzPodStatus()));
  }
}
