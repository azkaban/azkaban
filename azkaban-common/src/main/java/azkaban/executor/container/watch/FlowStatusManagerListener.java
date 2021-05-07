package azkaban.executor.container.watch;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.executor.container.watch.AzPodStatus.TransitionValidator;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides callback implementations of {@link AzPodStatusListener} for
 * (1) Updating WebServer and database state of flows based on Pod status
 * (2) Driving pod lifecycle actions, such as deleting flow pods in a final state.
 */
@Singleton
public class FlowStatusManagerListener implements AzPodStatusListener {

  private static final Logger logger = LoggerFactory.getLogger(FlowStatusManagerListener.class);
  public static final int EVENT_CACHE_STATS_FREQUENCY = 100;
  public static final int DEFAULT_EVENT_CACHE_MAX_ENTRIES = 4096;
  public static final int SHUTDOWN_TERMINATION_TIMEOUT_SECONDS = 5;

  private final ContainerizedImpl containerizedImpl;
  private final ExecutorLoader executorLoader;
  private final AlerterHolder alerterHolder;
  private final Cache<String, AzPodStatusMetadata> podStatusCache;
  private final ExecutorService executor;

  // Note about the cache size.
  // Each incoming event is expected to be less than 5KB in size and the maximum cache size will be
  // about 5KB * maxCacheEntries.
  private final int maxCacheEntries;

  private final AtomicLong flowContainerEventCount = new AtomicLong(0);

  @Inject
  public FlowStatusManagerListener(Props azkProps,
      ContainerizedImpl containerizedImpl,
      ExecutorLoader executorLoader,
      AlerterHolder alerterHolder) {
    requireNonNull(azkProps, "azkaban properties must not be null");
    requireNonNull(containerizedImpl, "container implementation must not be null");
    requireNonNull(executorLoader, "executor loader must not be null");
    requireNonNull(alerterHolder, "alerter holder must not be null");
    this.containerizedImpl = containerizedImpl;
    this.executorLoader = executorLoader;
    this.alerterHolder = alerterHolder;

    this.executor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("azk-watch-pool-%d").build());

    maxCacheEntries =
        azkProps.getInt(ContainerizedDispatchManagerProperties.KUBERNETES_WATCH_EVENT_CACHE_MAX_ENTRIES,
        DEFAULT_EVENT_CACHE_MAX_ENTRIES);
    this.podStatusCache = CacheBuilder.newBuilder()
        .maximumSize(maxCacheEntries)
        .recordStats()
        .build();
  }

  @VisibleForTesting
  public ContainerizedImpl getContainerizedImpl() {
    return this.containerizedImpl;
  }

  @VisibleForTesting
  public ExecutorLoader getExecutorLoader() {
    return this.executorLoader;
  }

  /**
   * Validate the the transition from the current state to the new state in the {@code event}
   * is supported. Note that the validation is on best-effort basis as the cache may not always
   * contain an event for all running flows. This could happen, for example, during webserver
   * restarts.
   * @param event pod watch event
   */
  private void validateTransition(AzPodStatusMetadata event) {
    AzPodStatus currentStatus = getCurrentAzPodStatus(event.getPodName());
    logger.debug(format("Transition requested from %s -> %s, for pod %s",
        currentStatus,
        event.getAzPodStatus(),
        event.getPodName()));

    if (!TransitionValidator.isTransitionValid(currentStatus, event.getAzPodStatus())) {
      IllegalStateException ise = new IllegalStateException(
          format("Pod status transition is not supported %s -> %s, for pod %s",
              currentStatus,
              event.getAzPodStatus(),
              event.getPodName()));
      logger.error("Unsupported state transition.", ise);
      throw ise;
    }
  }

  /**
   * Includes any validations needed on event that are common to all {@link AzPodStatus}
   *
   * @param event pod watch event
   */
  private void validateAndPreProcess(AzPodStatusMetadata event) {
    // Confirm that event is for a FlowContainer and has the corresponding Flow metadata.
    if (!event.getFlowPodMetadata().isPresent()) {
      IllegalStateException ise = new IllegalStateException(
          format("Flow metadata is not present for pod %s", event.getPodName()));
      logger.error("Pod is likely not a Flow Container.", ise);
      throw ise;
    }
    long eventCount = flowContainerEventCount.incrementAndGet();
    logEventCacheStats(eventCount);
    validateTransition(event);
  }

  /**
   * Common event processing required for all {@link AzPodStatus} callbacks.
   * This is also responsible for updating the event cache/map.
   *
   * @param event pod watch event
   */
  private void postProcess(AzPodStatusMetadata event) {
    updatePodStatus(event);
  }

  // This currently logs the pod event cache stats for every {@link EVENT_CACHE_STATS_FREQUENCY}.
  // An alternative to consider is to log this periodically based on a fixed time interval
  // duration. One way of doing this is to create a new periodically run task (and thread) another
  // is to look at the timestamps within the events and log the stats only if a certain fixed
  // duration has elapsed. For now this is event-count based and we will introduce more complex
  // solutions if there is a need for it in future. (If needed, these stats can be exposed as
  // metrics as well, for example)
  private void logEventCacheStats(long eventCount) {
    if (eventCount % EVENT_CACHE_STATS_FREQUENCY != 0) {
      return;
    }
    CacheStats stats = podStatusCache.stats();
    logger.info("Pod Event Cache Stats at flow event count {}:  {}", eventCount, stats.toString());
  }

  /**
   * Checks whether the {@link AzPodStatus} of a new event is the same as the one already cached.
   *
   * @param event pod watch event
   * @return true if the status is different from cached value, false otherwise
   */
  private boolean isUpdatedPodStatusDistinct(AzPodStatusMetadata event) {
    AzPodStatus currentStatus = getCurrentAzPodStatus(event.getPodName());
    boolean skipUpdates = (currentStatus == event.getAzPodStatus());
    if (skipUpdates) {
      logger.info(format("Event pod status is same as current %s, for pod %s."
          +" Any updates will be skipped.", currentStatus, event.getPodName()));
    }
    return !skipUpdates;
  }

  private AzPodStatus getCurrentAzPodStatus(String podName) {
    AzPodStatus currentStatus = AzPodStatus.AZ_POD_UNSET;
    AzPodStatusMetadata currentEvent = podStatusCache.getIfPresent(podName);
    if (currentEvent != null ) {
      currentStatus = currentEvent.getAzPodStatus();
    }
    return currentStatus;
  }

  // Update the cache with the given event.
  private void updatePodStatus(AzPodStatusMetadata event) {
    podStatusCache.put(event.getPodName(), event);
    logger.debug(format("Updated status to %s, for pod %s", event.getAzPodStatus(), event.getPodName()));
  }

  /**
   * If the Flow corresponding to Pod event is not already in a final state then finalize the flow
   * with the state {@link Status::Failed}
   *
   * <p> Note:
   * Unfortunately many (if not all) of the Flow status updates in Azkaban don't fully enforce
   * the Flow lifecycle state-machine and {@code ExecutionControllerUtils.finalizeFlow} used
   * within this method is no exception. It will simply finalize the flow (as failed) even if the
   * flow status is in a finalized state in the Db. <br>
   * This can indirectly impact this listener implementation in future, if more than one thread
   * tries to update the state of the same flow in db. Although it's not any worse than how
   * the rest of Azkaban already behaves, we should fix the behavior at least during the
   * finalization of flows. One way of achieving this is to add a utility method to atomically
   * test-and-finalize a flow from a non-final to a failed state.
   *
   * @implNote Flow status check and update is not atomic, details above.
   * @param event pod event
   * @return
   */
  private Optional<Status> compareAndFinalizeFlowStatus(AzPodStatusMetadata event) {
    requireNonNull(event, "event must not be null");

    int executionId = Integer.parseInt(event.getFlowPodMetadata().get().getExecutionId());
    ExecutableFlow executableFlow = null;
    try {
      executableFlow = executorLoader.fetchExecutableFlow(executionId);
    } catch (ExecutorManagerException e) {
      String message = format("Exception while fetching executable flow for pod %s",
          event.getPodName());
      logger.error(message, e);
      throw new AzkabanWatchException(message, e);
    }
    if (executableFlow == null) {
      logger.error("Unable to find executable flow for execution: " + executionId);
      return Optional.empty();
    }
    Status originalStatus = executableFlow.getStatus();

    if (!Status.isStatusFinished(originalStatus)) {
      logger.info(format(
          "Flow execution-id %d for pod %s does not have a final status in database and will be "
              + "finalized.", executionId, event.getPodName()));
      final String reason = "Flow Pod execution was completed.";
      ExecutionControllerUtils.finalizeFlow(executorLoader, alerterHolder, executableFlow, reason,
          null);
    }
    return Optional.of(originalStatus);
  }

  /**
   * Delete the the flow pod and any other related objects (such as services).
   *
   * @param event pod watch event
   */
  private void deleteFlowContainer(AzPodStatusMetadata event) {
    logger.info("Deleting Flow Pod: " + event.getPodName());
    try {
      containerizedImpl.deleteContainer(
          Integer.parseInt(
              event.getFlowPodMetadata().get().getExecutionId()));
    } catch (ExecutorManagerException e) {
      String message = format("Exception while deleting flow container.");
      logger.error(message, e);
      throw new AzkabanWatchException(message, e);
    } catch (NumberFormatException ne) {
      String message = format("Flow metadata execution id is not a valid integer %s",
          event.getFlowPodMetadata().get().getExecutionId());
      throw new AzkabanWatchException(message, ne);
    }
  }

  /**
   * Common processing for all the final states of the flow-pod. This is responsible
   * for deleting the flow container as well as finalizing the Flow status in the Db in case
   * it's not already in a final state.
   *
   * @param event pod event
   */
  private void processFinalState(AzPodStatusMetadata event) {
    requireNonNull(event, "event must not be null");
    validateAndPreProcess(event);
    boolean skipUpdates = !isUpdatedPodStatusDistinct(event);
    postProcess(event);
    if (!skipUpdates) {
      Optional<Status> originalFlowStatus = compareAndFinalizeFlowStatus(event);
      if (originalFlowStatus.isPresent() &&
          !Status.isStatusFinished(originalFlowStatus.get())) {
        logger.warn(format("Flow for pod %s was in the non-final state %s and was finalized",
            event.getPodName(), originalFlowStatus));
      }
      deleteFlowContainer(event);
    }
  }

  /**
   * Common processing for the non-final states for the flow-pod.
   *
   * @param event pod event
   */
  private void processNonFinalState(AzPodStatusMetadata event) {
    requireNonNull(event, "event must not be null");
    validateAndPreProcess(event);
    postProcess(event);
  }

  @Override
  public void onPodRequested(AzPodStatusMetadata event) {
    executor.execute(() -> processNonFinalState(event));
  }

  @Override
  public void onPodScheduled(AzPodStatusMetadata event) {
    executor.execute(() -> processNonFinalState(event));
  }

  @Override
  public void onPodInitContainersRunning(AzPodStatusMetadata event) {
    executor.execute(() -> processNonFinalState(event));
  }

  @Override
  public void onPodAppContainersStarting(AzPodStatusMetadata event) {
    executor.execute(() -> processNonFinalState(event));
  }

  @Override
  public void onPodReady(AzPodStatusMetadata event) {
    executor.execute(() -> processNonFinalState(event));
  }

  @Override
  public void onPodCompleted(AzPodStatusMetadata event) {
    executor.execute(() -> processFinalState(event));
  }

  // Note that a Pod can end up in a InitFailure state while the corresponding flow is
  // in DISPATCHING or PREPARING state. Current implementation will finalize such flows to
  // a 'failed' state. In future we can consider examining the reason for failure within the
  // {@link AzPodStatusMetadata} and resubmitting the flow for dispatch accordingly.
  @Override
  public void onPodInitFailure(AzPodStatusMetadata event) {
    executor.execute(() -> processFinalState(event));
  }

  @Override
  public void onPodAppFailure(AzPodStatusMetadata event) {
    executor.execute(() -> processFinalState(event));
  }

  @Override
  public void onPodUnexpected(AzPodStatusMetadata event) {
    executor.execute(() -> processFinalState(event));
  }

  public void shutdown() {
    this.executor.shutdown();
    try {
      this.executor.awaitTermination(SHUTDOWN_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Executor service shutdown for flow-pod status listener was interrupted.",
          e);
    }
  }
}
