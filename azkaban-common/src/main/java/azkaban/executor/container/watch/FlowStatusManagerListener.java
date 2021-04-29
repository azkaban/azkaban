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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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

  private final ContainerizedImpl containerizedImpl;
  private final ExecutorLoader executorLoader;
  private final AlerterHolder alerterHolder;
  private final Cache<String, AzPodStatusMetadata> podStatusCache;

  // Note about the cache size.
  // Each incoming event is expected to be less than 5KB in size and the maximum cache size will be
  // about 5KB * maxCacheEntries.
  private final int maxCacheEntries;

  // Convenience member for referring to the Cache through ConcurrentMap interface.
  private final ConcurrentMap<String, AzPodStatusMetadata> podStatusMap;
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

    maxCacheEntries =
        azkProps.getInt(ContainerizedDispatchManagerProperties.KUBERNETES_WATCH_EVENT_CACHE_MAX_ENTRIES,
        DEFAULT_EVENT_CACHE_MAX_ENTRIES);
    this.podStatusCache = CacheBuilder.newBuilder()
        .maximumSize(maxCacheEntries)
        .recordStats()
        .build();
    this.podStatusMap = podStatusCache.asMap();
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
    AzPodStatus currentStatus = AzPodStatus.AZ_POD_UNSET;
    if (podStatusMap.containsKey(event.getPodName())) {
      currentStatus = podStatusMap.get(event.getPodName()).getAzPodStatus();
    }
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
    AzPodStatus currentStatus = AzPodStatus.AZ_POD_UNSET;
    if (podStatusMap.containsKey(event.getPodName())) {
      currentStatus = podStatusMap.get(event.getPodName()).getAzPodStatus();
    }
    boolean shouldSkip = currentStatus == event.getAzPodStatus();
    if (shouldSkip) {
      logger.info(format("Event pod status is same as current %s, for pod %s."
          +" Any updates will be skipped.", currentStatus, event.getPodName()));
    }
    return !shouldSkip;
  }

  // Update the cache with the given event.
  private void updatePodStatus(AzPodStatusMetadata event) {
    podStatusMap.put(event.getPodName(), event);
    logger.debug(format("Updated status to %s, for pod %s", event.getAzPodStatus(), event.getPodName()));
  }

  /**
   * Apply the boolean function {@code expectedStatusMatcher} to execution-id and if it returns true
   * then finalize the status of the flow in db.
   * This can be used for finalizing the flow based on whether the current Flow Status has a
   * specific value or is present within a given set of values.
   *
   * <p> Note:
   * Unfortunately many (if not all) of the Flow status updates in Azkaban don't fully enforce
   * the Flow lifecycle state-machine and {@code ExecutionControllerUtils.finalizeFlow} used
   * within this method is no exception. It will simply finalize the flow (as failed) even if the
   * flow status is in a finalized state in the Db. <br>
   * This indirectly impacts this listener implementation as occasionally more than one thread
   * could try to update the state of the same flow in db. Although it's not any worse than how
   * the rest of Azkaban already behaves, we should fix the behavior at least during the
   * finalization of flows. One way of achieving this is to add a utility method to atomically
   * test-and-finalize a flow from a non-final to a failed state.
   *
   * @implNote Flow status check and update is not atomic, details above.
   * @param event
   * @param expectedStatusMatcher
   * @return
   */
  private azkaban.executor.Status compareAndFinalizeFlowStatus(AzPodStatusMetadata event,
      Function<Status, Boolean> expectedStatusMatcher) {
    requireNonNull(expectedStatusMatcher, "flow matcher must not be null");

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
    checkState(executableFlow != null, "executable flow must not be null");
    azkaban.executor.Status originalStatus = executableFlow.getStatus();

    if (!expectedStatusMatcher.apply(originalStatus)) {
      logger.info(format(
          "Flow for pod %s does not have the expected status in database and will be finalized.",
          event.getPodName()));
      final String reason = "Flow Pod execution was completed.";
      ExecutionControllerUtils.finalizeFlow(executorLoader, alerterHolder, executableFlow, reason,
          null);
    }
    return originalStatus;
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

  @Override
  public void onPodCompleted(AzPodStatusMetadata event) {
    requireNonNull(event, "event must not be null");
    validateAndPreProcess(event);
    if (!isUpdatedPodStatusDistinct(event)) {
      return;
    }
    azkaban.executor.Status originalFlowStatus = compareAndFinalizeFlowStatus(event,
        azkaban.executor.Status::isStatusFinished);
    if (!Status.isStatusFinished(originalFlowStatus)) {
      logger.warn(format("Flow for pod %s was in the non-final state %s and was finalized",
          event.getPodName(), originalFlowStatus));
    }
    deleteFlowContainer(event);
    postProcess(event);
  }

  // Temporary exception generation method, for operations which are not yet implemented.
  private UnsupportedOperationException createUnsupportedException(AzPodStatusMetadata event) {
    String message = format("Callback for Pod status %s is not yet implemented. Pod name %s",
        event.getAzPodStatus(),
        event.getPodName());
    return new UnsupportedOperationException(message);
  }

  @Override
  public void onPodRequested(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodScheduled(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodInitContainersRunning(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodAppContainersStarting(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodReady(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodInitFailure(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodAppFailure(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodUnexpected(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }
}
