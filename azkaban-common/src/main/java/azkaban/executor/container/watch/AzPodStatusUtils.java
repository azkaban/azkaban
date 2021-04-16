package azkaban.executor.container.watch;

import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_APP_FAILURE;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_COMPLETED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_DELETED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_INIT_FAILURE;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_READY;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_REQUESTED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_SCHEDULED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_UNEXPECTED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_UNSET;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains routines for validation of {@link AzPodStatus} transitions and any other processing
 * required for the statuses.
 */
public class AzPodStatusUtils {
  private static final Set<AzPodStatus> finalStatuses = EnumSet.of(
      AZ_POD_COMPLETED,
      AZ_POD_INIT_FAILURE,
      AZ_POD_APP_FAILURE,
      AZ_POD_DELETED,
      AZ_POD_UNEXPECTED
  );

  // A key in this map is the 'target' status and the value is the set of 'source statuses' from
  // which transition the the 'target' (key) status is not permitted and and event sequence
  // corresponding to these transitions should never be received.
  private static final Map<AzPodStatus, Set<AzPodStatus>> invalidSourceStatusMap =
      buildInvalidStatusMap();

  // Build the invalid status static map.
  private static Map<AzPodStatus, Set<AzPodStatus>> buildInvalidStatusMap() {
    ImmutableMap.Builder builder = ImmutableMap.builder()
        .put(AZ_POD_READY, finalStatuses);

    Set<AzPodStatus> mutableInvalidStatuses = new HashSet<>(finalStatuses);
    mutableInvalidStatuses.add(AZ_POD_READY);
    builder.put(AZ_POD_APP_CONTAINERS_STARTING, EnumSet.copyOf(mutableInvalidStatuses));

    mutableInvalidStatuses.add(AZ_POD_APP_CONTAINERS_STARTING);
    builder.put(AZ_POD_INIT_CONTAINERS_RUNNING, EnumSet.copyOf(mutableInvalidStatuses));

    mutableInvalidStatuses.add(AZ_POD_INIT_CONTAINERS_RUNNING);
    builder.put(AZ_POD_SCHEDULED, EnumSet.copyOf(mutableInvalidStatuses));

    mutableInvalidStatuses.add(AZ_POD_SCHEDULED);
    builder.put(AZ_POD_REQUESTED, EnumSet.copyOf(mutableInvalidStatuses));

    builder.put(AZ_POD_APP_FAILURE, EnumSet.of(AZ_POD_INIT_FAILURE, AZ_POD_DELETED));
    builder.put(AZ_POD_INIT_FAILURE, EnumSet.of(AZ_POD_APP_FAILURE, AZ_POD_DELETED));
    builder.put(AZ_POD_UNEXPECTED, EnumSet.of(AZ_POD_DELETED));
    builder.put(AZ_POD_DELETED, ImmutableSet.of());
    builder.put(AZ_POD_UNSET, EnumSet.allOf(AzPodStatus.class));
    return builder.build();
  }

  /**
   * Checks if the transition from {@code oldStatus} to {@code newStatus} is supported.
   * A return value of 'false' indicates a event status sequence that is not expected to occur in
   * practice and can help to identify problem with the event processing behavior.
   *
   * @param oldStatus
   * @param newStatus
   * @return true if transition is supported, false otherwise.
   */
  public static boolean isTransitionValid(AzPodStatus oldStatus, AzPodStatus newStatus) {
    Set<AzPodStatus> invalidSet = invalidSourceStatusMap
        .getOrDefault(newStatus, ImmutableSet.of());
    return !invalidSet.contains(oldStatus);
  }
}
