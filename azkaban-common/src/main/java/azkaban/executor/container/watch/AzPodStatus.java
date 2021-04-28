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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The Enum represents the different stages of the pod lifecycle.
 * While the Kubernetes API can provide very granular information about the current states
 * of various aspects of a POD, it doesn't quite provide any state-machine like representation of
 * POD life-cycle at a good enough granularity for use within Azkaban. <br>
 *   https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
 *
 * For example, a Pod Phase is very coarse and doesn't convey information regarding
 * pod-scheduling and init-container completions.
 * On the other hand Pod Conditions array along with the container statuses provides detailed
 * information down to the states and timestamps on individual containers but there is no direct
 * mechanism provided for distilling this information to a 'state' of generic pod (irrespective of
 * what or how many containers are included in the pod)
 *
 * Defining our own set of states also gives us the flexibility of splitting or merging states
 * based on how our needs evolve in future.
 *
 *   Unset
 *     |
 *  Scheduled --> InitContainersRunning --> AppContainersStarting --> Ready --> Completed
 *     |                   |                   |                        |          |
 *     |                   |                   |                        |          |
 *     '--> InitFailure <--'                   '-----> AppFailure <-----'          '--> Deleted
 *
 *
 * Note that the lifecycle state-machine currently assumes that PODs are single-use, such as for
 * FlowContainers. It's possible to extend it to PODs more generally but that seems unnecessary,
 * as in most of those cases monitoring Kubernetes ReplicaSets would be more useful.
 *
 */
public enum AzPodStatus {
  /**
   * Default state used for the flow pods whose actual state is not known.
   */
  AZ_POD_UNSET,

  /**
   * Pod creation has been received by kubernetes api-server but the pod is not scheduled yet.
   */
  AZ_POD_REQUESTED,

  /**
   * Pod has been scheduled (node ip is now available) but no init container have started.
   */
  AZ_POD_SCHEDULED,

  /**
   * Init containers are executing, all application containers are waiting.
   */
  AZ_POD_INIT_CONTAINERS_RUNNING,

  /**
   * Application container execution started (for at least 1 app container)
   */
  AZ_POD_APP_CONTAINERS_STARTING,

  /**
   * All application containers have started, pod status is 'ready'.
   */
  AZ_POD_READY,

  /**
   * Application containers exited without any errors.
   */
  AZ_POD_COMPLETED,

  /**
   * Failure during pod initialization.
   */
  AZ_POD_INIT_FAILURE,

  /**
   * Failure while running application containers.
   */
  AZ_POD_APP_FAILURE,

  /**
   * Pod was deleted.
   */
  AZ_POD_DELETED,

  /**
   * Status that can't be classified as one of the above.
   */
  AZ_POD_UNEXPECTED;

  /**
   * Contains static routines for validation of {@link AzPodStatus} transitions.
   */
  public static class TransitionValidator {
    private static final EnumSet<AzPodStatus> finalStatuses = EnumSet.of(
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

    private static EnumSet<AzPodStatus> getInvalidSourceStatusesForReady() {
      return finalStatuses;
    }

    private static EnumSet<AzPodStatus> getInvalidSourceStatusesForAppStarting() {
      EnumSet<AzPodStatus> invalidSources = EnumSet.copyOf(getInvalidSourceStatusesForReady());
      invalidSources.add(AZ_POD_READY);
      return invalidSources;
    }

    private static Set<AzPodStatus> getInvalidSourceStatusesForInitRunning() {
      EnumSet<AzPodStatus> invalidSources = EnumSet.copyOf(getInvalidSourceStatusesForAppStarting());
      invalidSources.add(AZ_POD_APP_CONTAINERS_STARTING);
      return invalidSources;
    }

    private static EnumSet<AzPodStatus> getInvalidSourceStatusesForScheduled() {
      EnumSet<AzPodStatus> invalidSources = EnumSet.copyOf(getInvalidSourceStatusesForInitRunning());
      invalidSources.add(AZ_POD_INIT_CONTAINERS_RUNNING);
      return invalidSources;
    }

    private static EnumSet<AzPodStatus> getInvalidSourceStatusesForRequested() {
      EnumSet<AzPodStatus> invalidSources = EnumSet.copyOf(getInvalidSourceStatusesForScheduled());
      invalidSources.add(AZ_POD_SCHEDULED);
      return invalidSources;
    }

    private static Set<AzPodStatus> getInvalidSourceStatusesForAppFailure() {
      return EnumSet.of(AZ_POD_INIT_FAILURE, AZ_POD_DELETED);
    }

    private static Set<AzPodStatus> getInvalidSourceStatusesForInitFailure() {
      return EnumSet.of(AZ_POD_APP_FAILURE, AZ_POD_DELETED);
    }

    private static Set<AzPodStatus> getInvalidSourceStatusesForUnexpected() {
      return EnumSet.of(AZ_POD_DELETED);
    }

    private static Set<AzPodStatus> getInvalidSourceStatusesForDeleted() {
      return ImmutableSet.of();
    }

    private static Set<AzPodStatus> getInvalidSourceStatusesForUnset() {
      return EnumSet.allOf(AzPodStatus.class);
    }

    // Build the invalid status static map.
    private static Map<AzPodStatus, Set<AzPodStatus>> buildInvalidStatusMap() {
      ImmutableMap.Builder builder = ImmutableMap.builder()
          .put(AZ_POD_READY, getInvalidSourceStatusesForReady())
          .put(AZ_POD_APP_CONTAINERS_STARTING, getInvalidSourceStatusesForAppStarting())
          .put(AZ_POD_INIT_CONTAINERS_RUNNING, getInvalidSourceStatusesForInitRunning())
          .put(AZ_POD_SCHEDULED, getInvalidSourceStatusesForScheduled())
          .put(AZ_POD_REQUESTED, getInvalidSourceStatusesForRequested())
          .put(AZ_POD_APP_FAILURE, getInvalidSourceStatusesForAppFailure())
          .put(AZ_POD_INIT_FAILURE, getInvalidSourceStatusesForInitFailure())
          .put(AZ_POD_UNEXPECTED, getInvalidSourceStatusesForUnexpected())
          .put(AZ_POD_DELETED, getInvalidSourceStatusesForDeleted())
          .put(AZ_POD_UNSET, getInvalidSourceStatusesForUnset());
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
}
