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

import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodCondition;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given an event of type {@link Watch.Response<V1Pod>} this class is useful for deriving
 * corresponding {@link AzPodStatus}. The states have the following interpretation.
 *
 *  AZ_POD_REQUESTED
 *  PodScheduled is missing or false.
 *
 *  AZ_POD_SCHEDULED
 *  PodScheduled is true, other conditions are missing or false, no init-containers running or
 *  completed.
 *
 *  AZ_POD_INIT_CONTAINERS_RUNNING
 *  PodScheduled is true, Initialized is false, at least 1 init-container running.
 *
 *  AZ_POD_APP_CONTAINERS_STARTING
 *  Initialized is true, but all application containers are waiting.
 *
 *  AZ_POD_READY
 *  ContainersReady is true, Ready is true. In absence of readiness gates both of these
 *  conditions are identical. We can consider splitting the AZ_POD_READY into 2 separate states
 *  if readiness gates are introduced and need to be accounted for in future.
 *
 *  AZ_POD_COMPLETED
 *  Phase is Succeeded.
 *
 *  AZ_POD_INIT_ERROR
 *  Phase is Failed. At least 1 init-container terminated with failure.
 *
 *  AZ_POD_APP_ERROR
 *  Phase is Failed. At least 1 application container terminated with failure.
 *
 *  AZ_POD_UNEXPECTED
 *  An event that can't be classified into any other AzPodStatus. These should be logged and
 *  tracked.
 *
 */
public class AzPodStatusExtractor {
  private static final Logger logger = LoggerFactory.getLogger(AzPodStatusExtractor.class);

  private final Watch.Response<V1Pod> podWatchEvent;
  private final V1Pod v1Pod;
  private final V1PodStatus v1PodStatus;
  private final List<V1PodCondition> podConditions;
  private final String podName;
  private Optional<V1PodCondition> scheduledCondition = Optional.empty();
  private Optional<V1PodCondition> containersReadyCondition = Optional.empty();
  private Optional<V1PodCondition> initializedCondition = Optional.empty();
  private Optional<V1PodCondition> readyCondition = Optional.empty();
  private Optional<PodConditionStatus> scheduledConditionStatus = Optional.empty();
  private Optional<PodConditionStatus> containersReadyConditionStatus = Optional.empty();
  private Optional<PodConditionStatus> initializedConditionStatus = Optional.empty();
  private Optional<PodConditionStatus> readyConditionStatus = Optional.empty();
  private PodPhase podPhase;

  /**
   * Construct the {@link AzPodStatusExtractor} from the given pod watch event.
   *
   * @param podWatchEvent
   */
  public AzPodStatusExtractor(Response<V1Pod> podWatchEvent) {
    requireNonNull(podWatchEvent, "pod watch response must not be null");
    requireNonNull(podWatchEvent.object, "watch v1Pod must not be null");
    this.podWatchEvent = podWatchEvent;
    this.v1Pod = podWatchEvent.object;
    this.podName = this.v1Pod.getMetadata().getName();

    requireNonNull(this.v1Pod.getStatus(), "pod status must not be null");
    requireNonNull(this.v1Pod.getStatus().getPhase(), "pod phase must not be null");
    this.v1PodStatus = this.v1Pod.getStatus();
    this.podConditions = this.v1Pod.getStatus().getConditions();

    if (this.podConditions != null) {
      extractConditions();
      extractConditionStatuses();
    }
    extractPhase();
  }

  public Response<V1Pod> getPodWatchEvent() {
    return this.podWatchEvent;
  }

  public V1Pod getV1Pod() {
    return this.v1Pod;
  }

  public V1PodStatus getV1PodStatus() {
    return this.v1PodStatus;
  }

  public String getPodName() {
    return this.podName;
  }

  public List<V1PodCondition> getPodConditions() {
    return this.podConditions;
  }

  public Optional<V1PodCondition> getScheduledCondition() {
    return this.scheduledCondition;
  }

  public Optional<V1PodCondition> getContainersReadyCondition() {
    return this.containersReadyCondition;
  }

  public Optional<V1PodCondition> getInitializedCondition() {
    return this.initializedCondition;
  }

  public Optional<V1PodCondition> getReadyCondition() {
    return this.readyCondition;
  }

  public Optional<PodConditionStatus> getScheduledConditionStatus() {
    return this.scheduledConditionStatus;
  }

  public Optional<PodConditionStatus> getContainersReadyConditionStatus() {
    return this.containersReadyConditionStatus;
  }

  public Optional<PodConditionStatus> getInitializedConditionStatus() {
    return this.initializedConditionStatus;
  }

  public Optional<PodConditionStatus> getReadyConditionStatus() {
    return this.readyConditionStatus;
  }

  public PodPhase getPodPhase() {
    return this.podPhase;
  }

  /**
   * Extract the 4 kubernetes conditions from the event. Events can have either any number of
   * conditions [0 - 4] present in them. Optional.empty() denotes a missing condition in the event.
   * @see
   * <a href="https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/">Pod Lifecycle</a>
   */
  private void extractConditions() {
    requireNonNull(this.podConditions, "pod status conditions must not be null");
    Map<String, V1PodCondition> conditionMap = new HashMap<>();
    this.podConditions.stream().forEach(
        condition ->
            conditionMap.put(condition.getType(), condition));
  this.scheduledCondition =
      Optional.ofNullable(conditionMap.remove(PodCondition.PodScheduled.name()));
  this.containersReadyCondition =
      Optional.ofNullable(conditionMap.remove(PodCondition.ContainersReady.name()));
  this.initializedCondition =
      Optional.ofNullable(conditionMap.remove(PodCondition.Initialized.name()));
  this.readyCondition = Optional.ofNullable(conditionMap.remove(PodCondition.Ready.name()));

  conditionMap.keySet().stream().forEach(type -> logger.warn("Unexpected condition of type: " + type));
  }

  /**
   * For each of the Conditions present in the the event, extract the corresponding status.
   * Optional.empty() denotes the corresponding condition was not present.
   */
  private void extractConditionStatuses() {
    this.scheduledCondition.ifPresent(cond -> {
      requireNonNull(cond.getStatus());
      this.scheduledConditionStatus = Optional.of(PodConditionStatus.valueOf(cond.getStatus()));
    });
    this.containersReadyCondition.ifPresent(cond -> {
      requireNonNull(cond.getStatus());
      this.containersReadyConditionStatus = Optional
          .of(PodConditionStatus.valueOf(cond.getStatus()));
    });
    this.initializedCondition.ifPresent(cond -> {
      requireNonNull(cond.getStatus());
      this.initializedConditionStatus =
          Optional.of(PodConditionStatus.valueOf(cond.getStatus()));
    });
    this.readyCondition.ifPresent(cond -> {
      requireNonNull(cond.getStatus());
      this.readyConditionStatus = Optional.of(PodConditionStatus.valueOf(cond.getStatus()));
    });
  }

  /**
   * Extract the Pod Phase from the event.
   * @see
   * <a href="https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/">Pod Lifecycle</a>
   */
  private void extractPhase() {
    requireNonNull(this.v1PodStatus.getPhase(), "pod status phase must not be null");
    // This will throw an IllegalArgumentException in case of an unexpected phase name.
    this.podPhase = PodPhase.valueOf(this.v1PodStatus.getPhase());
  }

  /**
   * Evaluate if event can be classified as status {@link AzPodStatus.AZ_POD_REQUESTED}
   * @return true if the classification succeeds, false otherwise.
   */
  private boolean checkForAzPodRequested() {
    // Scheduled conditions should either not be present or be false
    if (this.scheduledConditionStatus.isPresent() &&
        this.scheduledConditionStatus.get() == PodConditionStatus.True) {
      logger.debug("PodRequested is false as scheduled conditions is true");
      return false;
    }
    logger.debug("PodRequested is true");
    return true;
  }

  /**
   * Evaluate if event can be classified as status {@link AzPodStatus.AZ_POD_SCHEDULED}
   * @return true if the classification succeeds, false otherwise.
   */
  private boolean checkForAzPodScheduled() {
    // Pod must have been scheduled
    if (!this.scheduledConditionStatus.isPresent()) {
      logger.debug("PodScheduled false as scheduled condition is not present");
      return false;
    }
    if (this.scheduledConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("PodScheduled false as scheduled condition is not true");
      return false;
    }
    // Initialized condition is not present
    if (!this.initializedCondition.isPresent()) {
      logger.debug("PodScheduled true as initialized condition is not present");
      return true;
    }
    // Initialized condition is not true
    if (this.initializedConditionStatus.get() == PodConditionStatus.True) {
      logger.debug("PodScheduled false as initialized condition is true");
      return false;
    }
    // No init-containers should be running
    List<V1ContainerStatus> initContainerStatuses = this.v1PodStatus.getInitContainerStatuses();
    if (initContainerStatuses == null || initContainerStatuses.isEmpty()) {
      logger.debug("PodScheduled is true as init container status is null or empty");
      return true;
    }
    boolean anyContainerRunning = initContainerStatuses.stream().anyMatch(status ->
        (status.getState().getRunning() != null &&
            status.getState().getRunning().getStartedAt() != null) ||
            (status.getState().getTerminated() != null &&
                status.getState().getTerminated().getFinishedAt() != null));
    if (anyContainerRunning) {
      logger.debug("PodScheduled is false as an init container is running");
      return false;
    }
    logger.debug("PodScheduled is true");
    return true;
  }

  /**
   * Evaluate if event can be classified as status
   * {@link AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING}
   * @return true if the classification succeeds, false otherwise.
   */
  private boolean checkForAzPodInitContainersRunning() {
    // Pod must have scheduled
    if (!this.scheduledConditionStatus.isPresent() ||
        this.scheduledConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("InitRunning false as scheduled condition is not present or not true");
      return false;
    }
    // Initialization must have started, i.e condition should exist
    if (!this.initializedConditionStatus.isPresent()) {
      logger.debug("InitRunning false as initialized conditions is not present");
      return false;
    }
    // Initialization must not be complete
    if (this.initializedConditionStatus.get() == PodConditionStatus.True) {
      logger.debug("InitRunning false as initialized condition is true");
      return false;
    }
    logger.debug("InitRunning is true");
    return true;
  }

  /**
   * Evaluate if event can be classified as status
   * {@link AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING}
   * @return true if the classification succeeds, false otherwise.
   */
  private boolean checkForAzPodAppContainerStarting() {
    // Pod must have been initialized
    if (!this.initializedConditionStatus.isPresent() ||
        this.initializedConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("ContainerStarting false as initialized condition is not present or not true");
      return false;
    }
    // ContainersReady condition will not be True and all application containers should be waiting
    List<V1ContainerStatus> containerStatuses = this.v1PodStatus.getContainerStatuses();
    if (containerStatuses == null || containerStatuses.isEmpty()) {
      logger.debug("ContainerStarting false as container status is null or empty");
      return false;
    }
    boolean allContainersWaiting = containerStatuses.stream().allMatch(status ->
        status.getState().getWaiting() != null && status.getStarted() == false);
    if (!allContainersWaiting) {
      logger.debug("ContainerStarting false as all containers are not waiting");
      return false;
    }
    logger.debug("ContainerStarting is true");
    return true;
  }

  /**
   * Evaluate if event can be classified as status {@link AzPodStatus.AZ_POD_READY}
   * @return true if the classification succeeds, false otherwise.
   */
  private boolean checkForAzPodReady() {
    // ContainersReady condition must be True
    if (!this.containersReadyConditionStatus.isPresent() ||
        this.containersReadyConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("PodReady false as container-ready condition is not present or not true");
      return false;
    }
    // All application containers should be running
    List<V1ContainerStatus> containerStatuses = this.v1PodStatus.getContainerStatuses();
    if (containerStatuses == null || containerStatuses.isEmpty()) {
      logger.debug("PodReady false as container status is null or empty");
      return false;
    }
    boolean allContainersRunning = containerStatuses.stream().allMatch(status ->
        status.getState().getRunning() != null &&
            status.getState().getRunning().getStartedAt() != null);
    if (!allContainersRunning) {
      logger.debug("PodReady false as all containers are not running");
      return false;
    }
    logger.debug("PodReady is true");
    return true;
  }

  /**
   * Evaluate if event can be classified as status {@link AzPodStatus.AZ_POD_COMPLETED}
   * @return true if the classification succeeds, false otherwise.
   */
  private boolean checkForAzPodCompleted() {
    // Phase should be succeeded
    if(this.podPhase != PodPhase.Succeeded) {
      logger.debug("PodCompleted is false as phase is not succeeded");
      return false;
    }
    logger.debug("PodCompleted is true");
    return true;
  }

  /**
   * Evaluate if event can be classified as status {@link AzPodStatus.AZ_POD_INIT_FAILURE}
   * @return true if the classification succeeds, false otherwise.
   */
  private boolean checkForAzPodInitFailure() {
    // Phase must be failed.
      if (this.podPhase != PodPhase.Failed) {
        logger.debug("InitFailed is false and phase is not failed");
        return false;
    }
    // Initalized conditions should not be true
    if (this.initializedConditionStatus.isPresent() &&
        this.initializedConditionStatus.get() == PodConditionStatus.True) {
      logger.debug("InitFailed is failed as initialized conditions is not present or true");
      return false;
    }

    // There must be at least 1 failed init container
    List<V1ContainerStatus> initContainerStatuses = this.v1PodStatus.getInitContainerStatuses();
    if (initContainerStatuses == null || initContainerStatuses.isEmpty()) {
      logger.debug("InitFailed is false as init container status is null or empty");
      return false;
    }
    boolean anyContainerFailed = initContainerStatuses.stream().anyMatch(status ->
        status.getState().getTerminated() != null &&
            (status.getState().getTerminated().getExitCode() == null ||
                status.getState().getTerminated().getExitCode() != 0));
    if (!anyContainerFailed) {
      logger.debug("InitFailed is false as as all init container are terminated with exit code 0");
      return false;
    }
    logger.debug("InitFailed is true");
    return true;
  }

  /**
   * Evaluate if event can be classified as status {@link AzPodStatus.AZ_POD_APP_FAILURE}
   * @return true if the classification succeeds, false otherwise.
   */
  private boolean checkForAzPodAppFailure() {
    // Phase must be failed.
    if (this.podPhase != PodPhase.Failed) {
      logger.debug("AppFailed is false and phase is not failed");
      return false;
    }
    // Initialized condition should  be true
    if (!this.initializedConditionStatus.isPresent() ||
        this.initializedConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("AppFailed is failed as initialized conditions is not present or not true");
      return false;
    }
    // There must be at least 1 failed app container
    List<V1ContainerStatus> containerStatuses = this.v1PodStatus.getInitContainerStatuses();
    if (containerStatuses == null || containerStatuses.isEmpty()) {
      logger.debug("AppFailed is false as container status is null or empty");
      return false;
    }
    boolean anyContainerFailed = containerStatuses.stream().anyMatch(status ->
        status.getState().getTerminated() != null &&
            (status.getState().getTerminated().getExitCode() == null ||
                status.getState().getTerminated().getExitCode() != 0));
    if (!anyContainerFailed) {
      logger.debug("AppFailed is false as as no container terminated with non-zero exit code");
      return false;
    }
    logger.debug("AppFailed is true");
    return true;
  }

  /**
   * Return the {@link AzPodStatus} derived from given Pod watch event.
   * @return
   */
  public AzPodStatus createAzPodStatus() {
    if (checkForAzPodRequested()) {
      return AzPodStatus.AZ_POD_REQUESTED;
    }
    if (checkForAzPodScheduled()) {
      return AzPodStatus.AZ_POD_SCHEDULED;
    }
    if (checkForAzPodInitContainersRunning()) {
      return AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING;
    }
    if (checkForAzPodAppContainerStarting()) {
      return AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING;
    }
    if (checkForAzPodReady()) {
      return AzPodStatus.AZ_POD_READY;
    }
    if (checkForAzPodCompleted()) {
      return AzPodStatus.AZ_POD_COMPLETED;
    }
    if (checkForAzPodInitFailure()) {
      return AzPodStatus.AZ_POD_INIT_FAILURE;
    }
    if (checkForAzPodAppFailure()) {
      return AzPodStatus.AZ_POD_APP_FAILURE;
    }
    return AzPodStatus.AZ_POD_UNEXPECTED;
  }

  /**
   * Convenience method to create AzPodStatus from event in a single call.
   *
   * @param event
   * @return
   */
  public static AzPodStatusMetadata getAzPodStatusFromEvent(Watch.Response<V1Pod> event) {
    return new AzPodStatusMetadata(new AzPodStatusExtractor(event));
  }

  /**
   * Enum of all supported Condition names.
   * https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-and-container-status
   *
   * Unfortunately these values don't appear to be directly provided as enums in the kubernetes
   * client. (The only relevant references are for the grpc client supported values). Declaring
   * these values as enums is cleaner than using string literals.
   */
  private enum PodCondition {
    PodScheduled,
    ContainersReady,
    Initialized,
    Ready
  }

  /**
   * Enum of all supported Condition statuses.
   * https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-and-container-status
   */
  private enum PodConditionStatus {
    True,
    False,
    Unknown
  }

  /**
   * Enum of all supported Pod phases.
   * https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-and-container-status
   */
  private enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown
  }
}
