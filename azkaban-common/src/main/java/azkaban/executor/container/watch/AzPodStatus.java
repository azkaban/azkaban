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
 * information down the states and timestamps on individual containers but there is no direct
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
}
