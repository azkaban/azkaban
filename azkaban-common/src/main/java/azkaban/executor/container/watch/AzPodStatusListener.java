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
 * Provides callback methods for processing of {@link AzPodStatus} states.
 * Each of the methods here directly corresponds to an enum value in{@link AzPodStatus}
 *
 * Method implementations are expected to be idempotent as it's possible to receive successive
 * events which all map to the same enum value in {@link AzPodStatus}
 *
 * All methods have a no-op as the default implementation and implementations of this interface
 * are free to override only a subset of these methods.
 */
public interface AzPodStatusListener {

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_REQUESTED
   * @param event
   */
  default void onPodRequested(AzPodStatusMetadata event) {}

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_SCHEDULED
   * @param event
   */
  default void onPodScheduled(AzPodStatusMetadata event) {}

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING
   * @param event
   */
  default void onPodInitContainersRunning(AzPodStatusMetadata event) {}

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_APP_CONTAINERS_RUNNING
   * @param event
   */
  default void onPodAppContainersStarting(AzPodStatusMetadata event) {}

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_READY
   * @param event
   */
  default void onPodReady(AzPodStatusMetadata event) {}

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_COMPLETED
   * @param event
   */
  default void onPodCompleted(AzPodStatusMetadata event) {}

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_INIT_FAILURE
   * @param event
   */
  default void onPodInitFailure(AzPodStatusMetadata event) {}

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_APP_FAILURE
   * @param event
   */
  default void onPodAppFailure(AzPodStatusMetadata event) {}

  /**
   * Invoked for Pod events classified as AzPodStatus.AZ_POD_UNEXPECTED
   * @param event
   */
  default void onPodUnexpected(AzPodStatusMetadata event) {}
}
