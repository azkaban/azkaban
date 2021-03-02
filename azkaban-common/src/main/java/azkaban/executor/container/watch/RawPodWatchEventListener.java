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

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

/**
 * Listener for un-processed Pod watch events. <br>
 * Pod Watch executors, such as {@link KubernetesWatch} will use an implementation of this interface
 * to drive further event processing.
 */
public interface RawPodWatchEventListener {

  /**
   * Process a pod watch event. Implementations are expected to be resilient to identical copies of
   * an event being delivered. This could happen, for example, due to  re-initialization of the
   * Pod watch, which can trigger delivery of duplicated events.
   *
   * @param watchEvent
   */
  void onEvent(Watch.Response<V1Pod> watchEvent);
}
