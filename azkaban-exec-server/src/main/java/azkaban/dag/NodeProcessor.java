/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.dag;

public interface NodeProcessor {

  /**
   * Changes the status of the node.
   *
   * <p>Typically a processor implementation should handle the RUNNING and KILLING status by
   * starting or killing a unit of work and call the {@link DagService} to transition the node
   * to the next status.
   *
   * <p>The call will be made in the context of the DagService's one and only thread. Thus a
   * processor should limit the time it takes to process the call. For lengthy operations such as
   * I/O operations, consider offloading them to other threads.
   *
   * @param node the node to change
   * @param status the new status
   */
  void changeStatus(Node node, Status status);
}
