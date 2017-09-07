/*
 * Copyright 2017 LinkedIn Corp.
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

import java.util.HashSet;
import java.util.Set;

class Flow {

  private final String name;
  private final FlowProcessor flowProcessor;
  private final Set<Node> nodes = new HashSet<>();
  private Status status = Status.READY;

  Flow(final String name, final FlowProcessor flowProcessor) {
    this.name = name;
    this.flowProcessor = flowProcessor;
  }

  Flow addNode(final Node node) {
    this.nodes.add(node);
    return this;
  }

  void start() {
    final Set<Node> readyNodes = getInitialReadyNodes();
    for (final Node node : readyNodes) {
      node.run();
    }
    changeStatus(Status.RUNNING);
  }

  void kill() {
    changeStatus(Status.KILLED);
    for (final Node node : this.nodes) {
      node.kill();
    }

  }

  /**
   * Checks if the flow is done.
   */
  void checkFinished() {
    // A flow may have nodes that are disabled. It's safer to scan all the nodes.
    // The assumption is that the overhead is minimal. If it is not the case, we can optimize later.
    boolean failed = false;
    for (final Node node : this.nodes) {
      if (!node.isInTerminalState()) {
        return;
      }
      if (node.isFailure()) {
        failed = true;
      }
    }
    if (failed) {
      changeStatus(Status.FAILURE);
    } else {
      changeStatus(Status.SUCCESS);
    }
    this.flowProcessor.finish(this);
  }

  /**
   * Gets all the initial nodes that are ready to run.
   *
   * @return a set of nodes that are ready to run
   */
  private Set<Node> getInitialReadyNodes() {
    final Set<Node> readyNodes = new HashSet<>();
    for (final Node node : this.nodes) {
      if (node.isReady() && !node.hasParent()) {
        readyNodes.add(node);
      }
    }
    return readyNodes;
  }

  private void changeStatus(final Status status) {
    this.status = status;
  }
}
