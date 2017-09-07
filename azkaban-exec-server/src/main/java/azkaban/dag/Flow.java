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
  private final Set<Node> nodes = new HashSet<>();
  private Status status = Status.READY;

  Flow(final String name) {
    this.name = name;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

  Flow addNode(final Node node) {
    this.nodes.add(node);
    return this;
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

  void kill() {
    this.status = Status.KILLED;
    for (final Node node : this.nodes) {
      node.kill();
    }

  }

  /**
   * Checks if the flow is done.
   */
  void checkFinished() {
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
      this.status = Status.FAILURE;
    } else {
      this.status = Status.SUCCESS;
    }
  }

  void start() {
    final Set<Node> readyNodes = getInitialReadyNodes();
    for (final Node node : readyNodes) {
      node.run();
    }
    this.status = Status.RUNNING;
  }
}
