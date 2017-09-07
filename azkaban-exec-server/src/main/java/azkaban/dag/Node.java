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

//import static azkaban.dag.Status.FAILURE;
//import static azkaban.dag.Status.SUCCESS;
//import static azkaban.dag.Status.DISABLED;
//import static azkaban.dag.Status.READY;
//import static azkaban.dag.Status.RUNNING;

import static azkaban.dag.Status.FAILURE;
import static azkaban.dag.Status.KILLING;
import static azkaban.dag.Status.RUNNING;
import static azkaban.dag.Status.TERMINAL_STATES;

import java.util.HashSet;
import java.util.Set;

/**
 * Node in a DAG: Directed acyclic graph.
 */
class Node {

  private final String name;

  // The nodes that this node depends on.
  private final Set<Node> parents = new HashSet<>();

  // The nodes that depend on this node.
  private final Set<Node> children = new HashSet<>();

  private Status status = Status.READY;

  private Flow flow;

  Node(final String name) {
    this.name = name;
  }

  public Flow getFlow() {
    return this.flow;
  }

  public void setFlow(final Flow flow) {
    this.flow = flow;
  }

  Node addParent(final Node node) {
    this.parents.add(node);
    return this;
  }

  Node addChild(final Node node) {
    this.children.add(node);
    return this;
  }

  public boolean hasParent() {
    return !this.parents.isEmpty();
  }

  boolean isReady() {
    return this.status == Status.READY;
  }

  public void run() {
    this.status = Status.RUNNING;
  }

  public void markSuccess() {
    this.status = Status.SUCCESS;
    for (final Node child : this.children) {
      child.check();
    }
    this.flow.checkFinished();
  }

  public Set<Node> getChildren() {
    return this.children;
  }

  /**
   * Checks if all the dependencies are met and run if they are.
   */
  private void check() {
    if (this.status == Status.DISABLED) {
      return;
    }

    for (final Node node : this.parents) {
      if (!node.isSuccess()) {
        return;
      }
    }

    run();
  }

  /**
   * Returns true when the dependency requirement is considered satisfied.
   *
   * @return true if the status is either success or disabled.
   */
  private boolean isSuccess() {
    return this.status == Status.SUCCESS || this.status == Status.DISABLED;
  }

  void markFailure() {
    this.status = FAILURE;
    for (final Node child : this.children) {
      child.cancel();
    }
    this.flow.checkFinished();
  }

  private void cancel() {
    // The node can't be in the running or killed state since one of its ancestors has failed or
    // been killed. It shouldn't have started.
    if (this.status != Status.DISABLED) {
      this.status = Status.CANCELED;
    }
    for (final Node node : this.children) {
      node.cancel();
    }
  }

  void kill() {
    if (this.status == Status.READY || this.status == Status.BLOCKED) {
      this.status = Status.CANCELED;
    } else if (this.status == RUNNING) {
      // kill the job
      this.status = KILLING;
    }
  }

  boolean isInTerminalState() {
    return TERMINAL_STATES.contains(this.status);
  }

  boolean isFailure() {
    return this.status == FAILURE;
  }
}
