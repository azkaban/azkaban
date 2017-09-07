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

/**
 * Node in a DAG: Directed acyclic graph.
 */
class Node {

  private final String name;

  private final NodeProcessor nodeProcessor;

  // The nodes that this node depends on.
  private final Set<Node> parents = new HashSet<>();

  // The nodes that depend on this node.
  private final Set<Node> children = new HashSet<>();

  private Status status = Status.READY;

  private Flow flow;

  Node(final String name, final NodeProcessor nodeProcessor) {
    this.name = name;
    this.nodeProcessor = nodeProcessor;
  }

  public Flow getFlow() {
    return this.flow;
  }

  public void setFlow(final Flow flow) {
    this.flow = flow;
  }

  private Node addParent(final Node node) {
    this.parents.add(node);
    return this;
  }

  Node addChild(final Node node) {
    this.children.add(node);
    node.addParent(this);
    return this;
  }

  public boolean hasParent() {
    return !this.parents.isEmpty();
  }

  boolean isReady() {
    return this.status == Status.READY;
  }

  public void run() {
    changeStatus(Status.RUNNING);
    this.nodeProcessor.run(this);
  }

  private void saveStatus() {
    this.nodeProcessor.saveStatus(this, this.status);
  }

  public void markSuccess() {
    changeStatus(Status.SUCCESS);
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
    changeStatus(Status.FAILURE);
    for (final Node child : this.children) {
      child.cancel();
    }
    this.flow.checkFinished();
  }

  private void cancel() {
    // The node can't be in the running or killed state since one of its ancestors has failed or
    // been killed. It shouldn't have started.
    if (this.status != Status.DISABLED) {
      changeStatus(Status.CANCELED);
    }
    for (final Node node : this.children) {
      node.cancel();
    }
  }

  private void changeStatus(final Status status) {
    this.status = status;
    saveStatus();
  }

  void kill() {
    if (this.status == Status.READY || this.status == Status.BLOCKED) {
      this.status = Status.CANCELED;
    } else if (this.status == Status.RUNNING) {
      // kill the job
      this.status = Status.KILLING;
    }
  }

  boolean isInTerminalState() {
    return Status.TERMINAL_STATES.contains(this.status);
  }

  boolean isFailure() {
    return this.status == Status.FAILURE;
  }
}
