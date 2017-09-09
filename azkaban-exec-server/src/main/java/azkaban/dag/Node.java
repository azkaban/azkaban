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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;

/**
 * Node in a DAG: Directed acyclic graph.
 */
class Node {

  private final String name;

  private final NodeProcessor nodeProcessor;

  // The nodes that this node depends on.
  private final List<Node> parents = new ArrayList<>();

  // The nodes that depend on this node.
  private final List<Node> children = new ArrayList<>();

  private Status status = Status.READY;

  private Flow flow;

  Node(final String name, final NodeProcessor nodeProcessor) {
    this.name = name;
    requireNonNull(nodeProcessor);
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

  Node addChildren(final Node... nodes) {
    for (final Node node : nodes) {
      addChild(node);
    }
    return this;
  }

  boolean hasParent() {
    return !this.parents.isEmpty();
  }

  boolean isReady() {
    if (this.status != Status.READY) {
      return false;
    }
    for (final Node parent : this.parents) {
      if (!parent.isSuccessEffectively()) {
        return false;
      }
    }
    return true;
  }

  public void run() {
    changeStatus(Status.RUNNING);
  }

  void markSuccess() {
    changeStatus(Status.SUCCESS);
    for (final Node child : this.children) {
      child.check();
    }
    this.flow.checkFinished();
  }

  /**
   * Checks if all the dependencies are met and run if they are.
   */
  private void check() {
    if (this.status == Status.DISABLED) {
      return;
    }

    for (final Node node : this.parents) {
      if (!node.isSuccessEffectively()) {
        return;
      }
    }

    run();
  }

  /**
   * @return true if the dependency requirement is considered satisfied
   */
  private boolean isSuccessEffectively() {
    return Status.isSuccessEffectively(this.status);
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
    this.nodeProcessor.changeStatus(this, this.status);
  }

  void kill() {
    if (this.status == Status.READY || this.status == Status.BLOCKED) {
      changeStatus(Status.CANCELED);
    } else if (this.status == Status.RUNNING) {
      // kill the job
      changeStatus(Status.KILLING);
    }
  }

  boolean isInTerminalState() {
    return Status.isTerminal(this.status);
  }

  boolean isFailure() {
    return this.status == Status.FAILURE;
  }

  @Override
  public String toString() {
    return String.format("Node (%s) status (%s) in %s", this.name, this.status, this.flow);
  }

  Status getStatus() {
    return this.status;
  }

  @VisibleForTesting
  void setStatus(final Status status) {
    this.status = status;
  }

  String getName() {
    return this.name;
  }
}
