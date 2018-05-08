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
    requireNonNull(nodeProcessor, "The nodeProcessor parameter can't be null.");
    this.nodeProcessor = nodeProcessor;
  }

  public Flow getFlow() {
    return this.flow;
  }

  public void setFlow(final Flow flow) {
    this.flow = flow;
  }

  private void addParent(final Node node) {
    this.parents.add(node);
  }

  void addChild(final Node node) {
    this.children.add(node);
    node.addParent(this);
  }

  void addChildren(final Node... nodes) {
    for (final Node node : nodes) {
      addChild(node);
    }
  }

  boolean hasParent() {
    return !this.parents.isEmpty();
  }

  /**
   * Checks if the node is ready to run.
   *
   * @return true if the node is ready to run
   */
  private boolean isReady() {
    if (this.status != Status.READY) {
      // e.g. if the node is disabled, it is not ready to run.
      return false;
    }
    for (final Node parent : this.parents) {
      if (!parent.status.isSuccessEffectively()) {
        return false;
      }
    }
    return true;
  }

  public void run() {
    assert (isReady());
    changeStatus(Status.RUNNING);
  }

  void markSuccess() {
    // It's possible that the flow is killed before this method is called.
    assertRunningOrKilling();
    changeStatus(Status.SUCCESS);
    for (final Node child : this.children) {
      child.runIfAllowed();
    }
    this.flow.updateFlowStatus();
  }

  /**
   * Checks if all the dependencies are met and run if they are.
   */
  void runIfAllowed() {
    if (isReady()) {
      run();
    }
  }

  void markFailure() {
    // It's possible that the flow is killed before this method is called.
    assertRunningOrKilling();
    changeStatus(Status.FAILURE);
    for (final Node child : this.children) {
      child.cancel();
    }
    this.flow.updateFlowStatus();
  }

  private void cancel() {
    // The node can't be in the running, killing, success, failure, killed states since this method
    // will only be called when one of its ancestors has failed or been killed. It shouldn't have
    // started.
    assert (this.status == Status.DISABLED || this.status == Status.READY
        || this.status == Status.BLOCKED);
    if (this.status != Status.DISABLED) {
      changeStatus(Status.CANCELED);
    }
    for (final Node node : this.children) {
      node.cancel();
    }
  }

  /**
   * Asserts that the state is running or killing.
   */
  private void assertRunningOrKilling() {
    assert (this.status == Status.RUNNING || this.status == Status.KILLING);
  }

  private void changeStatus(final Status status) {
    this.status = status;
    this.nodeProcessor.changeStatus(this, this.status);
  }

  /**
   * Kills a node.
   *
   * <p>Unlike other events, this method expects that the caller will check if the flow is finished.
   * This action will only be invoked by the {@link Flow#kill()} method, i.e. there is no
   * DagService#killNode method. In the interest of efficiency, it only needs to check once in
   * the Flow#kill method.
   */
  void kill() {
    if (this.status == Status.READY || this.status == Status.BLOCKED) {
      // If the node is disabled, keep the status as disabled.
      changeStatus(Status.CANCELED);
    } else if (this.status == Status.RUNNING) {
      changeStatus(Status.KILLING);
    }
    // If the node has finished, leave the status intact.
  }

  void markKilled() {
    assert (this.status == Status.KILLING);
    changeStatus(Status.KILLED);
    this.flow.updateFlowStatus();
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
