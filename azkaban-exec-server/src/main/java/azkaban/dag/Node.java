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

  private Dag dag;

  Node(final String name, final NodeProcessor nodeProcessor) {
    this.name = name;
    requireNonNull(nodeProcessor, "The nodeProcessor parameter can't be null.");
    this.nodeProcessor = nodeProcessor;
  }

  public Dag getDag() {
    return this.dag;
  }

  public void setDag(final Dag flow) {
    this.dag = flow;
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

  /**
   * Transitions the node to the success state.
   */
  void markSuccess() {
    // It's possible that the flow is killed before this method is called.
    assertRunningOrKilling();
    changeStatus(Status.SUCCESS);
    for (final Node child : this.children) {
      child.runIfAllowed();
    }
    this.dag.updateDagStatus();
  }

  /**
   * Checks if all the dependencies are met and run if they are.
   */
  void runIfAllowed() {
    if (isReady()) {
      changeStatus(Status.RUNNING);
    }
  }

  /**
   * Transitions the node to the failure state.
   */
  void markFailed() {
    // It's possible that the flow is killed before this method is called.
    assertRunningOrKilling();
    changeStatus(Status.FAILURE);
    for (final Node child : this.children) {
      child.cancel();
    }
    //todo: HappyRay support failure options "Finish Current Running" and "Cancel All"
    this.dag.updateDagStatus();
  }

  private void cancel() {
    // The node shouldn't have started.
    assert (this.status.isPreRunState());
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
   * <p> A node is not designed to be killed individually. This method expects {@link Dag#kill()}
   * method to kill all nodes. Thus this method itself doesn't need to propagate the kill signal to
   * the node's children nodes.
   */
  void kill() {
    assert (this.dag.getStatus() == Status.KILLING);
    if (this.status == Status.READY || this.status == Status.BLOCKED) {
      // If the node is disabled, keep the status as disabled.
      changeStatus(Status.CANCELED);
    } else if (this.status == Status.RUNNING) {
      changeStatus(Status.KILLING);
    }
    // If the node has finished, leave the status intact.
  }

  /**
   * Transition the node from the killing state to the killed state.
   */
  void markKilled() {
    assert (this.status == Status.KILLING);
    changeStatus(Status.KILLED);
    this.dag.updateDagStatus();
  }

  @Override
  public String toString() {
    return String.format("Node (%s) status (%s) in (%s)", this.name, this.status, this.dag);
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
