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
 * A DAG (Directed acyclic graph) consists of {@link Node}s.
 *
 * <p>Most of the methods in this class should remain package private. Code outside of this
 * package should mainly interact with the {@link DagService}.
 */
public class Dag {

  private final String name;
  private final DagProcessor dagProcessor;
  private final List<Node> nodes = new ArrayList<>();
  private Status status = Status.READY;

  Dag(final String name, final DagProcessor dagProcessor) {
    requireNonNull(name, "The name of the Dag can't be null");
    this.name = name;
    requireNonNull(dagProcessor, "The dagProcessor parameter can't be null.");
    this.dagProcessor = dagProcessor;
  }

  /**
   * Adds a node to the current dag.
   *
   * <p>It's important NOT to expose this method as public. The design relies on this to ensure
   * correctness. The DAG's structure shouldn't change after it is created.
   *
   * <p>The DagBuilder will check that node names are unique within a dag. No check is necessary
   * here since the method package private and where it is called is carefully controlled within
   * a relatively small package.
   * </p>
   *
   * @param node a node to add
   */
  void addNode(final Node node) {
    assert (node.getDag() == this);
    this.nodes.add(node);
  }

  void start() {
    assert (this.status == Status.READY);
    changeStatus(Status.RUNNING);
    for (final Node node : this.nodes) {
      node.runIfAllowed();
    }
    // It's possible that all nodes are disabled. In this rare case the dag should be
    // marked success. Otherwise it will be stuck in the the running state.
    updateDagStatus();
  }

  void kill() {
    if (this.status.isTerminal() || this.status == Status.KILLING) {
      // It is possible that a kill is issued after a dag has finished or multiple kill requests
      // are received. Without this check, this method will make duplicate calls to the
      // DagProcessor.
      return;
    }
    changeStatus(Status.KILLING);
    for (final Node node : this.nodes) {
      node.kill();
    }
    updateDagStatus();
  }

  /**
   * Update the final dag status when all nodes are done.
   *
   * <p>If any node has not reached its terminal state, this method will simply return.
   */
  void updateDagStatus() {
    // A dag may have nodes that are disabled. It's safer to scan all the nodes.
    // Assume the overhead is minimal. If it is not the case, we can optimize later.
    boolean failed = false;
    for (final Node node : this.nodes) {
      final Status nodeStatus = node.getStatus();
      if (!nodeStatus.isTerminal()) {
        return;
      }
      if (nodeStatus == Status.FAILURE) {
        failed = true;
      }
    }

    // Update the dag status only after all nodes have reached terminal states.
    updateDagStatusInternal(failed);
  }

  /**
   * Update the final dag status.
   *
   * @param failed true if any of the jobs has failed
   */
  private void updateDagStatusInternal(final boolean failed) {
    if (this.status == Status.KILLING) {
      /*
      It's possible that some nodes have failed when the dag is killed.
      Since killing a dag signals an intent from an operator, it is more important to make
      the dag status reflect the result of that explict intent. e.g. if the killing is a
      result of handing a job failure, users more likely want to know that someone has taken
      an action rather than that a job has failed. Operators can still see the individual job
      status.
      */
      changeStatus(Status.KILLED);
    } else if (failed) {
      changeStatus(Status.FAILURE);
    } else {
      changeStatus(Status.SUCCESS);
    }
  }

  private void changeStatus(final Status status) {
    this.status = status;
    this.dagProcessor.changeStatus(this, status);
  }

  @Override
  public String toString() {
    return String.format("dag (%s), status (%s)", this.name, this.status);
  }

  String getName() {
    return this.name;
  }

  Status getStatus() {
    return this.status;
  }

  @VisibleForTesting
  void setStatus(final Status status) {
    this.status = status;
  }

  @VisibleForTesting
  public List<Node> getNodes() {
    return this.nodes;
  }
}
