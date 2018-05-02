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
 */
class Flow {

  private final String name;
  private final FlowProcessor flowProcessor;
  private final List<Node> nodes = new ArrayList<>();
  private Status status = Status.READY;

  Flow(final String name, final FlowProcessor flowProcessor) {
    this.name = name;
    requireNonNull(flowProcessor, "flowProcessor parameter can't be null.");
    this.flowProcessor = flowProcessor;
  }

  void addNode(final Node node) {
    node.setFlow(this);
    this.nodes.add(node);
  }

  void start() {
    assert (this.status == Status.READY);
    changeStatus(Status.RUNNING);
    for (final Node node : this.nodes) {
      node.runIfAllowed();
    }
    // It's possible that all nodes are disabled. In this rare case the flow should be
    // marked success. Otherwise it will be stuck in the the running state.
    checkFinished();
  }

  void kill() {
    if (this.status.isTerminal() || this.status == Status.KILLING) {
      // It is possible that a kill is issued after a flow has finished or multiple kill requests
      // are received. Without this check, this method will make duplicate calls to the
      // FlowProcessor.
      return;
    }
    changeStatus(Status.KILLING);
    for (final Node node : this.nodes) {
      node.kill();
    }
    checkFinished();
  }

  /**
   * Checks if the flow is done.
   */
  void checkFinished() {
    // A flow may have nodes that are disabled. It's safer to scan all the nodes.
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

    if (this.status == Status.KILLING) {
      /*
      It's possible that some nodes have failed when the flow is killed.
      Since killing a flow signals an intent from an operator, it is more important to make
      the flow status reflect the result of that explict intent. e.g. if the killing is a
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
    this.flowProcessor.changeStatus(this, status);
  }

  @Override
  public String toString() {
    return String.format("Flow (%s), status (%s)", this.name, this.status);
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
}
