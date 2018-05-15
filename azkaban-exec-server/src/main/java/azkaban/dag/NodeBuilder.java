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

import java.util.HashSet;
import java.util.Set;

public class NodeBuilder {

  private final String name;
  private final NodeProcessor nodeProcessor;

  private final DagBuilder dagBuilder;

  // The nodes that this node depends on
  private final Set<NodeBuilder> parents = new HashSet<>();

  public NodeBuilder(final String name, final NodeProcessor nodeProcessor,
      final DagBuilder dagBuilder) {
    requireNonNull(name, "The name of the NodeBuilder can't be null");
    this.name = name;
    requireNonNull(nodeProcessor, "The nodeProcessor of the NodeBuilder can't be null");
    this.nodeProcessor = nodeProcessor;
    requireNonNull(dagBuilder, "The dagBuilder of the NodeBuilder can't be null");
    this.dagBuilder = dagBuilder;
  }

  /**
   * Adds the given builder as a parent of this builder.
   *
   * <p>If the same builder is added multiple times to this builder, this builder will retain
   * only one reference to it.</p>
   */
  private void addParent(final NodeBuilder builder) {
    checkBuildersBelongToSameDag(builder);

    // Add the relationship to the data structure internal to the builder instead of changing
    // the associated node directly. This is done to prevent users of this method to change the
    // structure of the dag after the DagBuilder::build method is called.
    this.parents.add(builder);
  }

  /**
   * Checks if the given NodeBuilder belongs to the same DagBuilder as the current NodeBuilder.
   */
  private void checkBuildersBelongToSameDag(final NodeBuilder builder) {
    if (builder.dagBuilder != this.dagBuilder) {
      throw new DagException(String.format("Can't add a dependency from %s to %s since they "
          + "belong to different DagBuilders.", builder, this));
    }
  }

  /**
   * Add builders as parents of this builder.
   *
   * <p>This method handles de-duplication of builders.</p>
   *
   * @throws DagException if builders are not created from the same DagBuilder
   */
  public void addParents(final NodeBuilder... builders) {
    for (final NodeBuilder builder : builders) {
      addParent(builder);
    }
  }

  /**
   * Add builders as children of this builder.
   *
   * <p>This method handles de-duplication of builders.</p>
   *
   * @throws DagException if builders are not created from the same DagBuilder
   */
  public void addChildren(final NodeBuilder... builders) {
    for (final NodeBuilder builder : builders) {
      builder.addParent(this);
    }
  }

  Set<NodeBuilder> getParents() {
    return this.parents;
  }

  @Override
  public String toString() {
    return String.format("NodeBuilder (%s) in %s", this.name, this.dagBuilder);
  }

  String getName() {
    return this.name;
  }

  /**
   * Builds a Node and adds it to the given Dag.
   *
   * @param dag Dag to associate this node with
   * @return a node
   */
  public Node build(final Dag dag) {
    final Node node = new Node(this.name, this.nodeProcessor, dag);
    dag.addNode(node);
    return node;
  }
}
