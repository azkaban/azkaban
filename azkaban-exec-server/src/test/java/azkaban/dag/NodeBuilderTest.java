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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class NodeBuilderTest {

  private final DagBuilder dagBuilder = mock(DagBuilder.class);
  private final NodeBuilder builder = createBuilder("builder");

  @Test
  public void addChildren() {
    // given
    final NodeBuilder builder2 = createBuilder("builder2");
    final NodeBuilder builder3 = createBuilder("builder3");

    // when
    this.builder.addChildren(builder2, builder3);
    assertParentMatch(builder2);
    assertParentMatch(builder3);
  }

  /**
   * Asserts that the parent of the given node is the test builder node.
   */
  private void assertParentMatch(final NodeBuilder builder) {
    final Set<NodeBuilder> parents = builder.getParents();
    assertThat(parents).isEqualTo(new HashSet<>(Arrays.asList(this
        .builder)));
  }

  @Test
  public void addParents() {
    // given
    final NodeBuilder builder2 = createBuilder("builder2");
    final NodeBuilder builder3 = createBuilder("builder3");

    // when
    this.builder.addParents(builder2, builder3);
    final Set<NodeBuilder> parents = this.builder.getParents();

    // then
    assertThat(parents).isEqualTo(new HashSet<>(Arrays.asList(builder2, builder3)));
  }

  private NodeBuilder createBuilder(final String name) {
    return new NodeBuilder(name, mock(NodeProcessor.class), this.dagBuilder);
  }

  @Test
  public void depend_on_node_in_a_different_dag_should_throw_exception() {
    // given
    final NodeBuilder builderInAnotherDag = new NodeBuilder("builder from another dag", mock
        (NodeProcessor.class), mock
        (DagBuilder.class));

    // when
    final Throwable thrown = catchThrowable(() -> {
      this.builder.addChildren(builderInAnotherDag);
    });

    // then
    assertThat(thrown).isInstanceOf(DagException.class);
  }

  @Test
  public void toStringTest() {
    // given
    final DagBuilder dagBuilder = new DagBuilder("dag", mock(DagProcessor.class));
    final NodeBuilder nodeBuilder = new NodeBuilder("node", mock(NodeProcessor.class),
        dagBuilder);

    // when
    final String stringRepresentation = nodeBuilder.toString();

    // then
    assertThat(stringRepresentation).isEqualTo("NodeBuilder (node) in DagBuilder (dag)");
  }
}
