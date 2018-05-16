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
import java.util.List;
import org.junit.Test;

public class DagBuilderTest {

  private final DagBuilder dagBuilder = new DagBuilder("dag builder", mock(DagProcessor.class));

  @Test
  public void create_nodes_with_same_name_should_throw_an_exception() {
    final String name = "nb";
    // given
    final NodeBuilder nodeBuilder1 = createNodeBuilder(name);

    // when
    final Throwable thrown = catchThrowable(() -> {
      createNodeBuilder(name);
    });

    // then
    assertThat(thrown).isInstanceOf(DagException.class);
  }

  @Test
  public void build_should_return_expected_dag() {
    // given
    final NodeBuilder nodeBuilder1 = createNodeBuilder("nb1");
    final NodeBuilder nodeBuilder2 = createNodeBuilder("nb2");
    nodeBuilder1.addChildren(nodeBuilder2);

    // when
    final Dag dag = this.dagBuilder.build();

    // then
    assertThat(dag.getName()).isEqualTo("dag builder");
    assertDagNodes(dag);
  }

  private void assertDagNodes(final Dag dag) {
    final List<Node> nodes = dag.getNodes();
    assertThat(nodes.size()).isEqualTo(2);
    final Node node1 = nodes.get(0);
    final Node node2 = nodes.get(1);

    assertThat(node1.getName()).isEqualTo("nb1");
    assertThat(node2.getName()).isEqualTo("nb2");

    assertThat(node1.hasParent()).isFalse();
    assertThat(node1.getChildren()).isEqualTo(Arrays.asList(node2));

    assertThat(node2.hasParent()).isTrue();
    assertThat(node2.getChildren()).isEmpty();
    assertThat(node2.getParents()).isEqualTo(Arrays.asList(node1));
  }

  private NodeBuilder createNodeBuilder(final String name) {
    return this.dagBuilder.createNode(name, mock(NodeProcessor.class));
  }

  @Test
  public void build_should_throw_exception_when_circular_dependency_is_detected() {
    // given
    final NodeBuilder nodeBuilder1 = createNodeBuilder("nb1");
    final NodeBuilder nodeBuilder2 = createNodeBuilder("nb2");
    final NodeBuilder nodeBuilder3 = createNodeBuilder("nb3");
    nodeBuilder2.addParents(nodeBuilder1);
    nodeBuilder3.addParents(nodeBuilder2);
    nodeBuilder1.addParents(nodeBuilder3);

    // when
    final Throwable thrown = catchThrowable(() -> {
      this.dagBuilder.build();
    });

    System.out.println("Expect exception: " + thrown);

    // then
    assertThat(thrown).isInstanceOf(DagException.class);
  }

  @Test
  public void add_dependency_should_not_affect_dag_already_built() {
    // given
    final Dag dag = this.dagBuilder.build();

    // when
    createNodeBuilder("a");

    // then
    final List<Node> nodes = dag.getNodes();
    assertThat(nodes).hasSize(0);
  }

  @Test
  public void test_toString() {
    // given

    // when
    final String stringRepresentation = this.dagBuilder.toString();

    // then
    assertThat(stringRepresentation).isEqualTo("DagBuilder (dag builder)");
  }
}
