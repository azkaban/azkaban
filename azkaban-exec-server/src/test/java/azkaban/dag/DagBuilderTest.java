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

import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class DagBuilderTest {

  private final DagBuilder dagBuilder = new DagBuilder("dag builder", mock(DagProcessor.class));

  @Test
  public void create_nodes_with_same_name_should_throw_an_exception() {
    final String name = "nb";
    // given
    createNode(name);

    // when
    final Throwable thrown = catchThrowable(() -> createNode(name));

    // then
    assertThrownIsDagBuilderException(thrown);
  }

  @Test
  public void build_should_return_expected_dag() {
    // given
    final String parentNodeName = "nb1";
    final String childNodeName = "nb2";
    createNode(parentNodeName);
    createNode(childNodeName);
    addParentNode(childNodeName, parentNodeName);

    // when
    final Dag dag = this.dagBuilder.build();

    // then
    assertThat(dag.getName()).isEqualTo("dag builder");
    assertDagNodes(dag);
  }

  /**
   * Adds parent as the child's parent node.
   */
  private void addParentNode(final String childName, final String parentName) {
    this.dagBuilder.addParentNode(childName, parentName);
  }

  private void assertDagNodes(final Dag dag) {
    final List<Node> nodes = dag.getNodes();
    assertThat(nodes.size()).isEqualTo(2);
    final Node node1 = nodes.get(0);
    final Node node2 = nodes.get(1);

    assertThat(node1.getName()).isEqualTo("nb1");
    assertThat(node2.getName()).isEqualTo("nb2");

    assertThat(node1.hasParent()).isFalse();
    assertThat(node1.getChildren()).isEqualTo(Collections.singletonList(node2));

    assertThat(node2.hasParent()).isTrue();
    assertThat(node2.getChildren()).isEmpty();
    assertThat(node2.getParents()).isEqualTo(Collections.singletonList(node1));
  }

  private Node createNode(final String name) {
    return this.dagBuilder.createNode(name, mock(NodeProcessor.class));
  }

  @Test
  public void build_should_throw_exception_when_circular_dependency_is_detected() {
    // given
    final String n1Name = "nb1";
    final String n2Name = "nb2";
    final String n3Name = "nb3";
    createNode(n1Name);
    createNode(n2Name);
    createNode(n3Name);

    addParentNode(n2Name, n1Name);
    addParentNode(n3Name, n2Name);
    addParentNode(n1Name, n3Name);

    // when
    final Throwable thrown = catchThrowable(this.dagBuilder::build);

    // then
    // Expect the exception message to show the loop: nb1 -> nb2 -> nb3 -> nb1.
    System.out.println("Expect exception: " + thrown);
    assertThrownIsDagBuilderException(thrown);
  }

  @Test
  public void can_not_call_createNode_after_dag_already_built() {
    // given
    this.dagBuilder.build();

    // when
    final Throwable thrown = catchThrowable(() -> createNode("a"));

    // then
    assertThrownIsDagBuilderException(thrown);
  }

  @Test
  public void can_not_call_addParentNode_after_dag_already_built() {
    // given
    this.dagBuilder.build();

    // when
    final Throwable thrown = catchThrowable(() -> addParentNode("n1", "n2"));

    // then
    assertThrownIsDagBuilderException(thrown);
  }

  @Test
  public void can_not_call_build_after_dag_already_built() {
    // given
    this.dagBuilder.build();

    // when
    final Throwable thrown = catchThrowable(this.dagBuilder::build);

    // then
    assertThrownIsDagBuilderException(thrown);
  }

  private void assertThrownIsDagBuilderException(final Throwable thrown) {
    assertThat(thrown).isInstanceOf(DagException.class);
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
