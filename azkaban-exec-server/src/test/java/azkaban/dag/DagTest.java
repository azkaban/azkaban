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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.Test;

/**
 * Tests the dag state ( including its nodes' states) transitions.
 *
 * Focuses on how the dag state changes in response to one external request.
 */
public class DagTest {

  private final DagProcessor mockDagProcessor = mock(DagProcessor.class);
  private final Dag testFlow = new Dag("fa", this.mockDagProcessor);

  @Test
  public void dag_finish_with_only_disabled_nodes() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.DISABLED);
    this.testFlow.start();
    assertThat(aNode.getStatus()).isEqualTo(Status.DISABLED);
    assertThat(this.testFlow.getStatus()).isEqualTo(Status.SUCCESS);
  }

  @Test
  public void running_nodes_can_be_killed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.RUNNING);
    this.testFlow.setStatus(Status.RUNNING);
    this.testFlow.kill();
    assertThat(aNode.getStatus()).isEqualTo(Status.KILLING);
    assertThat(this.testFlow.getStatus()).isEqualTo(Status.KILLING);
  }

  @Test
  public void kill_node_in_terminal_state_should_have_no_effect() {
    for (final Status status : Status.TERMINAL_STATES) {
      kill_dag_in_this_state_should_have_no_effect(status);
    }
  }

  @Test
  public void kill_node_in_killing_state_should_have_no_effect() {
    kill_dag_in_this_state_should_have_no_effect(Status.KILLING);
  }

  private void kill_dag_in_this_state_should_have_no_effect(final Status status) {
    // given
    this.testFlow.setStatus(status);

    // when
    this.testFlow.kill();

    // then
    assertThat(this.testFlow.getStatus()).isEqualTo(status);
    verify(this.mockDagProcessor, never()).changeStatus(any(), any());
  }

  /**
   * Tests ready nodes are canceled when the dag is killed.
   */
  @Test
  public void waiting_nodes_are_canceled_when_killed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.RUNNING);
    final Node bNode = createAndAddNode("b");
    bNode.addParent(aNode);
    this.testFlow.setStatus(Status.RUNNING);
    this.testFlow.kill();
    assertThat(aNode.getStatus()).isEqualTo(Status.KILLING);
    assertThat(bNode.getStatus()).isEqualTo(Status.CANCELED);
    assertThat(this.testFlow.getStatus()).isEqualTo(Status.KILLING);
  }

  /**
   * Tests multiple ready nodes are canceled when the dag is killed.
   * <pre>
   *     a (running)
   *    / \
   *   b   c
   *        \
   *         d
   * </pre>
   */
  @Test
  public void multiple_waiting_nodes_are_canceled_when_killed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.RUNNING);
    final Node bNode = createAndAddNode("b");
    bNode.addParent(aNode);
    final Node cNode = createAndAddNode("c");
    cNode.addParent(aNode);
    final Node dNode = createAndAddNode("d");
    dNode.addParent(cNode);

    this.testFlow.setStatus(Status.RUNNING);
    this.testFlow.kill();
    assertThat(aNode.getStatus()).isEqualTo(Status.KILLING);
    assertThat(bNode.getStatus()).isEqualTo(Status.CANCELED);
    assertThat(dNode.getStatus()).isEqualTo(Status.CANCELED);
    assertThat(dNode.getStatus()).isEqualTo(Status.CANCELED);
    assertThat(this.testFlow.getStatus()).isEqualTo(Status.KILLING);
  }

  /**
   * Tests multiple ready nodes are canceled when the parent node failed.
   * <pre>
   *     a (running)
   *     |
   *     b
   *     |
   *     c
   * </pre>
   */
  @Test
  public void multiple_waiting_children_are_canceled_when_parent_failed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.RUNNING);
    final Node bNode = createAndAddNode("b");
    bNode.addParent(aNode);
    final Node cNode = createAndAddNode("c");
    cNode.addParent(bNode);

    this.testFlow.setStatus(Status.RUNNING);
    aNode.markFailed();
    assertThat(bNode.getStatus()).isEqualTo(Status.CANCELED);
    assertThat(cNode.getStatus()).isEqualTo(Status.CANCELED);
  }

  /**
   * Tests blocked nodes are canceled when the dag is killed.
   */
  @Test
  public void blocked_nodes_are_canceled_when_killed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.RUNNING);
    final Node bNode = createAndAddNode("b");
    bNode.addParent(aNode);
    bNode.setStatus(Status.BLOCKED);
    this.testFlow.setStatus(Status.RUNNING);
    this.testFlow.kill();
    assertThat(aNode.getStatus()).isEqualTo(Status.KILLING);
    assertThat(bNode.getStatus()).isEqualTo(Status.CANCELED);
  }

  /**
   * Tests success nodes' states remain the same when the dag is killed.
   * <pre>
   *     a (success)
   *    /
   *   b (running)
   * </pre>
   */
  @Test
  public void success_node_state_remain_the_same_when_killed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.SUCCESS);
    final Node bNode = createAndAddNode("b");
    bNode.setStatus(Status.RUNNING);
    bNode.addParent(aNode);
    this.testFlow.kill();
    assertThat(aNode.getStatus()).isEqualTo(Status.SUCCESS);
    assertThat(bNode.getStatus()).isEqualTo(Status.KILLING);
  }

  /**
   * Tests failed nodes' states remain the same when the dag is killed.
   * This can happen when running jobs are allowed to finish when a node fails.
   *
   * <pre>
   *  a (running)   b (failure)
   * </pre>
   */
  @Test
  public void failed_node_state_remain_the_same_when_killed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.RUNNING);
    final Node bNode = createAndAddNode("b");
    bNode.setStatus(Status.FAILURE);
    this.testFlow.kill();
    assertThat(aNode.getStatus()).isEqualTo(Status.KILLING);
    assertThat(bNode.getStatus()).isEqualTo(Status.FAILURE);
  }

  /**
   * Creates a node and add to the test dag.
   *
   * @param name node name
   * @return Node object
   */
  private Node createAndAddNode(final String name) {
    final Node node = TestUtil.createNodeWithNullProcessor(name, this.testFlow);
    this.testFlow.addNode(node);
    return node;
  }
}
