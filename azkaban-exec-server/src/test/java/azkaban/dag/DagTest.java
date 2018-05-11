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
import static org.mockito.Mockito.mock;

import org.junit.Test;

/**
 * Tests the dag state ( including its nodes' states) transitions.
 *
 * Focuses on how the dag state changes in response to one external request.
 */
public class DagTest {

  private final Dag testFlow = new Dag("fa", mock(DagProcessor.class));

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

  /**
   * Tests ready nodes are canceled when the dag is killed.
   */
  @Test
  public void waiting_nodes_are_canceled_when_killed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.RUNNING);
    final Node bNode = createAndAddNode("b");
    aNode.addChild(bNode);
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
    aNode.addChild(bNode);
    final Node cNode = createAndAddNode("c");
    aNode.addChild(cNode);
    final Node dNode = createAndAddNode("d");
    cNode.addChild(dNode);

    this.testFlow.setStatus(Status.RUNNING);
    this.testFlow.kill();
    assertThat(aNode.getStatus()).isEqualTo(Status.KILLING);
    assertThat(bNode.getStatus()).isEqualTo(Status.CANCELED);
    assertThat(dNode.getStatus()).isEqualTo(Status.CANCELED);
    assertThat(dNode.getStatus()).isEqualTo(Status.CANCELED);
    assertThat(this.testFlow.getStatus()).isEqualTo(Status.KILLING);
  }

  /**
   * Tests blocked nodes are canceled when the dag is killed.
   */
  @Test
  public void blocked_nodes_are_canceled_when_killed() {
    final Node aNode = createAndAddNode("a");
    aNode.setStatus(Status.RUNNING);
    final Node bNode = createAndAddNode("b");
    aNode.addChild(bNode);
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
    aNode.addChild(bNode);
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
    final Node node = TestUtil.createNodeWithNullProcessor(name);
    this.testFlow.addNode(node);
    return node;
  }
}
