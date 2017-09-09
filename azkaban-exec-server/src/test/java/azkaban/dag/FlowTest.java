/*
 * Copyright 2017 LinkedIn Corp.
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

import org.junit.Test;

/**
 * Tests the flow state ( including its nodes' states) transitions.
 *
 * Focuses on how the flow state changes in response to one external request.
 */
public class FlowTest {

  private final Flow testFlow = new Flow("fa", new NullFlowProcessor());

  @Test
  public void runWithOneDisabledNode() throws Exception {
    final Node aNode = createAndAddNode("a");
    aNode.disable();
    this.testFlow.start();
    assertThat(aNode.getStatus()).isEqualTo(Status.DISABLED);
    assertThat(this.testFlow.getStatus()).isEqualTo(Status.SUCCESS);
  }

  private Node createNode(final String name) {
    return new Node(name, new NullNodeProcessor());
  }

  /**
   * Creates a node and add to the test flow.
   *
   * @param name node name
   * @return Node object
   */
  private Node createAndAddNode(final String name) {
    final Node node = createNode(name);
    this.testFlow.addNode(node);
    return node;
  }

}
