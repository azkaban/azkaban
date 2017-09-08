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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

public class DagServiceTest {

  final DagService dagService = new DagService();
  final TestNodeProcessor nodeProcessor = new TestNodeProcessor(this.dagService);
  final CountDownLatch flowFinishedLatch = new CountDownLatch(1);

  @After
  public void tearDown() throws Exception {
    this.dagService.shutdownAndAwaitTermination();
  }

  private Node createNode(final String name) {
    return new Node(name, this.nodeProcessor);
  }

  /**
   * Tests a DAG with one node which will run successfully.
   */
  @Test
  public void OneNodeSuccess() throws Exception {
    final Node aNode = createNode("a");
    final FlowProcessor flowProcessor = new TestFlowProcessor(this.flowFinishedLatch);
    final Flow flow = new Flow("fa", flowProcessor);
    flow.addNode(aNode);
    this.dagService.startFlow(flow);
    final boolean isWaitSuccessful = this.flowFinishedLatch.await(120, TimeUnit.SECONDS);

    // Make sure the flow finishes.
    assertThat(isWaitSuccessful).isTrue();
  }

  /**
   * Tests a DAG with two nodes which will run successfully.
   * a -> b
   */
  @Test
  public void TwoNodesSuccess() throws Exception {
    final Node aNode = createNode("a");
    final Node bNode = createNode("b");
    aNode.addChild(bNode);
    final FlowProcessor flowProcessor = new TestFlowProcessor(this.flowFinishedLatch);
    final Flow flow = new Flow("fa", flowProcessor);
    flow.addNode(aNode);
    flow.addNode(bNode);
    this.dagService.startFlow(flow);
    final boolean isWaitSuccessful = this.flowFinishedLatch.await(120, TimeUnit.SECONDS);

    // Make sure the flow finishes.
    assertThat(isWaitSuccessful).isTrue();
  }
}
