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
  final FlowProcessor flowProcessor = new TestFlowProcessor(this.flowFinishedLatch);
  final Flow testFlow = createFlow("fa");

  @After
  public void tearDown() throws Exception {
    this.dagService.shutdownAndAwaitTermination();
  }

  private Node createNode(final String name) {
    return new Node(name, this.nodeProcessor);
  }

  private Flow createFlow(final String name) {
    return new Flow(name, this.flowProcessor);
  }
  /**
   * Tests a DAG with one node which will run successfully.
   */
  @Test
  public void oneNodeSuccess() throws Exception {
    final Node aNode = createNode("a");
    this.testFlow.addNode(aNode);
    this.dagService.startFlow(this.testFlow);
    final boolean isWaitSuccessful = this.flowFinishedLatch.await(120, TimeUnit.SECONDS);

    // Make sure the flow finishes.
    assertThat(isWaitSuccessful).isTrue();
  }

  /**
   * Tests a DAG with two nodes which will run successfully.
   * a -> b
   */
  @Test
  public void twoNodesSuccess() throws Exception {
    final Node aNode = createNode("a");
    final Node bNode = createNode("b");
    aNode.addChild(bNode);
    this.testFlow.addNode(aNode);
    this.testFlow.addNode(bNode);
    this.dagService.startFlow(this.testFlow);
    final boolean isWaitSuccessful = this.flowFinishedLatch.await(120, TimeUnit.SECONDS);

    // Make sure the flow finishes.
    assertThat(isWaitSuccessful).isTrue();
  }
}
