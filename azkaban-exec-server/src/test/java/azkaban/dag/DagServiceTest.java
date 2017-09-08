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
import org.junit.Before;
import org.junit.Test;

public class DagServiceTest {

  DagService dagService;

  @Before
  public void setUp() throws Exception {
    this.dagService = new DagService();
  }

  @After
  public void tearDown() throws Exception {
    this.dagService.shutdownAndAwaitTermination();
  }

  @Test
  /**
   * Tests a DAG with one node which will run successfully.
   */
  public void OneNodeSuccess() throws Exception {
    final NodeProcessor processor = new TestNodeProcessor(this.dagService);
    final Node aNode = new Node("a", processor);
    final CountDownLatch flowFinishedLatch = new CountDownLatch(1);
    final FlowProcessor flowProcessor = new TestFlowProcessor(flowFinishedLatch);
    final Flow flow = new Flow("fa", flowProcessor);
    flow.addNode(aNode);
    this.dagService.startFlow(flow);
    final boolean isWaitSuccessful = flowFinishedLatch.await(120, TimeUnit.SECONDS);

    // Make sure the flow finishes.
    assertThat(isWaitSuccessful).isTrue();
  }

  @Test
  /**
   * Tests a DAG with two nodes which will run successfully.
   * a -> b
   */
  public void TwoNodesSuccess() throws Exception {
    final NodeProcessor processor = new TestNodeProcessor(this.dagService);
    final Node aNode = new Node("a", processor);
    final Node bNode = new Node("b", processor);
    aNode.addChild(bNode);
    final CountDownLatch flowFinishedLatch = new CountDownLatch(1);
    final FlowProcessor flowProcessor = new TestFlowProcessor(flowFinishedLatch);
    final Flow flow = new Flow("fa", flowProcessor);
    flow.addNode(aNode);
    flow.addNode(bNode);
    this.dagService.startFlow(flow);
    final boolean isWaitSuccessful = flowFinishedLatch.await(120, TimeUnit.SECONDS);

    // Make sure the flow finishes.
    assertThat(isWaitSuccessful).isTrue();
  }

}
