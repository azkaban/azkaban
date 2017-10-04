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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javafx.util.Pair;
import org.junit.After;
import org.junit.Test;

public class DagServiceTest {

  private final DagService dagService = new DagService();
  private final StatusChangeRecorder statusChangeRecorder = new StatusChangeRecorder();
  private final Set<Node> nodesToFail = new HashSet<>();
  private final TestNodeProcessor nodeProcessor = new TestNodeProcessor(this.dagService,
      this.statusChangeRecorder, this.nodesToFail);
  private final CountDownLatch flowFinishedLatch = new CountDownLatch(1);
  private final FlowProcessor flowProcessor = new TestFlowProcessor(this.flowFinishedLatch,
      this.statusChangeRecorder);
  private final Flow testFlow = createFlow("fa");
  private final List<Pair<String, Status>> expectedSequence = new ArrayList<>();


  @After
  public void tearDown() throws Exception {
    this.dagService.shutdownAndAwaitTermination();
  }

  /**
   * Tests a DAG with one node which will run successfully.
   */
  @Test
  public void oneNodeSuccess() throws Exception {
    createNodeAndAddToTestFlow("a");
    addToExpectedSequence("fa", Status.RUNNING);
    addToExpectedSequence("a", Status.RUNNING);
    addToExpectedSequence("a", Status.SUCCESS);
    addToExpectedSequence("fa", Status.SUCCESS);

    runAndVerify();
  }

  /**
   * Tests a DAG with two nodes which will run successfully.
   * a
   * |
   * b
   */
  @Test
  public void twoNodesSuccess() throws Exception {
    final Node aNode = createNodeAndAddToTestFlow("a");
    final Node bNode = createNodeAndAddToTestFlow("b");
    aNode.addChild(bNode);
    addToExpectedSequence("fa", Status.RUNNING);
    addToExpectedSequence("a", Status.RUNNING);
    addToExpectedSequence("a", Status.SUCCESS);
    addToExpectedSequence("b", Status.RUNNING);
    addToExpectedSequence("b", Status.SUCCESS);
    addToExpectedSequence("fa", Status.SUCCESS);

    runAndVerify();
  }

  /**
   * Tests a DAG with three nodes which will run successfully.
   * <pre>
   *    a
   *  /  \
   * b    c
   * </pre>
   */
  @Test
  public void threeNodesSuccess() throws Exception {
    final Node aNode = createNodeAndAddToTestFlow("a");
    final Node bNode = createNodeAndAddToTestFlow("b");
    final Node cNode = createNodeAndAddToTestFlow("c");
    aNode.addChildren(bNode, cNode);

    addToExpectedSequence("fa", Status.RUNNING);
    addToExpectedSequence("a", Status.RUNNING);
    addToExpectedSequence("a", Status.SUCCESS);
    addToExpectedSequence("b", Status.RUNNING);
    addToExpectedSequence("c", Status.RUNNING);
    addToExpectedSequence("b", Status.SUCCESS);
    addToExpectedSequence("c", Status.SUCCESS);
    addToExpectedSequence("fa", Status.SUCCESS);

    runAndVerify();

  }

  /**
   * Tests a DAG with one node which will fail.
   */
  @Test
  public void oneNodeFailure() throws Exception {
    final Node aNode = createNodeAndAddToTestFlow("a");
    this.nodesToFail.add(aNode);
    addToExpectedSequence("fa", Status.RUNNING);
    addToExpectedSequence("a", Status.RUNNING);
    addToExpectedSequence("a", Status.FAILURE);
    addToExpectedSequence("fa", Status.FAILURE);

    runAndVerify();
  }

  /**
   * Tests a DAG with two nodes, fails the first one.
   *
   * Expects the child node to be marked canceled.
   *
   * a (fail)
   * |
   * b
   */
  @Test
  public void twoNodesFailFirst() throws Exception {
    final Node aNode = createNodeAndAddToTestFlow("a");
    final Node bNode = createNodeAndAddToTestFlow("b");
    aNode.addChild(bNode);
    this.nodesToFail.add(aNode);

    addToExpectedSequence("fa", Status.RUNNING);
    addToExpectedSequence("a", Status.RUNNING);
    addToExpectedSequence("a", Status.FAILURE);
    addToExpectedSequence("b", Status.CANCELED);
    addToExpectedSequence("fa", Status.FAILURE);

    runAndVerify();
  }

  /**
   * Tests a DAG with three nodes with one failure.
   *
   * Expects the sibling nodes to finish.
   *
   * <pre>
   *       a
   *   /      \
   * b (fail)    c
   * </pre>
   */
  @Test
  public void threeNodesFailSecond() throws Exception {
    final Node aNode = createNodeAndAddToTestFlow("a");
    final Node bNode = createNodeAndAddToTestFlow("b");
    final Node cNode = createNodeAndAddToTestFlow("c");
    aNode.addChildren(bNode, cNode);

    this.nodesToFail.add(bNode);

    addToExpectedSequence("fa", Status.RUNNING);
    addToExpectedSequence("a", Status.RUNNING);
    addToExpectedSequence("a", Status.SUCCESS);
    addToExpectedSequence("b", Status.RUNNING);
    addToExpectedSequence("c", Status.RUNNING);
    addToExpectedSequence("b", Status.FAILURE);
    addToExpectedSequence("c", Status.SUCCESS);
    addToExpectedSequence("fa", Status.FAILURE);

    runAndVerify();

  }

  /**
   * Tests a DAG with one subflow, all successful.
   *
   * <pre>
   *   sfb ( subflow. "sb" stands for sub flow. This prefix is used in a node name.)
   *   |
   *   c
   *
   * subflow:
   * a b
   * </pre>
   */
  @Test
  public void simple_subflow_success_case() throws Exception {
    final TestSubFlowFlowProcessor testSubFlowFlowProcessor = new TestSubFlowFlowProcessor
        (this.dagService, this.statusChangeRecorder);
    final Flow bFlow = new Flow("fb", testSubFlowFlowProcessor);
    createNodeAndAddToFlow("a", bFlow);
    createNodeAndAddToFlow("b", bFlow);

    final TestSubFlowNodeProcessor testSubFlowNodeProcessor = new TestSubFlowNodeProcessor
        (this.dagService, this.statusChangeRecorder, bFlow);
    final Node subFlowNode = new Node("sfb", testSubFlowNodeProcessor);
    testSubFlowFlowProcessor.setNode(subFlowNode);
    this.testFlow.addNode(subFlowNode);

    final Node cNode = createNodeAndAddToTestFlow("c");
    subFlowNode.addChild(cNode);

    addToExpectedSequence("fa", Status.RUNNING);
    addToExpectedSequence("sfb", Status.RUNNING);
    addToExpectedSequence("fb", Status.RUNNING);
    addToExpectedSequence("a", Status.RUNNING);
    addToExpectedSequence("b", Status.RUNNING);
    addToExpectedSequence("a", Status.SUCCESS);
    addToExpectedSequence("b", Status.SUCCESS);
    addToExpectedSequence("fb", Status.SUCCESS);
    addToExpectedSequence("sfb", Status.SUCCESS);
    addToExpectedSequence("c", Status.RUNNING);
    addToExpectedSequence("c", Status.SUCCESS);
    addToExpectedSequence("fa", Status.SUCCESS);

    runAndVerify();

  }

  /**
   * Tests killing a flow.
   */
  @Test
  public void kill_a_node() throws Exception {
    final CountDownLatch jobRunningLatch = new CountDownLatch(1);
    final TestKillNodeProcessor killNodeProcessor = new TestKillNodeProcessor(this.dagService,
        this.statusChangeRecorder, jobRunningLatch);
    final Node aNode = new Node("a", killNodeProcessor);
    this.testFlow.addNode(aNode);

    addToExpectedSequence("fa", Status.RUNNING);
    addToExpectedSequence("a", Status.RUNNING);
    addToExpectedSequence("fa", Status.KILLING);
    addToExpectedSequence("a", Status.KILLING);
    addToExpectedSequence("a", Status.KILLED);
    addToExpectedSequence("fa", Status.KILLED);

    this.dagService.startFlow(this.testFlow);
    jobRunningLatch.await(120, TimeUnit.SECONDS);
    this.dagService.killFlow(this.testFlow);

    final boolean isWaitSuccessful = this.flowFinishedLatch.await(120, TimeUnit.SECONDS);
    // Make sure the flow finishes.
    assertThat(isWaitSuccessful).isTrue();
    verifyStatusSequence();
  }


  private void addToExpectedSequence(final String name, final Status status) {
    this.expectedSequence.add(new Pair(name, status));
  }

  private void runFlow() throws InterruptedException {
    this.dagService.startFlow(this.testFlow);
    final boolean isWaitSuccessful = this.flowFinishedLatch.await(120, TimeUnit.SECONDS);

    // Make sure the flow finishes.
    assertThat(isWaitSuccessful).isTrue();
  }

  private void verifyStatusSequence() {
    this.statusChangeRecorder.verifySequence(this.expectedSequence);
  }

  private void runAndVerify() throws InterruptedException {
    runFlow();
    verifyStatusSequence();
  }

  /**
   * Creates a node and add to the test flow.
   *
   * @param name node name
   * @return Node object
   */
  private Node createNode(final String name) {
    final Node node = new Node(name, this.nodeProcessor);
    return node;
  }

  private Node createNodeAndAddToFlow(final String name, final Flow flow) {
    final Node node = createNode(name);
    flow.addNode(node);
    return node;
  }

  private Node createNodeAndAddToTestFlow(final String name) {
    return createNodeAndAddToFlow(name, this.testFlow);
  }

  private Flow createFlow(final String name) {
    return new Flow(name, this.flowProcessor);
  }
}
