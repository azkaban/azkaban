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

package azkaban.execapp;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.dag.Dag;
import azkaban.dag.DagBuilder;
import azkaban.dag.DagProcessor;
import azkaban.dag.DagService;
import azkaban.dag.Node;
import azkaban.dag.NodeProcessor;
import azkaban.dag.Status;
import azkaban.project.NodeBean;
import azkaban.project.NodeBeanLoader;
import azkaban.utils.ExecutorServiceUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Tests for running flows.
 */
public class FlowRunner2Test {

  private final DagService dagService = new DagService(new ExecutorServiceUtils());
  private final CountDownLatch flowFinishedLatch = new CountDownLatch(1);

  // The recorded event sequence.
  private final List<String> eventSequence = new ArrayList<>();

  @Test

  public void runSimpleV2Flow() throws Exception {
    final NodeBean flowNode = loadFlowNode();
    final Dag dag = createDag(flowNode);
    this.dagService.startDag(dag);
    this.flowFinishedLatch.await(2, TimeUnit.SECONDS);
    assertThat(this.eventSequence).isEqualTo(Arrays.asList("n1", "n2"));
    this.dagService.shutdownAndAwaitTermination();
  }

  private NodeBean loadFlowNode() throws Exception {
    final File flowFile = loadFlowFileFromResource();
    final NodeBeanLoader beanLoader = new NodeBeanLoader();
    return beanLoader.load(flowFile);
  }

  private Dag createDag(final NodeBean flowNode) {
    final DagCreator creator = new DagCreator(flowNode);
    return creator.create();

  }

  private class DagCreator {

    private final NodeBean flowNode;
    private final DagBuilder dagBuilder;

    DagCreator(final NodeBean flowNode) {
      final String flowName = flowNode.getName();
      this.flowNode = flowNode;
      this.dagBuilder = new DagBuilder(flowName, new SimpleDagProcessor());
    }

    Dag create() {
      createNodes();
      linkNodes();
      return this.dagBuilder.build();
    }

    private void createNodes() {
      for (final NodeBean node : this.flowNode.getNodes()) {
        createNode(node);
      }
    }

    private void createNode(final NodeBean node) {
      final String nodeName = node.getName();
      final SimpleNodeProcessor nodeProcessor = new SimpleNodeProcessor(nodeName, node.getConfig());
      this.dagBuilder.createNode(nodeName, nodeProcessor);
    }

    private void linkNodes() {
      for (final NodeBean node : this.flowNode.getNodes()) {
        linkNode(node);
      }
    }

    private void linkNode(final NodeBean node) {
      final String name = node.getName();
      final List<String> parents = node.getDependsOn();
      if (parents == null) {
        return;
      }
      for (final String parentNodeName : parents) {
        this.dagBuilder.addParentNode(name, parentNodeName);
      }
    }
  }

  private File loadFlowFileFromResource() {
    final ClassLoader loader = getClass().getClassLoader();
    return new File(loader.getResource("hello_world_flow.flow").getFile());
  }

  class SimpleDagProcessor implements DagProcessor {

    @Override
    public void changeStatus(final Dag dag, final Status status) {
      System.out.println(dag + " status changed to " + status);
      if (status.isTerminal()) {
        FlowRunner2Test.this.flowFinishedLatch.countDown();
      }
    }
  }

  class SimpleNodeProcessor implements NodeProcessor {

    private final String name;
    private final Map<String, String> config;

    SimpleNodeProcessor(final String name, final Map<String, String> config) {
      this.name = name;
      this.config = config;
    }

    @Override
    public void changeStatus(final Node node, final Status status) {
      System.out.println(node + " status changed to " + status);
      switch (status) {
        case RUNNING:
          System.out.println(String.format("Running with config: %s", this.config));
          FlowRunner2Test.this.dagService.markNodeSuccess(node);
          FlowRunner2Test.this.eventSequence.add(this.name);
          break;
        default:
          break;
      }
    }
  }
}
