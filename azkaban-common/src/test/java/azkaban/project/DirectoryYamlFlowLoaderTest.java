/*
* Copyright 2017 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

package azkaban.project;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryYamlFlowLoaderTest {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryYamlFlowLoaderTest
      .class);

  private static final String BASIC_FLOW_YAML_DIR = "basicflowyamltest";
  private static final String MULTIPLE_FLOW_YAML_DIR = "multipleflowyamltest";
  private static final String EMBEDDED_FLOW_YAML_DIR = "embeddedflowyamltest";
  private static final String INVALID_FLOW_YAML_DIR = "invalidflowyamltest";
  private static final String BASIC_FLOW_1 = "basic_flow";
  private static final String BASIC_FLOW_2 = "basic_flow2";
  private static final String EMBEDDED_FLOW = "embedded_flow";
  private static final String EMBEDDED_FLOW_1 = "embedded_flow1";
  private static final String EMBEDDED_FLOW_2 = "embedded_flow2";
  private static final String INVALID_FLOW_1 = "dependency_not_found";
  private static final String INVALID_FLOW_2 = "cycle_found";
  private static final String DEPENDENCY_NOT_FOUND_ERROR = "Dependency not found.";
  private static final String CYCLE_FOUND_ERROR = "Cycles found.";
  private Project project;

  @Before
  public void setUp() {
    this.project = new Project(12, "myTestProject");
  }

  @Test
  public void testLoadBasicYamlFile() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(BASIC_FLOW_YAML_DIR));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(1, loader.getFlowMap().size());
    Assert.assertTrue(loader.getFlowMap().containsKey(BASIC_FLOW_1));
    final Flow flow = loader.getFlowMap().get(BASIC_FLOW_1);

    Assert.assertEquals(4, flow.getNodes().size());

    Assert.assertEquals(1, loader.getEdgeMap().size());
    Assert.assertEquals(3, loader.getEdgeMap().get(BASIC_FLOW_1).size());
    Assert.assertEquals(3, flow.getEdges().size());
    for (final Edge edge : loader.getEdgeMap().get(BASIC_FLOW_1)) {
      this.logger.info(BASIC_FLOW_1 + " has edge: " + edge.getId());
      Assert.assertNull(edge.getError());
    }
  }

  @Test
  public void testLoadMultipleYamlFiles() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(MULTIPLE_FLOW_YAML_DIR));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(2, loader.getFlowMap().size());
    Assert.assertTrue(loader.getFlowMap().containsKey(BASIC_FLOW_1));
    Assert.assertTrue(loader.getFlowMap().containsKey(BASIC_FLOW_2));
    final Flow flow2 = loader.getFlowMap().get(BASIC_FLOW_2);
    Assert.assertEquals(3, flow2.getNodes().size());

    // Verify flow edges
    Assert.assertEquals(2, loader.getEdgeMap().size());
    Assert.assertEquals(3, loader.getEdgeMap().get(BASIC_FLOW_1).size());
    for (final Edge edge : loader.getEdgeMap().get(BASIC_FLOW_1)) {
      this.logger.info(BASIC_FLOW_1 + ".flow has edge: " + edge.getId());
      Assert.assertNull(edge.getError());
    }

    Assert.assertEquals(2, loader.getEdgeMap().get(BASIC_FLOW_2).size());
    Assert.assertEquals(2, flow2.getEdges().size());
    for (final Edge edge : loader.getEdgeMap().get(BASIC_FLOW_2)) {
      this.logger.info(BASIC_FLOW_2 + ".flow has edge: " + edge.getId());
      Assert.assertNull(edge.getError());
    }
  }

  @Test
  public void testLoadEmbeddedFlowYamlFile() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(EMBEDDED_FLOW_YAML_DIR));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(3, loader.getFlowMap().size());
    Assert.assertTrue(loader.getFlowMap().containsKey(EMBEDDED_FLOW));
    final Flow flow = loader.getFlowMap().get(EMBEDDED_FLOW);
    Assert.assertEquals(4, flow.getNodes().size());

    Assert.assertTrue(loader.getFlowMap().containsKey(EMBEDDED_FLOW_1));
    final Flow flow1 = loader.getFlowMap().get(EMBEDDED_FLOW_1);
    Assert.assertEquals(4, flow1.getNodes().size());

    Assert.assertTrue(loader.getFlowMap().containsKey(EMBEDDED_FLOW_2));
    final Flow flow2 = loader.getFlowMap().get(EMBEDDED_FLOW_2);
    Assert.assertEquals(2, flow2.getNodes().size());

    // Verify flow edges
    Assert.assertEquals(3, loader.getEdgeMap().size());
    Assert.assertEquals(3, loader.getEdgeMap().get(EMBEDDED_FLOW).size());
    Assert.assertEquals(3, flow.getEdges().size());
    for (final Edge edge : loader.getEdgeMap().get(EMBEDDED_FLOW)) {
      this.logger
          .info(EMBEDDED_FLOW + ".flow has edge: " + edge.getId());
      Assert.assertNull(edge.getError());
    }

    Assert.assertEquals(3, loader.getEdgeMap().get(EMBEDDED_FLOW_1).size());
    for (final Edge edge : loader.getEdgeMap().get(EMBEDDED_FLOW_1)) {
      this.logger.info(
          EMBEDDED_FLOW_1 + ".flow has edge: " + edge.getId());
      Assert.assertNull(edge.getError());
    }

    Assert.assertEquals(1, loader.getEdgeMap().get(EMBEDDED_FLOW_2).size());
    for (final Edge edge : loader.getEdgeMap().get(EMBEDDED_FLOW_2)) {
      this.logger.info(EMBEDDED_FLOW_2 + ".flow has edge: " + edge.getId());
      Assert.assertNull(edge.getError());
    }
  }

  @Test
  public void testLoadInvalidFlowYamlFiles() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(INVALID_FLOW_YAML_DIR));
    Assert.assertEquals(2, loader.getErrors().size());
    Assert.assertEquals(2, loader.getFlowMap().size());
    Assert.assertEquals(2, loader.getEdgeMap().size());

    // Invalid flow 1: Dependency not found.
    Assert.assertTrue(loader.getFlowMap().containsKey(INVALID_FLOW_1));
    final Flow flow1 = loader.getFlowMap().get(INVALID_FLOW_1);

    Assert.assertEquals(3, flow1.getNodes().size());
    Assert.assertEquals(1, flow1.getErrors().size());

    Assert.assertEquals(3, loader.getEdgeMap().get(INVALID_FLOW_1).size());
    Assert.assertEquals(3, flow1.getEdges().size());
    for (final Edge edge : loader.getEdgeMap().get(INVALID_FLOW_1)) {
      this.logger.info(INVALID_FLOW_1 + ".flow has edge: " + edge.getId());
      if (edge.getError() != null) {
        Assert.assertEquals(DEPENDENCY_NOT_FOUND_ERROR, edge.getError());
      }
    }

    // Invalid flow 2: Cycles found.
    Assert.assertTrue(loader.getFlowMap().containsKey(INVALID_FLOW_2));
    final Flow flow2 = loader.getFlowMap().get(INVALID_FLOW_2);

    Assert.assertEquals(4, flow2.getNodes().size());
    Assert.assertEquals(1, flow2.getErrors().size());

    Assert.assertEquals(4, loader.getEdgeMap().get(INVALID_FLOW_2).size());
    Assert.assertEquals(4, flow2.getEdges().size());
    for (final Edge edge : loader.getEdgeMap().get(INVALID_FLOW_2)) {
      this.logger.info(INVALID_FLOW_2 + ".flow has edge: " + edge.getId());
      if (edge.getError() != null) {
        Assert.assertEquals(CYCLE_FOUND_ERROR, edge.getError());
      }
    }
  }
}
