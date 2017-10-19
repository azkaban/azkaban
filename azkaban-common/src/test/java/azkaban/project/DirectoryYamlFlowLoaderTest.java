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

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
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
    assertThat(loader.getErrors().size()).isEqualTo(0);
    assertThat(loader.getFlowMap().size()).isEqualTo(1);
    assertThat(loader.getFlowMap().containsKey(BASIC_FLOW_1)).isTrue();
    final Flow flow = loader.getFlowMap().get(BASIC_FLOW_1);

    assertThat(flow.getNodes().size()).isEqualTo(4);
    assertThat(loader.getEdgeMap().size()).isEqualTo(1);
    assertFlowEdgeNoError(loader, flow, 3, BASIC_FLOW_1);
  }

  @Test
  public void testLoadMultipleYamlFiles() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(MULTIPLE_FLOW_YAML_DIR));
    assertThat(loader.getErrors().size()).isEqualTo(0);
    assertThat(loader.getFlowMap().size()).isEqualTo(2);
    assertThat(loader.getFlowMap().containsKey(BASIC_FLOW_1)).isTrue();
    assertThat(loader.getFlowMap().containsKey(BASIC_FLOW_2)).isTrue();

    final Flow flow1 = loader.getFlowMap().get(BASIC_FLOW_1);
    final Flow flow2 = loader.getFlowMap().get(BASIC_FLOW_2);
    assertThat(flow2.getNodes().size()).isEqualTo(3);

    // Verify flow edges
    assertThat(loader.getEdgeMap().size()).isEqualTo(2);
    assertFlowEdgeNoError(loader, flow1, 3, BASIC_FLOW_1);
    assertFlowEdgeNoError(loader, flow2, 2, BASIC_FLOW_2);
  }

  @Test
  public void testLoadEmbeddedFlowYamlFile() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(EMBEDDED_FLOW_YAML_DIR));
    assertThat(loader.getErrors().size()).isEqualTo(0);
    assertThat(loader.getFlowMap().size()).isEqualTo(3);
    assertThat(loader.getFlowMap().containsKey(EMBEDDED_FLOW)).isTrue();

    final Flow flow = loader.getFlowMap().get(EMBEDDED_FLOW);
    assertThat(flow.getNodes().size()).isEqualTo(4);

    assertThat(loader.getFlowMap().containsKey(EMBEDDED_FLOW_1)).isTrue();
    final Flow flow1 = loader.getFlowMap().get(EMBEDDED_FLOW_1);
    assertThat(flow1.getNodes().size()).isEqualTo(4);

    assertThat(loader.getFlowMap().containsKey(EMBEDDED_FLOW_2)).isTrue();
    final Flow flow2 = loader.getFlowMap().get(EMBEDDED_FLOW_2);
    assertThat(flow2.getNodes().size()).isEqualTo(2);

    // Verify flow edges
    assertThat(loader.getEdgeMap().size()).isEqualTo(3);
    assertFlowEdgeNoError(loader, flow, 3, EMBEDDED_FLOW);
    assertFlowEdgeNoError(loader, flow1, 3, EMBEDDED_FLOW_1);
    assertFlowEdgeNoError(loader, flow2, 1, EMBEDDED_FLOW_2);
  }

  @Test
  public void testLoadInvalidFlowYamlFiles() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(INVALID_FLOW_YAML_DIR));
    assertThat(loader.getErrors().size()).isEqualTo(2);
    assertThat(loader.getFlowMap().size()).isEqualTo(2);
    assertThat(loader.getEdgeMap().size()).isEqualTo(2);

    // Invalid flow 1: Dependency not found.
    assertThat(loader.getFlowMap().containsKey(INVALID_FLOW_1)).isTrue();
    final Flow flow1 = loader.getFlowMap().get(INVALID_FLOW_1);

    assertThat(flow1.getNodes().size()).isEqualTo(3);
    assertThat(flow1.getErrors().size()).isEqualTo(1);
    assertFlowEdgeHasError(loader, flow1, 3, INVALID_FLOW_1, DEPENDENCY_NOT_FOUND_ERROR);

    // Invalid flow 2: Cycles found.
    assertThat(loader.getFlowMap().containsKey(INVALID_FLOW_2)).isTrue();
    final Flow flow2 = loader.getFlowMap().get(INVALID_FLOW_2);

    assertThat(flow2.getNodes().size()).isEqualTo(4);
    assertThat(flow2.getErrors().size()).isEqualTo(1);
    assertFlowEdgeHasError(loader, flow2, 4, INVALID_FLOW_2, CYCLE_FOUND_ERROR);
  }

  /* Helper method to verify there is no error in the flow edge. */
  private void assertFlowEdgeNoError(final DirectoryYamlFlowLoader loader, final Flow flow,
      final int num_edge, final String flow_name) {
    assertThat(loader.getEdgeMap().get(flow_name).size()).isEqualTo(num_edge);
    assertThat(flow.getEdges().size()).isEqualTo(num_edge);
    for (final Edge edge : loader.getEdgeMap().get(flow_name)) {
      this.logger.info(flow_name + ".flow has edge: " + edge.getId());
      assertThat(edge.getError()).isNull();
    }
  }

  /* Helper method to verify there is an error in the flow edge. */
  private void assertFlowEdgeHasError(final DirectoryYamlFlowLoader loader, final Flow flow,
      final int num_edge, final String flow_name, final String error) {
    assertThat(loader.getEdgeMap().get(flow_name).size()).isEqualTo(num_edge);
    assertThat(flow.getEdges().size()).isEqualTo(num_edge);
    for (final Edge edge : loader.getEdgeMap().get(flow_name)) {
      this.logger.info(flow_name + ".flow has edge: " + edge.getId());
      if (edge.getError() != null) {
        assertThat(edge.getError()).isEqualTo(error);
      }
    }
  }
}
