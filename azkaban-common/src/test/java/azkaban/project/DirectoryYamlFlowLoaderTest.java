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

import azkaban.Constants;
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
  private static final String RECURSIVE_DIRECTORY_FLOW_YAML_DIR = "recursivedirectoryyamltest";
  private static final String EMBEDDED_FLOW_YAML_DIR = "embeddedflowyamltest";
  private static final String MULTIPLE_EMBEDDED_FLOW_YAML_DIR = "multipleembeddedflowyamltest";
  private static final String CYCLE_FOUND_YAML_DIR = "cyclefoundyamltest";
  private static final String DUPLICATE_NODENAME_YAML_DIR = "duplicatenodenamesyamltest";
  private static final String DEPENDENCY_UNDEFINED_YAML_DIR = "dependencyundefinedyamltest";
  private static final String INVALID_JOBPROPS_YAML_DIR = "invalidjobpropsyamltest";
  private static final String NO_FLOW_YAML_DIR = "noflowyamltest";
  private static final String BASIC_FLOW_1 = "basic_flow";
  private static final String BASIC_FLOW_2 = "basic_flow2";
  private static final String EMBEDDED_FLOW = "embedded_flow";
  private static final String EMBEDDED_FLOW_1 = "embedded_flow" + Constants.PATH_DELIMITER +
      "embedded_flow1";
  private static final String EMBEDDED_FLOW_2 =
      "embedded_flow" + Constants.PATH_DELIMITER + "embedded_flow1" + Constants.PATH_DELIMITER
          + "embedded_flow2";
  private static final String EMBEDDED_FLOW_B = "embedded_flow_b";
  private static final String EMBEDDED_FLOW_B1 =
      "embedded_flow_b" + Constants.PATH_DELIMITER + "embedded_flow1";
  private static final String EMBEDDED_FLOW_B2 =
      "embedded_flow_b" + Constants.PATH_DELIMITER + "embedded_flow1" + Constants.PATH_DELIMITER
          + "embedded_flow2";
  private static final String DUPLICATE_NODENAME_FLOW_FILE = "duplicate_nodename.flow";
  private static final String DEPENDENCY_UNDEFINED_FLOW_FILE = "dependency_undefined.flow";
  private static final String CYCLE_FOUND_FLOW = "cycle_found";
  private static final String CYCLE_FOUND_ERROR = "Cycles found.";
  private static final String SHELL_PWD = "invalid_jobprops:shell_pwd";
  private Project project;

  @Before
  public void setUp() {
    this.project = new Project(12, "myTestProject");
  }

  @Test
  public void testLoadBasicYamlFile() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(BASIC_FLOW_YAML_DIR));
    checkFlowLoaderProperties(loader, 0, 1, 1);
    checkFlowProperties(loader, BASIC_FLOW_1, 0, 4, 1, 3, null);
  }

  @Test
  public void testLoadMultipleYamlFiles() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(MULTIPLE_FLOW_YAML_DIR));
    checkFlowLoaderProperties(loader, 0, 2, 2);
    checkFlowProperties(loader, BASIC_FLOW_1, 0, 4, 1, 3, null);
    checkFlowProperties(loader, BASIC_FLOW_2, 0, 3, 1, 2, null);
  }

  @Test
  public void testLoadYamlFileRecursively() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project,
        ExecutionsTestUtil.getFlowDir(RECURSIVE_DIRECTORY_FLOW_YAML_DIR));
    checkFlowLoaderProperties(loader, 0, 2, 2);
    checkFlowProperties(loader, BASIC_FLOW_1, 0, 3, 1, 2, null);
    checkFlowProperties(loader, BASIC_FLOW_2, 0, 4, 1, 3, null);
  }

  @Test
  public void testLoadEmbeddedFlowYamlFile() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(EMBEDDED_FLOW_YAML_DIR));
    checkFlowLoaderProperties(loader, 0, 3, 3);
    checkFlowProperties(loader, EMBEDDED_FLOW, 0, 4, 1, 3, null);
    checkFlowProperties(loader, EMBEDDED_FLOW_1, 0, 4, 1, 3, null);
    checkFlowProperties(loader, EMBEDDED_FLOW_2, 0, 2, 1, 1, null);
  }

  @Test
  public void testLoadMultipleEmbeddedFlowYamlFiles() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project,
        ExecutionsTestUtil.getFlowDir(MULTIPLE_EMBEDDED_FLOW_YAML_DIR));
    checkFlowLoaderProperties(loader, 0, 6, 6);
    checkFlowProperties(loader, EMBEDDED_FLOW, 0, 4, 1, 3, null);
    checkFlowProperties(loader, EMBEDDED_FLOW_1, 0, 4, 1, 3, null);
    checkFlowProperties(loader, EMBEDDED_FLOW_2, 0, 2, 1, 1, null);
    checkFlowProperties(loader, EMBEDDED_FLOW_B, 0, 4, 1, 3, null);
    checkFlowProperties(loader, EMBEDDED_FLOW_B1, 0, 4, 1, 3, null);
    checkFlowProperties(loader, EMBEDDED_FLOW_B2, 0, 2, 1, 1, null);
  }

  @Test
  public void testLoadInvalidFlowYamlFileWithDuplicateNodeNames() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project,
        ExecutionsTestUtil.getFlowDir(DUPLICATE_NODENAME_YAML_DIR));
    checkFlowLoaderProperties(loader, 1, 0, 0);
    assertThat(loader.getErrors()).containsExactly(
        "Failed to validate nodeBean for " + DUPLICATE_NODENAME_FLOW_FILE
            + ". Duplicate nodes found or dependency undefined.");
  }

  @Test
  public void testLoadInvalidFlowYamlFileWithUndefinedDependency() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project,
        ExecutionsTestUtil.getFlowDir(DEPENDENCY_UNDEFINED_YAML_DIR));
    checkFlowLoaderProperties(loader, 1, 0, 0);
    assertThat(loader.getErrors()).containsExactly(
        "Failed to validate nodeBean for " + DEPENDENCY_UNDEFINED_FLOW_FILE
            + ". Duplicate nodes found or dependency undefined.");
  }

  @Test
  public void testLoadInvalidFlowYamlFileWithCycle() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(CYCLE_FOUND_YAML_DIR));
    checkFlowLoaderProperties(loader, 1, 1, 1);
    checkFlowProperties(loader, CYCLE_FOUND_FLOW, 1, 4, 1, 4, CYCLE_FOUND_ERROR);
  }

  @Test
  public void testLoadFlowYamlFileWithInvalidJobProps() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project,
        ExecutionsTestUtil.getFlowDir(INVALID_JOBPROPS_YAML_DIR));
    checkFlowLoaderProperties(loader, 1, 1, 1);
    assertThat(loader.getErrors()).containsExactly(
        SHELL_PWD + ": Xms value has exceeded the allowed limit (max Xms = "
            + Constants.JobProperties.MAX_XMS_DEFAULT + ")");
  }

  @Test
  public void testLoadNoFlowYamlFile() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(NO_FLOW_YAML_DIR));
    checkFlowLoaderProperties(loader, 0, 0, 0);
  }

  private void checkFlowLoaderProperties(final DirectoryYamlFlowLoader loader, final int numError,
      final int numFlowMap, final int numEdgeMap) {
    assertThat(loader.getErrors().size()).isEqualTo(numError);
    assertThat(loader.getFlowMap().size()).isEqualTo(numFlowMap);
    assertThat(loader.getEdgeMap().size()).isEqualTo(numEdgeMap);
  }

  private void checkFlowProperties(final DirectoryYamlFlowLoader loader, final String flowName,
      final int numError, final int numNode, final int numFlowProps, final int numEdge, final
  String edgeError) {
    assertThat(loader.getFlowMap().containsKey(flowName)).isTrue();
    final Flow flow = loader.getFlowMap().get(flowName);
    if (numError != 0) {
      assertThat(flow.getErrors().size()).isEqualTo(numError);
    }
    assertThat(flow.getNodes().size()).isEqualTo(numNode);
    assertThat(flow.getAllFlowProps().size()).isEqualTo(numFlowProps);

    // Verify flow edges
    assertThat(loader.getEdgeMap().get(flowName).size()).isEqualTo(numEdge);
    assertThat(flow.getEdges().size()).isEqualTo(numEdge);
    for (final Edge edge : loader.getEdgeMap().get(flowName)) {
      this.logger.info(flowName + ".flow has edge: " + edge.getId());
      if (edge.getError() != null) {
        assertThat(edge.getError()).isEqualTo(edgeError);
      }
    }
  }
}
