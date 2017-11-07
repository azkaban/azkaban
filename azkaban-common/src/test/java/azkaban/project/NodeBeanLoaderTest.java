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
 *
 */

package azkaban.project;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.Constants;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import org.junit.Test;

public class NodeBeanLoaderTest {

  private static final String BASIC_FLOW_YML_TEST_DIR = "basicflowyamltest";
  private static final String BASIC_FLOW_NAME = "basic_flow";
  private static final String BASIC_FLOW_YML_FILE = BASIC_FLOW_NAME + ".flow";
  private static final String EMBEDDED_FLOW_YML_TEST_DIR = "embeddedflowyamltest";
  private static final String EMBEDDED_FLOW_NAME = "embedded_flow";
  private static final String EMBEDDED_FLOW_YML_FILE = EMBEDDED_FLOW_NAME + ".flow";
  private static final String FLOW_CONFIG_KEY = "flow-level-parameter";
  private static final String FLOW_CONFIG_VALUE = "value";
  private static final String SHELL_END = "shell_end";
  private static final String SHELL_ECHO = "shell_echo";
  private static final String SHELL_BASH = "shell_bash";
  private static final String SHELL_PWD = "shell_pwd";
  private static final String ECHO_COMMAND = "echo \"This is an echoed text.\"";
  private static final String ECHO_COMMAND_1 = "echo \"This is an echoed text from embedded_flow1.\"";
  private static final String PWD_COMMAND = "pwd";
  private static final String EMBEDDED_FLOW1 = "embedded_flow1";
  private static final String EMBEDDED_FLOW2 = "embedded_flow2";
  private static final String TYPE_NOOP = "noop";
  private static final String TYPE_COMMAND = "command";

  @Test
  public void testLoadNodeBeanForBasicFlow() throws Exception {

    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));

    assertThat(nodeBean.getName()).isEqualTo(BASIC_FLOW_NAME);
    assertThat(nodeBean.getType()).isEqualTo(Constants.FLOW_NODE_TYPE);
    assertThat(nodeBean.getConfig().get(FLOW_CONFIG_KEY)).isEqualTo(FLOW_CONFIG_VALUE);
    assertThat(nodeBean.getNodes().size()).isEqualTo(4);

    final NodeBean node0 = nodeBean.getNodes().get(0);
    assertThat(node0.getName()).isEqualTo(SHELL_END);
    assertThat(node0.getType()).isEqualTo(TYPE_NOOP);
    assertThat(node0.getDependsOn()).contains(SHELL_PWD, SHELL_ECHO, SHELL_BASH);

    final NodeBean node1 = nodeBean.getNodes().get(1);
    assertThat(node1.getName()).isEqualTo(SHELL_ECHO);
    assertThat(node1.getType()).isEqualTo(TYPE_COMMAND);
    assertThat(node1.getConfig().get(TYPE_COMMAND)).isEqualTo(ECHO_COMMAND);
  }

  @Test
  public void testLoadNodeBeanForEmbeddedFlow() throws Exception {

    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));

    assertThat(nodeBean.getName()).isEqualTo(EMBEDDED_FLOW_NAME);
    assertThat(nodeBean.getType()).isEqualTo(Constants.FLOW_NODE_TYPE);
    assertThat(nodeBean.getConfig().get(FLOW_CONFIG_KEY)).isEqualTo(FLOW_CONFIG_VALUE);
    assertThat(nodeBean.getNodes().size()).isEqualTo(4);

    final NodeBean node0 = nodeBean.getNodes().get(0);
    assertThat(node0.getName()).isEqualTo(SHELL_END);
    assertThat(node0.getType()).isEqualTo(TYPE_NOOP);
    assertThat(node0.getDependsOn()).contains(SHELL_PWD, SHELL_ECHO, EMBEDDED_FLOW1);

    final NodeBean node1 = nodeBean.getNodes().get(1);
    assertThat(node1.getName()).isEqualTo(SHELL_PWD);

    final NodeBean node2 = nodeBean.getNodes().get(2);
    assertThat(node2.getName()).isEqualTo(SHELL_ECHO);

    final NodeBean node3 = nodeBean.getNodes().get(3);
    assertThat(node3.getName()).isEqualTo(EMBEDDED_FLOW1);
    assertThat(node3.getType()).isEqualTo(Constants.FLOW_NODE_TYPE);
    assertThat(node3.getNodes().size()).isEqualTo(4);

    // Verify nodes in embedded_flow1 are loaded correctly.
    final NodeBean node3_0 = node3.getNodes().get(0);
    assertThat(node3_0.getName()).isEqualTo(SHELL_END);

    final NodeBean node3_1 = node3.getNodes().get(1);
    assertThat(node3_1.getName()).isEqualTo(SHELL_ECHO);

    final NodeBean node3_2 = node3.getNodes().get(2);
    assertThat(node3_2.getName()).isEqualTo(EMBEDDED_FLOW2);
    assertThat(node3_2.getType()).isEqualTo(Constants.FLOW_NODE_TYPE);
    assertThat(node3_2.getDependsOn()).contains(SHELL_BASH);
    assertThat(node3_2.getNodes().size()).isEqualTo(2);

    final NodeBean node3_3 = node3.getNodes().get(3);
    assertThat(node3_3.getName()).isEqualTo(SHELL_BASH);

    // Verify nodes in embedded_flow2 are loaded correctly.
    final NodeBean node3_2_0 = node3_2.getNodes().get(0);
    assertThat(node3_2_0.getName()).isEqualTo(SHELL_END);

    final NodeBean node3_2_1 = node3_2.getNodes().get(1);
    assertThat(node3_2_1.getName()).isEqualTo(SHELL_PWD);

  }

  @Test
  public void testToBasicAzkabanFlow() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));
    final AzkabanFlow flow = (AzkabanFlow) loader.toAzkabanNode(nodeBean);

    assertThat(flow.getName()).isEqualTo(BASIC_FLOW_NAME);
    assertThat(flow.getProps().get(FLOW_CONFIG_KEY)).isEqualTo(FLOW_CONFIG_VALUE);
    assertThat(flow.getNodes().size()).isEqualTo(4);

    final AzkabanJob shellEnd = (AzkabanJob) flow.getNode(SHELL_END);
    assertThat(shellEnd.getName()).isEqualTo(SHELL_END);
    assertThat(shellEnd.getType()).isEqualTo(TYPE_NOOP);
    assertThat(shellEnd.getProps().size()).isEqualTo(0);
    assertThat(shellEnd.getDependsOn()).contains(SHELL_PWD, SHELL_ECHO, SHELL_BASH);

    final AzkabanJob shellEcho = (AzkabanJob) flow.getNode(SHELL_ECHO);
    assertThat(shellEcho.getName()).isEqualTo(SHELL_ECHO);
    assertThat(shellEcho.getType()).isEqualTo(TYPE_COMMAND);
    assertThat(shellEcho.getProps().size()).isEqualTo(1);
    assertThat(shellEcho.getProps().get(TYPE_COMMAND)).isEqualTo(ECHO_COMMAND);
  }

  @Test
  public void testToEmbeddedAzkabanFlow() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));
    final AzkabanFlow flow = (AzkabanFlow) loader.toAzkabanNode(nodeBean);

    assertThat(flow.getName()).isEqualTo(EMBEDDED_FLOW_NAME);
    assertThat(flow.getProps().get(FLOW_CONFIG_KEY)).isEqualTo(FLOW_CONFIG_VALUE);
    assertThat(flow.getNodes().size()).isEqualTo(4);

    final AzkabanJob shellEnd = (AzkabanJob) flow.getNode(SHELL_END);
    assertThat(shellEnd.getName()).isEqualTo(SHELL_END);
    assertThat(shellEnd.getType()).isEqualTo(TYPE_NOOP);
    assertThat(shellEnd.getProps().size()).isEqualTo(0);
    assertThat(shellEnd.getDependsOn()).contains(SHELL_PWD, SHELL_ECHO, EMBEDDED_FLOW1);

    final AzkabanFlow embeddedFlow1 = (AzkabanFlow) flow.getNode(EMBEDDED_FLOW1);
    assertThat(embeddedFlow1.getName()).isEqualTo(EMBEDDED_FLOW1);
    assertThat(flow.getProps().get(FLOW_CONFIG_KEY)).isEqualTo(FLOW_CONFIG_VALUE);
    assertThat(embeddedFlow1.getNodes().size()).isEqualTo(4);
    assertThat(embeddedFlow1.getNodes().containsKey(EMBEDDED_FLOW2)).isTrue();

    final AzkabanFlow embeddedFlow2 = (AzkabanFlow) embeddedFlow1.getNode(EMBEDDED_FLOW2);
    assertThat(embeddedFlow2.getName()).isEqualTo(EMBEDDED_FLOW2);
    assertThat(flow.getProps().get(FLOW_CONFIG_KEY)).isEqualTo(FLOW_CONFIG_VALUE);
    assertThat(embeddedFlow2.getDependsOn()).contains(SHELL_BASH);
    assertThat(embeddedFlow2.getNodes().size()).isEqualTo(2);
    assertThat(embeddedFlow2.getNodes().containsKey(SHELL_END)).isTrue();
    assertThat(embeddedFlow2.getNodes().containsKey(SHELL_PWD)).isTrue();

  }

  @Test
  public void testGetFlowName() {
    assertThat(new NodeBeanLoader().getFlowName(ExecutionsTestUtil.getFlowFile(
        BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE))).isEqualTo(BASIC_FLOW_NAME);
  }

  @Test
  public void testGetFlowProps() {
    final Props flowProps = FlowLoaderUtils.getPropsFromYamlFile(BASIC_FLOW_NAME,
        ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));
    assertThat(flowProps.size()).isEqualTo(1);
    assertThat(flowProps.get(FLOW_CONFIG_KEY)).isEqualTo(FLOW_CONFIG_VALUE);
  }

  @Test
  public void testGetJobPropsFromBasicFlow() {
    final Props jobProps = FlowLoaderUtils
        .getPropsFromYamlFile(BASIC_FLOW_NAME + Constants.PATH_DELIMITER + SHELL_ECHO,
            ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));
    assertThat(jobProps.size()).isEqualTo(1);
    assertThat(jobProps.get(TYPE_COMMAND)).isEqualTo(ECHO_COMMAND);
  }

  @Test
  public void testGetJobPropsWithInvalidPath() {
    final Props jobProps = FlowLoaderUtils
        .getPropsFromYamlFile(BASIC_FLOW_NAME + Constants.PATH_DELIMITER + EMBEDDED_FLOW_NAME,
            ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));
    assertThat(jobProps).isNull();
  }

  @Test
  public void testGetJobPropsFromEmbeddedFlow() {
    // Get job props from parent flow
    String jobPrefix = EMBEDDED_FLOW_NAME + Constants.PATH_DELIMITER;
    Props jobProps = FlowLoaderUtils.getPropsFromYamlFile(jobPrefix + SHELL_ECHO,
        ExecutionsTestUtil.getFlowFile(EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));
    assertThat(jobProps.size()).isEqualTo(1);
    assertThat(jobProps.get(TYPE_COMMAND)).isEqualTo(ECHO_COMMAND);

    // Get job props from first level embedded flow
    jobPrefix = jobPrefix + EMBEDDED_FLOW1 + Constants.PATH_DELIMITER;
    jobProps = FlowLoaderUtils.getPropsFromYamlFile(jobPrefix + SHELL_ECHO,
        ExecutionsTestUtil.getFlowFile(EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));
    assertThat(jobProps.size()).isEqualTo(1);
    assertThat(jobProps.get(TYPE_COMMAND)).isEqualTo(ECHO_COMMAND_1);

    // Get job props from second level embedded flow
    jobPrefix = jobPrefix + EMBEDDED_FLOW2 + Constants.PATH_DELIMITER;
    jobProps = FlowLoaderUtils.getPropsFromYamlFile(jobPrefix + SHELL_PWD,
        ExecutionsTestUtil.getFlowFile(EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));
    assertThat(jobProps.size()).isEqualTo(1);
    assertThat(jobProps.get(TYPE_COMMAND)).isEqualTo(PWD_COMMAND);
  }
}
