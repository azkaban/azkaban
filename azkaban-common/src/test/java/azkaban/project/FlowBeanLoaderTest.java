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

import java.io.File;
import org.junit.Test;

public class FlowBeanLoaderTest {

  public static final String TEST_FLOW_NAME = "sample_flow";
  public static final String TEST_FLOW_YML_FILENAME = TEST_FLOW_NAME + ".yml";
  public static final String SHELL_END = "shell_end";
  public static final String SHELL_ECHO = "shell_echo";
  public static final String SHELL_BASH = "shell_bash";
  public static final String SHELL_PWD = "shell_pwd";
  public static final String ECHO_COMMAND = "echo \"This is an echoed text.\"";

  final File TEST_FLOW_YML_FILE =
      new File(getClass().getClassLoader().getResource(TEST_FLOW_YML_FILENAME).getFile());

  @Test
  public void testLoad() throws Exception {

    final FlowBeanLoader loader = new FlowBeanLoader();
    final FlowBean flowBean = loader.load(TEST_FLOW_YML_FILE);

    assertThat(flowBean.getConfig().get("flow-level-parameter")).isEqualTo("value");
    assertThat(flowBean.getNodes().size()).isEqualTo(4);

    final NodeBean node0 = flowBean.getNodes().get(0);
    assertThat(node0.getName()).isEqualTo(SHELL_END);
    assertThat(node0.getType()).isEqualTo("noop");
    assertThat(node0.getDependsOn()).contains(SHELL_PWD, SHELL_ECHO, SHELL_BASH);

    final NodeBean node1 = flowBean.getNodes().get(1);
    assertThat(node1.getName()).isEqualTo(SHELL_ECHO);
    assertThat(node1.getConfig().get("command")).isEqualTo(ECHO_COMMAND);
  }

  @Test
  public void testToAzkabanFlow() throws Exception {
    final FlowBeanLoader loader = new FlowBeanLoader();
    final FlowBean flowBean = loader.load(TEST_FLOW_YML_FILE);
    final AzkabanFlow flow = loader
        .toAzkabanFlow(loader.getFlowName(TEST_FLOW_YML_FILE), flowBean);

    assertThat(flow.getName()).isEqualTo(TEST_FLOW_NAME);
    assertThat(flow.getProps().get("flow-level-parameter")).isEqualTo("value");
    assertThat(flow.getNodes().size()).isEqualTo(4);

    final AzkabanJob shellEnd = flow.getJob(SHELL_END);
    assertThat(shellEnd.getName()).isEqualTo(SHELL_END);
    assertThat(shellEnd.getType()).isEqualTo("noop");
    assertThat(shellEnd.getProps().size()).isEqualTo(0);
    assertThat(shellEnd.getDependsOn()).contains(SHELL_PWD, SHELL_ECHO, SHELL_BASH);

    final AzkabanJob shellEcho = flow.getJob(SHELL_ECHO);
    assertThat(shellEcho.getName()).isEqualTo(SHELL_ECHO);
    assertThat(shellEcho.getType()).isEqualTo("command");
    assertThat(shellEcho.getProps().size()).isEqualTo(1);
    assertThat(shellEcho.getProps().get("command")).isEqualTo(ECHO_COMMAND);
  }

  @Test
  public void testGetFlowName() throws Exception {
    assertThat(new FlowBeanLoader().getFlowName(TEST_FLOW_YML_FILE)).isEqualTo(TEST_FLOW_NAME);
  }
}
