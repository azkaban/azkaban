/*
* Copyright 2018 LinkedIn Corp.
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
package azkaban.execapp;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Status;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import java.io.File;
import java.util.HashMap;
import org.junit.Test;

public class FlowRunnerTestYaml extends FlowRunnerTestBase {

  private static final String BASIC_FLOW_YAML_DIR = "basicflowwithoutendnode";
  private static final String FAIL_BASIC_FLOW_YAML_DIR = "failbasicflowwithoutendnode";
  private static final String EMBEDDED_FLOW_YAML_DIR = "embeddedflowwithoutendnode";
  private static final String BASIC_FLOW_NAME = "basic_flow";
  private static final String BASIC_FLOW_YAML_FILE = BASIC_FLOW_NAME + ".flow";
  private static final String FAIL_BASIC_FLOW_NAME = "fail_basic_flow";
  private static final String FAIL_BASIC_FLOW_YAML_FILE = FAIL_BASIC_FLOW_NAME + ".flow";
  private static final String EMBEDDED_FLOW_NAME = "embedded_flow";
  private static final String EMBEDDED_FLOW_YAML_FILE = EMBEDDED_FLOW_NAME + ".flow";
  private FlowRunnerTestUtil testUtil;

  @Test
  public void testBasicFlowWithoutEndNode() throws Exception {
    setUp(BASIC_FLOW_YAML_DIR, BASIC_FLOW_YAML_FILE);
    final HashMap<String, String> flowProps = new HashMap<>();
    this.runner = this.testUtil.createFromFlowMap(BASIC_FLOW_NAME, flowProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    FlowRunnerTestUtil.startThread(this.runner);
    assertFlowStatus(flow, Status.RUNNING);
    assertStatus("jobA", Status.SUCCEEDED);
    assertStatus("jobB", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.RUNNING);
    assertStatus("jobC", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.SUCCEEDED);
  }

  @Test
  public void testKillBasicFlowWithoutEndNode() throws Exception {
    setUp(BASIC_FLOW_YAML_DIR, BASIC_FLOW_YAML_FILE);
    final HashMap<String, String> flowProps = new HashMap<>();
    this.runner = this.testUtil.createFromFlowMap(BASIC_FLOW_NAME, flowProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    FlowRunnerTestUtil.startThread(this.runner);
    assertFlowStatus(flow, Status.RUNNING);
    assertStatus("jobA", Status.SUCCEEDED);
    assertStatus("jobB", Status.SUCCEEDED);
    this.runner.kill();
    assertStatus("jobC", Status.KILLED);
    assertFlowStatus(flow, Status.KILLED);
  }

  @Test
  public void testFailBasicFlowWithoutEndNode() throws Exception {
    setUp(FAIL_BASIC_FLOW_YAML_DIR, FAIL_BASIC_FLOW_YAML_FILE);
    final HashMap<String, String> flowProps = new HashMap<>();
    this.runner = this.testUtil.createFromFlowMap(FAIL_BASIC_FLOW_NAME, flowProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    FlowRunnerTestUtil.startThread(this.runner);
    assertFlowStatus(flow, Status.FAILED_FINISHING);
    assertStatus("jobC", Status.FAILED);
    assertStatus("jobB", Status.SUCCEEDED);
    assertStatus("jobA", Status.SUCCEEDED);
    assertStatus("jobD", Status.CANCELLED);
    assertFlowStatus(flow, Status.FAILED);
  }

  @Test
  public void testEmbeddedFlowWithoutEndNode() throws Exception {
    setUp(EMBEDDED_FLOW_YAML_DIR, EMBEDDED_FLOW_YAML_FILE);
    final HashMap<String, String> flowProps = new HashMap<>();
    this.runner = this.testUtil.createFromFlowMap(EMBEDDED_FLOW_NAME, flowProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    FlowRunnerTestUtil.startThread(this.runner);
    assertFlowStatus(flow, Status.RUNNING);
    assertStatus("jobA", Status.SUCCEEDED);
    assertStatus("embedded_flow1:jobB", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.RUNNING);
    assertStatus("embedded_flow1:jobC", Status.SUCCEEDED);
    assertStatus("embedded_flow1", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.RUNNING);
    assertStatus("jobD", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.SUCCEEDED);
  }

  private void setUp(final String projectDir, final String flowYamlFile) throws Exception {
    this.testUtil = new FlowRunnerTestUtil(projectDir, this.temporaryFolder);
    final Project project = this.testUtil.getProject();
    when(this.testUtil.getProjectLoader()
        .getLatestFlowVersion(project.getId(), project.getVersion(), flowYamlFile))
        .thenReturn(1);
    when(this.testUtil.getProjectLoader()
        .getUploadedFlowFile(eq(project.getId()), eq(project.getVersion()),
            eq(flowYamlFile),
            eq(1), any(File.class)))
        .thenReturn(
            ExecutionsTestUtil.getFlowFile(projectDir, flowYamlFile));
  }

}
