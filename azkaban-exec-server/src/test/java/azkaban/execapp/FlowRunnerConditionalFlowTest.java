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

package azkaban.execapp;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.Status;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import java.io.File;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;

public class FlowRunnerConditionalFlowTest extends FlowRunnerTestBase {

  private static final String FLOW_YAML_DIR = "conditionalflowyamltest";
  private static final String FLOW_NAME_1 = "conditional_flow1";
  private static final String FLOW_NAME_2 = "conditional_flow2";
  private static final String FLOW_YAML_FILE_1 = FLOW_NAME_1 + ".flow";
  private static final String FLOW_YAML_FILE_2 = FLOW_NAME_2 + ".flow";
  private FlowRunnerTestUtil testUtil;
  private Project project;

  @Before
  public void setUp() throws Exception {
    this.testUtil = new FlowRunnerTestUtil(FLOW_YAML_DIR, this.temporaryFolder);
    this.project = this.testUtil.getProject();
  }

  @Test
  public void runFlowOnJobPropsCondition() throws Exception {

    when(this.testUtil.getProjectLoader()
        .getLatestFlowVersion(this.project.getId(), this.project.getVersion(), FLOW_YAML_FILE_1))
        .thenReturn(1);
    when(this.testUtil.getProjectLoader()
        .getUploadedFlowFile(eq(this.project.getId()), eq(this.project.getVersion()),
            eq(FLOW_YAML_FILE_1),
            eq(1), any(File.class)))
        .thenReturn(ExecutionsTestUtil.getFlowFile(FLOW_YAML_DIR, FLOW_YAML_FILE_1));

    final HashMap<String, String> flowProps = new HashMap<>();
    flowProps.put("azkaban.server.name", "foo");
    final FlowRunner runner = this.testUtil.createFromFlowMap(FLOW_NAME_1, flowProps);
    final ExecutableFlow flow = runner.getExecutableFlow();

    FlowRunnerTestUtil.startThread(runner);

    assertStatus(flow, "jobA", Status.SUCCEEDED);
    assertStatus(flow, "jobB", Status.SUCCEEDED);
    assertStatus(flow, "jobC", Status.CANCELLED);
    assertStatus(flow, "jobD", Status.CANCELLED);
    assertFlowStatus(flow, Status.KILLED);
  }

  @Test
  public void runFlowOnJobOutputCondition() throws Exception {

    when(this.testUtil.getProjectLoader()
        .getLatestFlowVersion(this.project.getId(), this.project.getVersion(), FLOW_YAML_FILE_2))
        .thenReturn(1);
    when(this.testUtil.getProjectLoader()
        .getUploadedFlowFile(eq(this.project.getId()), eq(this.project.getVersion()),
            eq(FLOW_YAML_FILE_2),
            eq(1), any(File.class)))
        .thenReturn(ExecutionsTestUtil.getFlowFile(FLOW_YAML_DIR, FLOW_YAML_FILE_2));

    final HashMap<String, String> flowProps = new HashMap<>();
    final FlowRunner runner = this.testUtil.createFromFlowMap(FLOW_NAME_2, flowProps);
    final ExecutableFlow flow = runner.getExecutableFlow();

    FlowRunnerTestUtil.startThread(runner);

    assertStatus(flow, "jobA", Status.RUNNING);
    final Props generatedProperties = new Props();
    generatedProperties.put("key1", "value1");
    generatedProperties.put("key2", "value2");
    InteractiveTestJob.getTestJob("jobA").succeedJob(generatedProperties);
    assertStatus(flow, "jobA", Status.SUCCEEDED);
    assertStatus(flow, "jobB", Status.SUCCEEDED);
    assertStatus(flow, "jobC", Status.CANCELLED);
    assertStatus(flow, "jobD", Status.CANCELLED);
    assertFlowStatus(flow, Status.KILLED);
  }
}
