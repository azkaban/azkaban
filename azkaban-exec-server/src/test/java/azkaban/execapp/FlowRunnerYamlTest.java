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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.Constants.ConfigurationKeys;
import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.Status;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import org.junit.Ignore;
import org.junit.Test;

public class FlowRunnerYamlTest extends FlowRunnerTestBase {

  private static final String BASIC_FLOW_YAML_DIR = "basicflowwithoutendnode";
  private static final String FAIL_BASIC_FLOW_YAML_DIR = "failbasicflowwithoutendnode";
  private static final String EMBEDDED_FLOW_YAML_DIR = "embeddedflowwithoutendnode";
  private static final String ALERT_FLOW_YAML_DIR = "alertflow";
  private static final String BASIC_FLOW_NAME = "basic_flow";
  private static final String BASIC_FLOW_YAML_FILE = BASIC_FLOW_NAME + ".flow";
  private static final String FAIL_BASIC_FLOW_NAME = "fail_basic_flow";
  private static final String FAIL_BASIC_FLOW_YAML_FILE = FAIL_BASIC_FLOW_NAME + ".flow";
  private static final String EMBEDDED_FLOW_NAME = "embedded_flow";
  private static final String EMBEDDED_FLOW_YAML_FILE = EMBEDDED_FLOW_NAME + ".flow";
  private static final String ALERT_FLOW_NAME = "alert_flow";
  private static final String ALERT_FLOW_YAML_FILE = ALERT_FLOW_NAME + ".flow";
  private FlowRunnerTestUtil testUtil;

  @Test
  public void testBasicFlowWithoutEndNode() throws Exception {
    setUp(BASIC_FLOW_YAML_DIR, BASIC_FLOW_YAML_FILE);
    final HashMap<String, String> flowProps = new HashMap<>();
    this.runner = this.testUtil.createFromFlowMap(BASIC_FLOW_NAME, flowProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    FlowRunnerTestUtil.startThread(this.runner);
    assertStatus("jobA", Status.SUCCEEDED);
    assertStatus("jobB", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.RUNNING);
    InteractiveTestJob.getTestJob("jobC").succeedJob();
    assertStatus("jobC", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.SUCCEEDED);
  }

  /**
   * There seems to be an actual race condition bug in the runtime code. See: issue #1921: Flaky
   * test FlowRunnerTestYaml & issue #1311: Potential race condition between flowRunner thread and
   * jetty killing thread. Disable this test until the potential bug is fixed or new DAG engine
   * code is ready.
   */
  @Ignore
  @Test
  public void testKillBasicFlowWithoutEndNode() throws Exception {
    setUp(BASIC_FLOW_YAML_DIR, BASIC_FLOW_YAML_FILE);
    final HashMap<String, String> flowProps = new HashMap<>();
    this.runner = this.testUtil.createFromFlowMap(BASIC_FLOW_NAME, flowProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    FlowRunnerTestUtil.startThread(this.runner);
    assertStatus("jobA", Status.SUCCEEDED);
    assertStatus("jobB", Status.SUCCEEDED);
    this.runner.kill();
    assertStatus("jobC", Status.KILLED);
    assertFlowStatus(flow, Status.KILLED);
  }

  @Ignore
  @Test
  public void testFailBasicFlowWithoutEndNode() throws Exception {
    setUp(FAIL_BASIC_FLOW_YAML_DIR, FAIL_BASIC_FLOW_YAML_FILE);
    final HashMap<String, String> flowProps = new HashMap<>();
    this.runner = this.testUtil.createFromFlowMap(FAIL_BASIC_FLOW_NAME, flowProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    FlowRunnerTestUtil.startThread(this.runner);
    InteractiveTestJob.getTestJob("jobC").failJob();
    assertStatus("jobC", Status.FAILED);
    InteractiveTestJob.getTestJob("jobB").succeedJob();
    assertStatus("jobB", Status.SUCCEEDED);
    InteractiveTestJob.getTestJob("jobA").succeedJob();
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
    assertStatus("jobA", Status.SUCCEEDED);
    assertStatus("embedded_flow1:jobB", Status.SUCCEEDED);
    assertStatus("embedded_flow1:jobC", Status.SUCCEEDED);
    assertStatus("embedded_flow1", Status.SUCCEEDED);
    assertStatus("jobD", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.SUCCEEDED);
  }

  @Test
  public void testAlertOnFlowFinished() throws Exception {
    setUp(ALERT_FLOW_YAML_DIR, ALERT_FLOW_YAML_FILE);
    final Alerter mailAlerter = mock(Alerter.class);
    final ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setFailureEmails(Arrays.asList("test@example.com"));
    final Props azkabanProps = new Props();
    azkabanProps.put(ConfigurationKeys.AZKABAN_POLL_MODEL, "true");
    this.runner = this.testUtil
        .createFromFlowMap(ALERT_FLOW_NAME, executionOptions, new HashMap<>(), azkabanProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    when(this.runner.getAlerterHolder().get("email")).thenReturn(mailAlerter);
    FlowRunnerTestUtil.startThread(this.runner);
    InteractiveTestJob.getTestJob("jobA").failJob();
    InteractiveTestJob.getTestJob("jobB").failJob();
    InteractiveTestJob.getTestJob("jobC").succeedJob();
    assertFlowStatus(flow, Status.FAILED);
    verify(mailAlerter).alertOnError(flow, "Flow finished");
  }

  @Test
  public void testAlertOnFirstError() throws Exception {
    setUp(ALERT_FLOW_YAML_DIR, ALERT_FLOW_YAML_FILE);
    final Alerter mailAlerter = mock(Alerter.class);
    final ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setNotifyOnFirstFailure(true);
    final Props azkabanProps = new Props();
    azkabanProps.put(ConfigurationKeys.AZKABAN_POLL_MODEL, "true");
    this.runner = this.testUtil
        .createFromFlowMap(ALERT_FLOW_NAME, executionOptions, new HashMap<>(), azkabanProps);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    when(this.runner.getAlerterHolder().get("email")).thenReturn(mailAlerter);
    FlowRunnerTestUtil.startThread(this.runner);
    InteractiveTestJob.getTestJob("jobA").failJob();
    assertFlowStatus(flow, Status.FAILED_FINISHING);
    InteractiveTestJob.getTestJob("jobB").failJob();
    assertFlowStatus(flow, Status.FAILED_FINISHING);
    InteractiveTestJob.getTestJob("jobC").succeedJob();
    assertFlowStatus(flow, Status.FAILED);
    verify(mailAlerter, times(1)).alertOnFirstError(flow);
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
