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
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class FlowRunnerConditionalJobsTest extends FlowRunnerTestBase {

  private static final String FLOW_DIR = "conditionalflowtest";
  private static final String CONDITIONAL_FLOW_1 = "conditional_flow1";
  private static final String CONDITIONAL_FLOW_2 = "conditional_flow2";
  private static final String CONDITIONAL_FLOW_3 = "conditional_flow3";
  private static final String CONDITIONAL_FLOW_4 = "conditional_flow4";
  private static final String CONDITIONAL_FLOW_5 = "conditional_flow5";
  private static final String CONDITIONAL_FLOW_6 = "conditional_flow6";
  private static final String CONDITIONAL_FLOW_7 = "conditional_flow7";
  private FlowRunnerTestUtil testUtil;
  private Project project;


  @Ignore
  @Test
  public void runFlowOnJobPropsCondition() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    flowProps.put("azkaban.server.name", "foo");
    setUp(CONDITIONAL_FLOW_1, flowProps, "jobD");
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    assertStatus(flow, "jobA", Status.SUCCEEDED);
    assertStatus(flow, "jobB", Status.SUCCEEDED);
    assertStatus(flow, "jobC", Status.CANCELLED);
    assertStatus(flow, "jobD", Status.CANCELLED);
    assertFlowStatus(flow, Status.KILLED);
  }

  @Test
  public void runFlowOnJobOutputCondition() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    setUp(CONDITIONAL_FLOW_2, flowProps, "jobD");
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    assertStatus(flow, "jobA", Status.RUNNING);
    final Props generatedProperties = new Props();
    generatedProperties.put("key1", "value1");
    generatedProperties.put("key2", "value2");
    generatedProperties.put("key3", "value4");
    InteractiveTestJob.getTestJob((CONDITIONAL_FLOW_2 + ":") + "jobA").succeedJob(generatedProperties);
    assertStatus(flow, "jobA", Status.SUCCEEDED);
    assertStatus(flow, "jobB", Status.SUCCEEDED);
    assertStatus(flow, "jobC", Status.CANCELLED);
    assertStatus(flow, "jobD", Status.CANCELLED);
    assertFlowStatus(flow, Status.KILLED);
  }

  @Test
  public void runFlowOnJobStatusAllFailed() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    setUp(CONDITIONAL_FLOW_4, flowProps, "jobC");
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    InteractiveTestJob.getTestJob((CONDITIONAL_FLOW_4 + ":") + "jobA").failJob();
    assertStatus(flow, "jobA", Status.FAILED);
    assertStatus(flow, "jobB", Status.RUNNING);
    assertStatus(flow, "jobC", Status.READY);
    InteractiveTestJob.getTestJob((CONDITIONAL_FLOW_4 + ":") + "jobB").failJob();
    assertStatus(flow, "jobB", Status.FAILED);
    assertStatus(flow, "jobC", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.SUCCEEDED);
  }

  @Test
  public void runFlowOnJobStatusOneSuccess() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    setUp(CONDITIONAL_FLOW_5, flowProps, "jobC");
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    InteractiveTestJob.getTestJob((CONDITIONAL_FLOW_5 + ":") + "jobA").succeedJob();
    assertStatus(flow, "jobA", Status.SUCCEEDED);
    assertStatus(flow, "jobB", Status.RUNNING);
    assertStatus(flow, "jobC", Status.READY);
    InteractiveTestJob.getTestJob((CONDITIONAL_FLOW_5 + ":") + "jobB").failJob();
    assertStatus(flow, "jobB", Status.FAILED);
    assertStatus(flow, "jobC", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.SUCCEEDED);
  }

  @Test
  public void runFlowOnBothJobStatusAndPropsCondition() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    setUp(CONDITIONAL_FLOW_6, flowProps, "jobD");
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    final Props generatedProperties = new Props();
    generatedProperties.put("props", "foo");
    InteractiveTestJob.getTestJob((CONDITIONAL_FLOW_6 + ":") + "jobA").succeedJob(generatedProperties);
    assertStatus(flow, "jobA", Status.SUCCEEDED);
    assertStatus(flow, "jobB", Status.SUCCEEDED);
    assertStatus(flow, "jobC", Status.CANCELLED);
    assertStatus(flow, "jobD", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.SUCCEEDED);
  }

  @Test
  public void runFlowOnJobStatusConditionNull() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    setUp(CONDITIONAL_FLOW_3, flowProps, "jobC");
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    flow.getExecutableNode("jobC").setConditionOnJobStatus(null);
    InteractiveTestJob.getTestJob((CONDITIONAL_FLOW_3 + ":") + "jobA").succeedJob();
    assertStatus(flow, "jobA", Status.SUCCEEDED);
    InteractiveTestJob.getTestJob((CONDITIONAL_FLOW_3 + ":") + "jobB").succeedJob();
    assertStatus(flow, "jobB", Status.SUCCEEDED);
    assertStatus(flow, "jobC", Status.SUCCEEDED);
    assertFlowStatus(flow, Status.SUCCEEDED);
  }

  /**
   * JobB has defined "condition: var fImport = new JavaImporter(java.io.File); with(fImport) { var
   * f = new File('new'); f.createNewFile(); }"
   * Null ProtectionDomain will restrict this arbitrary code from creating a new file.
   * However it will not kick in when the change for condition allow-listing is implemented.
   * As a result, this test case will be ignored.
   *
   * @throws Exception the exception
   */
  @Ignore
  @Test
  public void runFlowOnArbitraryCondition() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    setUp(CONDITIONAL_FLOW_7, flowProps, "jobD");
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    assertStatus(flow, "jobA", Status.SUCCEEDED);
    assertStatus(flow, "jobB", Status.CANCELLED);
    assertFlowStatus(flow, Status.KILLED);
    // The arbitrary code should be restricted from creating a new file.
    final File file = new File("new.txt");
    Assert.assertFalse(file.exists());
  }

  private void setUp(final String flowName, final HashMap<String, String> flowProps, String rootJob)
      throws Exception {
    String dir = FLOW_DIR + "/" + flowName;
    this.testUtil = new FlowRunnerTestUtil(dir, this.temporaryFolder);
    if (System.getSecurityManager() == null) {
      Policy.setPolicy(new Policy() {
        @Override
        public boolean implies(final ProtectionDomain domain, final Permission permission) {
          return true; // allow all
        }
      });
      System.setSecurityManager(new SecurityManager());
    }

    this.project = this.testUtil.getProject();
    for (String job : Arrays.asList("jobA", "jobB", "jobC", "jobD")) {
      final String jobFile = job + ".job";
      when(this.testUtil.getProjectLoader()
          .getLatestFlowVersion(
              this.project.getId(),
              this.project.getVersion(),
              jobFile
          ))
          .thenReturn(1);
      when(this.testUtil.getProjectLoader()
          .getUploadedFlowFile(
              eq(this.project.getId()),
              eq(this.project.getVersion()),
              eq(jobFile),
              eq(1),
              any(File.class)
          ))
          .thenReturn(ExecutionsTestUtil.getFlowFile(dir, jobFile));
    }
    this.runner = this.testUtil.createFromFlowMap(rootJob, flowName);
    FlowRunnerTestUtil.startThread(this.runner);
  }
}
