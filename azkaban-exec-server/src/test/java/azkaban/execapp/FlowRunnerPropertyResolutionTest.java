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

import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.Status;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the property resolution of jobs in a flow.
 * <p>
 * The tests are contained in execpropstest, and should be resolved in the following fashion, where
 * the later props take precedence over the previous ones.
 * <p>
 * 1. Global props (set in the FlowRunner) 2. Shared job props (depends on job directory) 3.
 * Previous job outputs to the embedded flow (Only if contained in embedded flow) 4. Embedded flow
 * properties (Only if contained in embedded flow) 5. Previous job outputs (if exists) 6. Job Props
 * 7. Flow Override properties
 * <p>
 * The test contains the following structure: job2 -> innerFlow (job1 -> job4 ) -> job3
 * <p>
 * job2 and 4 are in nested directories so should have different shared properties than other jobs.
 */
public class FlowRunnerPropertyResolutionTest extends FlowRunnerTestBase {

  private static final String EXEC_FLOW_DIR = "execpropstest";
  private static final String FLOW_YAML_DIR = "loadpropsflowyamltest";
  private static final String FLOW_NAME = "job3";
  private static final String FLOW_YAML_FILE = FLOW_NAME + ".flow";
  private FlowRunnerTestUtil testUtil;

  /**
   * Tests the basic flow resolution. Flow is defined in execpropstest
   */
  @Test
  public void testPropertyResolution() throws Exception {
    this.testUtil = new FlowRunnerTestUtil(EXEC_FLOW_DIR, this.temporaryFolder);
    assertProperties(false);
  }

  /**
   * Tests the YAML flow resolution. Flow is defined in loadpropsflowyamltest
   */
  @Test
  public void testYamlFilePropertyResolution() throws Exception {
    this.testUtil = new FlowRunnerTestUtil(FLOW_YAML_DIR, this.temporaryFolder);
    final Project project = this.testUtil.getProject();
    when(this.testUtil.getProjectLoader().isFlowFileUploaded(project.getId(), project.getVersion()))
        .thenReturn(true);
    when(this.testUtil.getProjectLoader()
        .getLatestFlowVersion(project.getId(), project.getVersion(), FLOW_YAML_FILE)).thenReturn(1);
    when(this.testUtil.getProjectLoader()
        .getUploadedFlowFile(eq(project.getId()), eq(project.getVersion()), eq(FLOW_YAML_FILE),
            eq(1), any(File.class)))
        .thenReturn(ExecutionsTestUtil.getFlowFile(FLOW_YAML_DIR, FLOW_YAML_FILE));
    assertProperties(true);
  }

  /**
   * Helper method to test the flow property resolution.
   */
  private void assertProperties(final boolean isAzkabanFlowVersion20) throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    flowProps.put("props7", "flow7");
    flowProps.put("props6", "flow6");
    flowProps.put("props5", "flow5");
    // enable overriding also for existing job props
    final FlowRunner runner = this.testUtil.createFromFlowMap(FLOW_NAME, null, flowProps,
        Props.of(ConfigurationKeys.EXECUTOR_PROPS_RESOLVE_OVERRIDE_EXISTING_ENABLED, "true"));
    final Map<String, ExecutableNode> nodeMap = new HashMap<>();
    createNodeMap(runner.getExecutableFlow(), nodeMap);
    final ExecutableFlow flow = runner.getExecutableFlow();

    // 1. Start flow. Job 2 should start
    FlowRunnerTestUtil.startThread(runner);
    assertStatus(flow, "job2", Status.RUNNING);

    // Job 2 is a normal job.
    // Only the flow overrides and the shared properties matter
    final Props job2Props = nodeMap.get("job2").getInputProps();
    Assert.assertEquals("shared1", job2Props.get("props1"));
    Assert.assertEquals("job2", job2Props.get("props2"));
    Assert.assertEquals("moo3", job2Props.get("props3"));
    Assert.assertEquals("flow7", job2Props.get("props7"));
    Assert.assertEquals("flow5", job2Props.get("props5"));
    Assert.assertEquals("flow6", job2Props.get("props6"));
    Assert.assertEquals("shared4", job2Props.get("props4"));
    Assert.assertEquals("shared8", job2Props.get("props8"));

    // Job 1 is inside another flow, and is nested in a different directory
    // The priority order should be:
    // job1->innerflow->job2.output->job1.sharedprops->flow.overrides
    final Props job2Generated = new Props();
    job2Generated.put("props6", "gjob6");
    job2Generated.put("props9", "gjob9");
    job2Generated.put("props10", "gjob10");
    InteractiveTestJob.getTestJob("job2").succeedJob(job2Generated);
    assertStatus(flow, "innerflow:job1", Status.RUNNING);

    final Props job1Props = nodeMap.get("innerflow:job1").getInputProps();
    Assert.assertEquals("job1", job1Props.get("props1"));
    Assert.assertEquals("job2", job1Props.get("props2"));
    Assert.assertEquals("job8", job1Props.get("props8"));
    Assert.assertEquals("gjob9", job1Props.get("props9"));
    Assert.assertEquals("gjob10", job1Props.get("props10"));
    Assert.assertEquals("flow6", job1Props.get("props6"));
    Assert.assertEquals("flow5", job1Props.get("props5"));
    Assert.assertEquals("flow7", job1Props.get("props7"));
    Assert.assertEquals("moo3", job1Props.get("props3"));
    Assert.assertEquals("moo4", job1Props.get("props4"));

    // Job 4 is inside another flow and takes output from job 1
    // The priority order should be:
    // job4->job1.output->innerflow->job2.output->job4.sharedprops->flow.overrides
    final Props job1GeneratedProps = new Props();
    job1GeneratedProps.put("props9", "g2job9");
    job1GeneratedProps.put("props7", "g2job7");
    InteractiveTestJob.getTestJob("innerflow:job1").succeedJob(
        job1GeneratedProps);
    assertStatus(flow, "innerflow:job4", Status.RUNNING);
    final Props job4Props = nodeMap.get("innerflow:job4").getInputProps();
    Assert.assertEquals("job8", job4Props.get("props8"));
    Assert.assertEquals("job9", job4Props.get("props9"));
    Assert.assertEquals("flow7", job4Props.get("props7"));
    Assert.assertEquals("flow5", job4Props.get("props5"));
    Assert.assertEquals("flow6", job4Props.get("props6"));
    Assert.assertEquals("gjob10", job4Props.get("props10"));
    Assert.assertEquals("shared4", job4Props.get("props4"));
    Assert.assertEquals("shared1", job4Props.get("props1"));
    Assert.assertEquals("shared2", job4Props.get("props2"));
    Assert.assertEquals("moo3", job4Props.get("props3"));

    // Job 3 is a normal job taking props from an embedded flow
    // The priority order should be:
    // job3->innerflow.output->job3.sharedprops->flow.overrides
    final Props job4GeneratedProps = new Props();
    job4GeneratedProps.put("props9", "g4job9");
    job4GeneratedProps.put("props6", "g4job6");
    InteractiveTestJob.getTestJob("innerflow:job4").succeedJob(
        job4GeneratedProps);
    assertStatus(flow, FLOW_NAME, Status.RUNNING);
    final Props job3Props = nodeMap.get("job3").getInputProps();
    Assert.assertEquals("job3", job3Props.get("props3"));
    Assert.assertEquals("flow6", job3Props.get("props6"));
    Assert.assertEquals("g4job9", job3Props.get("props9"));
    Assert.assertEquals("flow7", job3Props.get("props7"));
    Assert.assertEquals("flow5", job3Props.get("props5"));
    Assert.assertEquals("shared1", job3Props.get("props1"));
    Assert.assertEquals("shared2", job3Props.get("props2"));
    Assert.assertEquals("moo4", job3Props.get("props4"));
  }

  private void createNodeMap(final ExecutableFlowBase flow,
      final Map<String, ExecutableNode> nodeMap) {
    for (final ExecutableNode node : flow.getExecutableNodes()) {
      nodeMap.put(node.getNestedId(), node);

      if (node instanceof ExecutableFlowBase) {
        createNodeMap((ExecutableFlowBase) node, nodeMap);
      }
    }
  }
}
