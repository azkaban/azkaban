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
import com.google.common.collect.ImmutableMap;
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
 * 1. Global props (set in the FlowRunner) 2. Shared job props (depends on job directory) 3. Flow
 * Override properties 4. Previous job outputs to the embedded flow (Only if contained in embedded
 * flow) 5. Embedded flow properties (Only if contained in embedded flow) 6. Previous job outputs
 * (if exists) 7. Job Props
 * <p>
 * The test contains the following structure: job2 -> innerFlow (job1 -> job4 ) -> job3
 * <p>
 * job2 and 4 are in nested directories so should have different shared properties than other jobs.
 */
public class FlowRunnerPropertyResolutionTest extends FlowRunnerTestBase {

  private static final String EXEC_FLOW_DIR = "execpropstest";
  private static final String FLOW_YAML_DIR = "loadpropsflowyamltest";
  private static final String FLOW_NAME = "job3";
  private static final String FLOW_YAML_FILE = FlowRunnerPropertyResolutionTest.FLOW_NAME + ".flow";
  private FlowRunnerTestUtil testUtil;

  @Test
  public void testFlow1_0RuntimePropertyResolution() throws Exception {
    this.testUtil = new FlowRunnerTestUtil(FlowRunnerPropertyResolutionTest.EXEC_FLOW_DIR,
        this.temporaryFolder);
    assertProperties();
  }

  @Test
  public void testFlow1_0RuntimePropertyResolutionWithHighestPrecedenceToRuntimePropsEnabled()
      throws Exception {
    this.testUtil = new FlowRunnerTestUtil(FlowRunnerPropertyResolutionTest.EXEC_FLOW_DIR,
        this.temporaryFolder);
    assertPropertiesWithHighestPrecedenceToRuntimePropsEnabled();
  }

  @Test
  public void testFlow2_0RuntimePropertyResolution() throws Exception {
    this.testUtil = new FlowRunnerTestUtil(FlowRunnerPropertyResolutionTest.FLOW_YAML_DIR,
        this.temporaryFolder);
    final Project project = this.testUtil.getProject();
    when(this.testUtil.getProjectLoader().isFlowFileUploaded(project.getId(), project.getVersion()))
        .thenReturn(true);
    when(this.testUtil.getProjectLoader()
        .getLatestFlowVersion(project.getId(), project.getVersion(),
            FlowRunnerPropertyResolutionTest.FLOW_YAML_FILE)).thenReturn(1);
    when(this.testUtil.getProjectLoader()
        .getUploadedFlowFile(eq(project.getId()), eq(project.getVersion()), eq(
            FlowRunnerPropertyResolutionTest.FLOW_YAML_FILE),
            eq(1), any(File.class)))
        .thenReturn(ExecutionsTestUtil.getFlowFile(FlowRunnerPropertyResolutionTest.FLOW_YAML_DIR,
            FlowRunnerPropertyResolutionTest.FLOW_YAML_FILE));
    assertProperties();
  }

  @Test
  public void testFlow2_0RuntimePropertyResolutionWithHighestPrecedenceToRuntimePropsEnabled()
      throws Exception {
    this.testUtil = new FlowRunnerTestUtil(FlowRunnerPropertyResolutionTest.FLOW_YAML_DIR,
        this.temporaryFolder);
    final Project project = this.testUtil.getProject();
    when(this.testUtil.getProjectLoader().isFlowFileUploaded(project.getId(), project.getVersion()))
        .thenReturn(true);
    when(this.testUtil.getProjectLoader()
        .getLatestFlowVersion(project.getId(), project.getVersion(),
            FlowRunnerPropertyResolutionTest.FLOW_YAML_FILE)).thenReturn(1);
    when(this.testUtil.getProjectLoader()
        .getUploadedFlowFile(eq(project.getId()), eq(project.getVersion()), eq(
            FlowRunnerPropertyResolutionTest.FLOW_YAML_FILE),
            eq(1), any(File.class)))
        .thenReturn(ExecutionsTestUtil.getFlowFile(FlowRunnerPropertyResolutionTest.FLOW_YAML_DIR,
            FlowRunnerPropertyResolutionTest.FLOW_YAML_FILE));
    assertPropertiesWithHighestPrecedenceToRuntimePropsEnabled();
  }

  private void assertProperties() throws Exception {
    final HashMap<String, String> rootFlowNodeRuntimeProps = new HashMap<>();
    rootFlowNodeRuntimeProps.put("props7", "execflow7");
    rootFlowNodeRuntimeProps.put("props6", "execflow6");
    rootFlowNodeRuntimeProps.put("props5", "execflow5");
    rootFlowNodeRuntimeProps.put("runtime1", "runtime1-ROOT");
    rootFlowNodeRuntimeProps.put("runtime2", "runtime2-ROOT");

    // Set some node (root flow + other DAG nodes) runtime properties.
    final FlowRunner runner = this.testUtil.createFromFlowMap(
        FlowRunnerPropertyResolutionTest.FLOW_NAME, rootFlowNodeRuntimeProps);
    runner.getExecutableFlow().getExecutionOptions().addAllRuntimeProperties(ImmutableMap.of(
        "job2", ImmutableMap.of(
            "job-prop-2", "job2-val-2",
            "props6", "job2-val-6"),
        "innerflow", ImmutableMap.of(
            "props6", "innerflow-val-6",
            "props4", "innerflow-val-4",
            "props10", "innerflow-val-10"),
        // overrides by nested job id (or fully qualified name): this is the most specific, so
        // always wins
        "innerflow:job4", ImmutableMap.of(
            "runtime1", "runtime1-job4",
            "props4", "innerflow-job4-val-4",
            "props5", "innerflow-job4-val-5"),
        // job3 is a job, but it's also the root node of this flow
        "job3", ImmutableMap.of("prop-job3", "should-be-set-only-for-job3")
    ));
    final Map<String, ExecutableNode> nodeMap = new HashMap<>();
    createNodeMap(runner.getExecutableFlow(), nodeMap);
    final ExecutableFlow flow = runner.getExecutableFlow();

    // Start flow. Job 2 should start
    FlowRunnerTestUtil.startThread(runner);
    assertStatus(flow, "job2", Status.RUNNING);

    // The priority order should be:
    // job2-overrides -> job2 -> root-flow-node-overrides -> flow-or-shared-props
    final Props job2Props = nodeMap.get("job2").getInputProps();
    Assert.assertEquals("shared1", job2Props.get("props1"));
    Assert.assertEquals("job2", job2Props.get("props2"));
    Assert.assertEquals("moo3", job2Props.get("props3"));
    Assert.assertEquals("job7", job2Props.get("props7"));
    Assert.assertEquals("execflow5", job2Props.get("props5"));
    Assert.assertEquals("job2-val-6", job2Props.get("props6"));
    Assert.assertEquals("shared4", job2Props.get("props4"));
    Assert.assertEquals("shared8", job2Props.get("props8"));
    Assert.assertEquals("job2-val-2", job2Props.get("job-prop-2"));
    Assert.assertEquals("runtime1-ROOT", job2Props.get("runtime1"));
    Assert.assertEquals("runtime2-ROOT", job2Props.get("runtime2"));
    Assert.assertNull(job2Props.get("props10"));

    // The priority order should be:
    // job1-overrides -> job1 -> innerflow-overrides -> innerflow -> job2-output ->
    // root-flow-node-overrides -> flow-or-shared-props
    final Props job2Generated = new Props();
    job2Generated.put("props6", "g2job6");
    job2Generated.put("props4", "g2job4");
    job2Generated.put("props5", "g2job5");
    job2Generated.put("props7", "g2job7");
    job2Generated.put("props10", "g2job10");
    InteractiveTestJob.getTestJob("job2").succeedJob(job2Generated);
    assertStatus(flow, "innerflow:job1", Status.RUNNING);
    final Props job1Props = nodeMap.get("innerflow:job1").getInputProps();
    Assert.assertEquals("job1", job1Props.get("props1"));
    Assert.assertEquals("job2", job1Props.get("props2"));
    Assert.assertEquals("job8", job1Props.get("props8"));
    Assert.assertEquals("innerflow-val-6", job1Props.get("props6"));
    Assert.assertEquals("innerflow5", job1Props.get("props5"));
    Assert.assertEquals("g2job7", job1Props.get("props7"));
    Assert.assertEquals("moo3", job1Props.get("props3"));
    Assert.assertEquals("innerflow-val-4", job1Props.get("props4"));
    Assert.assertEquals("innerflow-val-10", job1Props.get("props10"));
    Assert.assertEquals("runtime1-ROOT", job1Props.get("runtime1"));
    Assert.assertEquals("runtime2-ROOT", job1Props.get("runtime2"));

    // The priority order should be:
    // job4-overrides -> job4 -> job1-output -> innerflow-overrides -> innerflow ->
    // job2-output -> root-flow-node-overrides -> flow-or-shared-props
    final Props job1GeneratedProps = new Props();
    job1GeneratedProps.put("props4", "g1job4");
    job1GeneratedProps.put("props10", "g1job10");
    InteractiveTestJob.getTestJob("innerflow:job1").succeedJob(job1GeneratedProps);
    assertStatus(flow, "innerflow:job4", Status.RUNNING);
    final Props job4Props = nodeMap.get("innerflow:job4").getInputProps();
    Assert.assertEquals("job8", job4Props.get("props8"));
    Assert.assertEquals("job9", job4Props.get("props9"));
    Assert.assertEquals("innerflow-job4-val-4", job4Props.get("props4"));
    Assert.assertEquals("g1job10", job4Props.get("props10"));
    Assert.assertEquals("innerflow-val-6", job4Props.get("props6"));
    Assert.assertEquals("g2job7", job4Props.get("props7"));
    Assert.assertEquals("innerflow-job4-val-5", job4Props.get("props5"));
    Assert.assertEquals("shared1", job4Props.get("props1"));
    Assert.assertEquals("shared2", job4Props.get("props2"));
    Assert.assertEquals("moo3", job4Props.get("props3"));
    Assert.assertEquals("runtime1-job4", job4Props.get("runtime1"));
    Assert.assertEquals("runtime2-ROOT", job4Props.get("runtime2"));

    // The priority order should be:
    // job3-overrides -> job3 -> innerflow-output -> root-flow-node-overrides -> flow-or-shared-props
    final Props job4GeneratedProps = new Props();
    job4GeneratedProps.put("props9", "g4job9");
    job4GeneratedProps.put("props6", "g4job6");
    InteractiveTestJob.getTestJob("innerflow:job4").succeedJob(job4GeneratedProps);
    assertStatus(flow, FlowRunnerPropertyResolutionTest.FLOW_NAME, Status.RUNNING);
    final Props job3Props = nodeMap.get("job3").getInputProps();
    Assert.assertEquals("job3", job3Props.get("props3"));
    Assert.assertEquals("g4job6", job3Props.get("props6"));
    Assert.assertEquals("g4job9", job3Props.get("props9"));
    Assert.assertEquals("execflow7", job3Props.get("props7"));
    Assert.assertEquals("execflow5", job3Props.get("props5"));
    Assert.assertEquals("shared1", job3Props.get("props1"));
    Assert.assertEquals("shared2", job3Props.get("props2"));
    Assert.assertEquals("moo4", job3Props.get("props4"));
    Assert.assertNull(job3Props.get("props10"));
    Assert.assertEquals("runtime1-ROOT", job3Props.get("runtime1"));
    Assert.assertEquals("runtime2-ROOT", job3Props.get("runtime2"));

    Assert.assertEquals("should-be-set-only-for-job3", job3Props.get("prop-job3"));
    Assert.assertNull(job2Props.get("prop-job3"));
    Assert.assertNull(job4Props.get("prop-job3"));
  }

  private void assertPropertiesWithHighestPrecedenceToRuntimePropsEnabled() throws Exception {
    final HashMap<String, String> rootFlowNodeRuntimeProps = new HashMap<>();
    rootFlowNodeRuntimeProps.put("props7", "execflow7");
    rootFlowNodeRuntimeProps.put("props6", "execflow6");
    rootFlowNodeRuntimeProps.put("props5", "execflow5");

    final FlowRunner runner = this.testUtil.createFromFlowMap(
        FlowRunnerPropertyResolutionTest.FLOW_NAME, null, rootFlowNodeRuntimeProps,
        Props.of(ConfigurationKeys.EXECUTOR_PROPS_RESOLVE_OVERRIDE_EXISTING_ENABLED, "true"));

    // Set some node (root flow + other DAG nodes) runtime properties.
    runner.getExecutableFlow().getExecutionOptions().addAllRuntimeProperties(ImmutableMap.of(
        "job2", ImmutableMap.of(
            "job-prop-2", "job2-val-2",
            "props6", "job2-val-6"),
        "innerflow", ImmutableMap.of(
            "props6", "innerflow-val-6",
            "props4", "innerflow-val-4",
            "props10", "innerflow-val-10"),
        // overrides by nested job id: this is the most specific, so always wins
        "innerflow:job4", ImmutableMap.of(
            "props4", "innerflow-job4-val-4",
            "props5", "innerflow-job4-val-5"),
        // job3 is a job, but it's also the root node of this flow
        "job3", ImmutableMap.of("prop-job3", "should-be-set-only-for-job3")
    ));
    final Map<String, ExecutableNode> nodeMap = new HashMap<>();
    createNodeMap(runner.getExecutableFlow(), nodeMap);
    final ExecutableFlow flow = runner.getExecutableFlow();

    // Start flow. Job 2 should start
    FlowRunnerTestUtil.startThread(runner);
    assertStatus(flow, "job2", Status.RUNNING);

    // The priority order should be:
    // job2-overrides -> root-flow-node-overrides -> job2 -> flow-or-shared-props
    final Props job2Props = nodeMap.get("job2").getInputProps();
    Assert.assertEquals("shared1", job2Props.get("props1"));
    Assert.assertEquals("job2", job2Props.get("props2"));
    Assert.assertEquals("moo3", job2Props.get("props3"));
    Assert.assertEquals("execflow7", job2Props.get("props7"));
    Assert.assertEquals("execflow5", job2Props.get("props5"));
    Assert.assertEquals("job2-val-6", job2Props.get("props6"));
    Assert.assertEquals("shared4", job2Props.get("props4"));
    Assert.assertEquals("shared8", job2Props.get("props8"));
    Assert.assertEquals("job2-val-2", job2Props.get("job-prop-2"));
    Assert.assertNull(job2Props.get("props10"));

    // The priority order should be:
    // job1-overrides -> innerflow-overrides -> root-flow-node-overrides -> job1 -> innerflow ->
    // job2-output -> flow-or-shared-props
    final Props job2Generated = new Props();
    job2Generated.put("props6", "g2job6");
    job2Generated.put("props8", "g2job8");
    job2Generated.put("props10", "g2job10");
    InteractiveTestJob.getTestJob("job2").succeedJob(job2Generated);
    assertStatus(flow, "innerflow:job1", Status.RUNNING);
    final Props job1Props = nodeMap.get("innerflow:job1").getInputProps();
    Assert.assertEquals("job1", job1Props.get("props1"));
    Assert.assertEquals("job2", job1Props.get("props2"));
    Assert.assertEquals("job8", job1Props.get("props8"));
    Assert.assertEquals("innerflow-val-6", job1Props.get("props6"));
    Assert.assertEquals("innerflow-val-10", job1Props.get("props10"));
    Assert.assertEquals("innerflow-val-4", job1Props.get("props4"));
    Assert.assertEquals("execflow5", job1Props.get("props5"));
    Assert.assertEquals("execflow7", job1Props.get("props7"));
    Assert.assertEquals("moo3", job1Props.get("props3"));

    // The priority order should be:
    // job4-overrides -> innerflow-overrides -> root-flow-node-overrides -> job4 ->
    // job1-output -> innerflow -> job2-output -> flow-or-shared-props
    final Props job1GeneratedProps = new Props();
    job1GeneratedProps.put("props10", "g1job10");
    job1GeneratedProps.put("props1", "g1job1");
    InteractiveTestJob.getTestJob("innerflow:job1").succeedJob(job1GeneratedProps);
    assertStatus(flow, "innerflow:job4", Status.RUNNING);
    final Props job4Props = nodeMap.get("innerflow:job4").getInputProps();
    Assert.assertEquals("job8", job4Props.get("props8"));
    Assert.assertEquals("job9", job4Props.get("props9"));
    Assert.assertEquals("innerflow-val-6", job4Props.get("props6"));
    Assert.assertEquals("innerflow-val-10", job4Props.get("props10"));
    Assert.assertEquals("execflow7", job4Props.get("props7"));
    Assert.assertEquals("innerflow-job4-val-4", job4Props.get("props4"));
    Assert.assertEquals("innerflow-job4-val-5", job4Props.get("props5"));
    Assert.assertEquals("g1job1", job4Props.get("props1"));
    Assert.assertEquals("shared2", job4Props.get("props2"));
    Assert.assertEquals("moo3", job4Props.get("props3"));

    // The priority order should be:
    // job3-overrides -> root-flow-node-overrides -> job3 -> innerflow-output -> flow-or-shared-props
    final Props job4GeneratedProps = new Props();
    job4GeneratedProps.put("props9", "g4job9");
    job4GeneratedProps.put("props6", "g4job6");
    InteractiveTestJob.getTestJob("innerflow:job4").succeedJob(job4GeneratedProps);
    assertStatus(flow, FlowRunnerPropertyResolutionTest.FLOW_NAME, Status.RUNNING);
    final Props job3Props = nodeMap.get("job3").getInputProps();
    Assert.assertEquals("job3", job3Props.get("props3"));
    Assert.assertEquals("execflow6", job3Props.get("props6"));
    Assert.assertEquals("g4job9", job3Props.get("props9"));
    Assert.assertEquals("execflow7", job3Props.get("props7"));
    Assert.assertEquals("execflow5", job3Props.get("props5"));
    Assert.assertEquals("shared1", job3Props.get("props1"));
    Assert.assertEquals("shared2", job3Props.get("props2"));
    Assert.assertEquals("moo4", job3Props.get("props4"));
    Assert.assertNull(job3Props.get("props10"));

    Assert.assertEquals("should-be-set-only-for-job3", job3Props.get("prop-job3"));
    Assert.assertNull(job2Props.get("prop-job3"));
    Assert.assertNull(job4Props.get("prop-job3"));
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
