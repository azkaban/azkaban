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

import static org.mockito.Mockito.mock;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.JavaJob;
import azkaban.executor.MockExecutorLoader;
import azkaban.flow.Flow;
import azkaban.jobtype.JobTypeManager;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.spi.AzkabanEventReporter;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the property resolution of jobs in a flow.
 *
 * The tests are contained in execpropstest, and should be resolved in the following fashion, where
 * the later props take precedence over the previous ones.
 *
 * 1. Global props (set in the FlowRunner) 2. Shared job props (depends on job directory) 3. Flow
 * Override properties 4. Previous job outputs to the embedded flow (Only if contained in embedded
 * flow) 5. Embedded flow properties (Only if contained in embedded flow) 6. Previous job outputs
 * (if exists) 7. Job Props
 *
 * The test contains the following structure: job2 -> innerFlow (job1 -> job4 ) -> job3
 *
 * job2 and 4 are in nested directories so should have different shared properties than other jobs.
 */
public class FlowRunnerPropertyResolutionTest {

  private static int id = 101;
  private final AzkabanEventReporter azkabanEventReporter =
      EventReporterUtil.getTestAzkabanEventReporter();
  private File workingDir;
  private JobTypeManager jobtypeManager;
  private ExecutorLoader fakeExecutorLoader;
  private Project project;
  private Map<String, Flow> flowMap;

  @Before
  public void setUp() throws Exception {
    System.out.println("Create temp dir");
    this.workingDir = new File("_AzkabanTestDir_" + System.currentTimeMillis());
    if (this.workingDir.exists()) {
      FileUtils.deleteDirectory(this.workingDir);
    }
    this.workingDir.mkdirs();
    this.jobtypeManager =
        new JobTypeManager(null, null, this.getClass().getClassLoader());
    this.jobtypeManager.getJobTypePluginSet().addPluginClass("java", JavaJob.class);
    this.jobtypeManager.getJobTypePluginSet().addPluginClass("test",
        InteractiveTestJob.class);
    this.fakeExecutorLoader = new MockExecutorLoader();
    this.project = new Project(1, "testProject");

    final File dir = new File("unit/executions/execpropstest");
    this.flowMap = FlowRunnerTestUtil
        .prepareProject(this.project, dir, this.workingDir);

    InteractiveTestJob.clearTestJobs();
  }

  @After
  public void tearDown() throws IOException {
    System.out.println("Teardown temp dir");
    if (this.workingDir != null) {
      FileUtils.deleteDirectory(this.workingDir);
      this.workingDir = null;
    }
  }

  /**
   * Tests the basic flow resolution. Flow is defined in execpropstest
   */
  @Ignore
  @Test
  public void testPropertyResolution() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    flowProps.put("props7", "flow7");
    flowProps.put("props6", "flow6");
    flowProps.put("props5", "flow5");
    final FlowRunner runner = createFlowRunner("job3", flowProps);
    final Map<String, ExecutableNode> nodeMap = new HashMap<>();
    createNodeMap(runner.getExecutableFlow(), nodeMap);

    // 1. Start flow. Job 2 should start
    runFlowRunnerInThread(runner);
    pause(250);

    // Job 2 is a normal job.
    // Only the flow overrides and the shared properties matter
    ExecutableNode node = nodeMap.get("job2");
    final Props job2Props = node.getInputProps();
    Assert.assertEquals("shared1", job2Props.get("props1"));
    Assert.assertEquals("job2", job2Props.get("props2"));
    Assert.assertEquals("moo3", job2Props.get("props3"));
    Assert.assertEquals("job7", job2Props.get("props7"));
    Assert.assertEquals("flow5", job2Props.get("props5"));
    Assert.assertEquals("flow6", job2Props.get("props6"));
    Assert.assertEquals("shared4", job2Props.get("props4"));
    Assert.assertEquals("shared8", job2Props.get("props8"));

    // Job 1 is inside another flow, and is nested in a different directory
    // The priority order should be:
    // job1->innerflow->job2.output->flow.overrides->job1 shared props
    final Props job2Generated = new Props();
    job2Generated.put("props6", "gjob6");
    job2Generated.put("props9", "gjob9");
    job2Generated.put("props10", "gjob10");
    InteractiveTestJob.getTestJob("job2").succeedJob(job2Generated);
    pause(250);
    node = nodeMap.get("innerflow:job1");
    final Props job1Props = node.getInputProps();
    Assert.assertEquals("job1", job1Props.get("props1"));
    Assert.assertEquals("job2", job1Props.get("props2"));
    Assert.assertEquals("job8", job1Props.get("props8"));
    Assert.assertEquals("gjob9", job1Props.get("props9"));
    Assert.assertEquals("gjob10", job1Props.get("props10"));
    Assert.assertEquals("innerflow6", job1Props.get("props6"));
    Assert.assertEquals("innerflow5", job1Props.get("props5"));
    Assert.assertEquals("flow7", job1Props.get("props7"));
    Assert.assertEquals("moo3", job1Props.get("props3"));
    Assert.assertEquals("moo4", job1Props.get("props4"));

    // Job 4 is inside another flow and takes output from job 1
    // The priority order should be:
    // job4->job1.output->innerflow->job2.output->flow.overrides->job4 shared
    // props
    final Props job1GeneratedProps = new Props();
    job1GeneratedProps.put("props9", "g2job9");
    job1GeneratedProps.put("props7", "g2job7");
    InteractiveTestJob.getTestJob("innerflow:job1").succeedJob(
        job1GeneratedProps);
    pause(250);
    node = nodeMap.get("innerflow:job4");
    final Props job4Props = node.getInputProps();
    Assert.assertEquals("job8", job4Props.get("props8"));
    Assert.assertEquals("job9", job4Props.get("props9"));
    Assert.assertEquals("g2job7", job4Props.get("props7"));
    Assert.assertEquals("innerflow5", job4Props.get("props5"));
    Assert.assertEquals("innerflow6", job4Props.get("props6"));
    Assert.assertEquals("gjob10", job4Props.get("props10"));
    Assert.assertEquals("shared4", job4Props.get("props4"));
    Assert.assertEquals("shared1", job4Props.get("props1"));
    Assert.assertEquals("shared2", job4Props.get("props2"));
    Assert.assertEquals("moo3", job4Props.get("props3"));

    // Job 3 is a normal job taking props from an embedded flow
    // The priority order should be:
    // job3->innerflow.output->flow.overrides->job3.sharedprops
    final Props job4GeneratedProps = new Props();
    job4GeneratedProps.put("props9", "g4job9");
    job4GeneratedProps.put("props6", "g4job6");
    InteractiveTestJob.getTestJob("innerflow:job4").succeedJob(
        job4GeneratedProps);
    pause(250);
    node = nodeMap.get("job3");
    final Props job3Props = node.getInputProps();
    Assert.assertEquals("job3", job3Props.get("props3"));
    Assert.assertEquals("g4job6", job3Props.get("props6"));
    Assert.assertEquals("g4job9", job3Props.get("props9"));
    Assert.assertEquals("flow7", job3Props.get("props7"));
    Assert.assertEquals("flow5", job3Props.get("props5"));
    Assert.assertEquals("shared1", job3Props.get("props1"));
    Assert.assertEquals("shared2", job3Props.get("props2"));
    Assert.assertEquals("moo4", job3Props.get("props4"));
  }

  private FlowRunner createFlowRunner(final String flowName,
      final HashMap<String, String> flowParams) throws Exception {
    return createFlowRunner(flowName, flowParams, new Props());
  }

  private FlowRunner createFlowRunner(final String flowName,
      final HashMap<String, String> flowParams, final Props azkabanProps) throws Exception {
    final Flow flow = this.flowMap.get(flowName);

    final int exId = id++;
    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    exFlow.setExecutionPath(this.workingDir.getPath());
    exFlow.setExecutionId(exId);

    exFlow.getExecutionOptions().addAllFlowParameters(flowParams);
    this.fakeExecutorLoader.uploadExecutableFlow(exFlow);

    final FlowRunner runner =
        new FlowRunner(this.fakeExecutorLoader.fetchExecutableFlow(exId),
            this.fakeExecutorLoader, mock(ProjectLoader.class), this.jobtypeManager, azkabanProps,
            this.azkabanEventReporter);
    return runner;
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

  private Thread runFlowRunnerInThread(final FlowRunner runner) {
    final Thread thread = new Thread(runner);
    thread.start();
    return thread;
  }

  private void pause(final long millisec) {
    try {
      Thread.sleep(millisec);
    } catch (final InterruptedException e) {
    }
  }
}
