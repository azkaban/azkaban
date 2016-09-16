/*
 * Copyright 2014 LinkedIn Corp.
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.JavaJob;
import azkaban.executor.MockExecutorLoader;
import azkaban.flow.Flow;
import azkaban.jobtype.JobTypeManager;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.project.MockProjectLoader;
import azkaban.utils.Props;

/**
 * Test the property resolution of jobs in a flow.
 *
 * The tests are contained in execpropstest, and should be resolved in the
 * following fashion, where the later props take precedence over the previous
 * ones.
 *
 * 1. Global props (set in the FlowRunner)
 * 2. Shared job props (depends on job directory)
 * 3. Flow Override properties
 * 4. Previous job outputs to the embedded flow (Only if contained in embedded flow)
 * 5. Embedded flow properties (Only if contained in embedded flow)
 * 6. Previous job outputs (if exists)
 * 7. Job Props
 *
 * The test contains the following structure:
 * job2 -> innerFlow (job1 -> job4 ) -> job3
 *
 * job2 and 4 are in nested directories so should have different shared
 * properties than other jobs.
 */
public class FlowRunnerPropertyResolutionTest {
  private File workingDir;
  private JobTypeManager jobtypeManager;
  private ProjectLoader fakeProjectLoader;
  private ExecutorLoader fakeExecutorLoader;
  private Logger logger = Logger.getLogger(FlowRunnerTest2.class);
  private Project project;
  private Map<String, Flow> flowMap;
  private static int id = 101;

  @Before
  public void setUp() throws Exception {
    System.out.println("Create temp dir");
    workingDir = new File("_AzkabanTestDir_" + System.currentTimeMillis());
    if (workingDir.exists()) {
      FileUtils.deleteDirectory(workingDir);
    }
    workingDir.mkdirs();
    jobtypeManager =
        new JobTypeManager(null, null, this.getClass().getClassLoader());
    jobtypeManager.getJobTypePluginSet().addPluginClass("java", JavaJob.class);
    jobtypeManager.getJobTypePluginSet().addPluginClass("test",
        InteractiveTestJob.class);
    fakeProjectLoader = new MockProjectLoader(workingDir);
    fakeExecutorLoader = new MockExecutorLoader();
    project = new Project(1, "testProject");

    File dir = new File("unit/executions/execpropstest");
    prepareProject(project, dir);

    InteractiveTestJob.clearTestJobs();
  }

  @After
  public void tearDown() throws IOException {
    System.out.println("Teardown temp dir");
    if (workingDir != null) {
      FileUtils.deleteDirectory(workingDir);
      workingDir = null;
    }
  }

  /**
   * Tests the basic flow resolution. Flow is defined in execpropstest
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testPropertyResolution() throws Exception {
    HashMap<String, String> flowProps = new HashMap<String, String>();
    flowProps.put("props7", "flow7");
    flowProps.put("props6", "flow6");
    flowProps.put("props5", "flow5");
    FlowRunner runner = createFlowRunner("job3", flowProps);
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();
    createNodeMap(runner.getExecutableFlow(), nodeMap);

    // 1. Start flow. Job 2 should start
    runFlowRunnerInThread(runner);
    pause(250);

    // Job 2 is a normal job.
    // Only the flow overrides and the shared properties matter
    ExecutableNode node = nodeMap.get("job2");
    Props job2Props = node.getInputProps();
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
    Props job2Generated = new Props();
    job2Generated.put("props6", "gjob6");
    job2Generated.put("props9", "gjob9");
    job2Generated.put("props10", "gjob10");
    InteractiveTestJob.getTestJob("job2").succeedJob(job2Generated);
    pause(250);
    node = nodeMap.get("innerflow:job1");
    Props job1Props = node.getInputProps();
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
    Props job1GeneratedProps = new Props();
    job1GeneratedProps.put("props9", "g2job9");
    job1GeneratedProps.put("props7", "g2job7");
    InteractiveTestJob.getTestJob("innerflow:job1").succeedJob(
        job1GeneratedProps);
    pause(250);
    node = nodeMap.get("innerflow:job4");
    Props job4Props = node.getInputProps();
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
    Props job4GeneratedProps = new Props();
    job4GeneratedProps.put("props9", "g4job9");
    job4GeneratedProps.put("props6", "g4job6");
    InteractiveTestJob.getTestJob("innerflow:job4").succeedJob(
        job4GeneratedProps);
    pause(250);
    node = nodeMap.get("job3");
    Props job3Props = node.getInputProps();
    Assert.assertEquals("job3", job3Props.get("props3"));
    Assert.assertEquals("g4job6", job3Props.get("props6"));
    Assert.assertEquals("g4job9", job3Props.get("props9"));
    Assert.assertEquals("flow7", job3Props.get("props7"));
    Assert.assertEquals("flow5", job3Props.get("props5"));
    Assert.assertEquals("shared1", job3Props.get("props1"));
    Assert.assertEquals("shared2", job3Props.get("props2"));
    Assert.assertEquals("moo4", job3Props.get("props4"));
  }

  private void prepareProject(Project project, File directory) throws ProjectManagerException,
      IOException {
    DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);
    loader.loadProjectFlow(project, directory);
    if (!loader.getErrors().isEmpty()) {
      for (String error : loader.getErrors()) {
        System.out.println(error);
      }

      throw new RuntimeException("Errors found in setup");
    }

    flowMap = loader.getFlowMap();
    project.setFlows(flowMap);
    FileUtils.copyDirectory(directory, workingDir);
  }

  private FlowRunner createFlowRunner(String flowName,
      HashMap<String, String> flowParams) throws Exception {
    Flow flow = flowMap.get(flowName);

    int exId = id++;
    ExecutableFlow exFlow = new ExecutableFlow(project, flow);
    exFlow.setExecutionPath(workingDir.getPath());
    exFlow.setExecutionId(exId);

    exFlow.getExecutionOptions().addAllFlowParameters(flowParams);
    fakeExecutorLoader.uploadExecutableFlow(exFlow);

    FlowRunner runner =
        new FlowRunner(fakeExecutorLoader.fetchExecutableFlow(exId),
            fakeExecutorLoader, fakeProjectLoader, jobtypeManager);
    return runner;
  }

  private void createNodeMap(ExecutableFlowBase flow,
      Map<String, ExecutableNode> nodeMap) {
    for (ExecutableNode node : flow.getExecutableNodes()) {
      nodeMap.put(node.getNestedId(), node);

      if (node instanceof ExecutableFlowBase) {
        createNodeMap((ExecutableFlowBase) node, nodeMap);
      }
    }
  }

  private Thread runFlowRunnerInThread(FlowRunner runner) {
    Thread thread = new Thread(runner);
    thread.start();
    return thread;
  }

  private void pause(long millisec) {
    synchronized (this) {
      try {
        wait(millisec);
      } catch (InterruptedException e) {
      }
    }
  }
}
