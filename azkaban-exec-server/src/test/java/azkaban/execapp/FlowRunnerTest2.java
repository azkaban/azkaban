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

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.JavaJob;
import azkaban.executor.MockExecutorLoader;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.jobtype.JobTypeManager;
import azkaban.jobtype.JobTypePluginSet;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.project.MockProjectLoader;
import azkaban.utils.Props;

/**
 * Test the flow run, especially with embedded flows.
 *
 * This test uses executions/embedded2. It also mainly uses the flow named
 * jobf. The test is designed to control success/failures explicitly so we
 * don't have to time the flow exactly.
 *
 * Flow jobf looks like the following:
 *
 *
 *       joba       joba1
 *      /  |  \      |
 *     /   |   \     |
 *  jobb  jobd jobc  |
 *     \   |   /    /
 *      \  |  /    /
 *        jobe    /
 *         |     /
 *         |    /
 *        jobf
 *
 *  The job 'jobb' is an embedded flow:
 *
 *  jobb:innerFlow
 *
 *        innerJobA
 *        /       \
 *   innerJobB   innerJobC
 *        \       /
 *        innerFlow
 *
 *
 *  The job 'jobd' is a simple embedded flow:
 *
 *  jobd:innerFlow2
 *
 *       innerJobA
 *           |
 *       innerFlow2
 *
 *  The following tests checks each stage of the flow run by forcing jobs to
 *  succeed or fail.
 */
public class FlowRunnerTest2 {
  private File workingDir;
  private JobTypeManager jobtypeManager;
  private ProjectLoader fakeProjectLoader;
  private ExecutorLoader fakeExecutorLoader;
  private Logger logger = Logger.getLogger(FlowRunnerTest2.class);
  private Project project;
  private Map<String, Flow> flowMap;
  private static int id=101;

  public FlowRunnerTest2() {
  }

  @Before
  public void setUp() throws Exception {
    System.out.println("Create temp dir");
    workingDir = new File("_AzkabanTestDir_" + System.currentTimeMillis());
    if (workingDir.exists()) {
      FileUtils.deleteDirectory(workingDir);
    }
    workingDir.mkdirs();
    jobtypeManager = new JobTypeManager(null, null,
        this.getClass().getClassLoader());
    JobTypePluginSet pluginSet = jobtypeManager.getJobTypePluginSet();

    pluginSet.addPluginClass("java", JavaJob.class);
    pluginSet.addPluginClass("test", InteractiveTestJob.class);
    fakeProjectLoader = new MockProjectLoader(workingDir);
    fakeExecutorLoader = new MockExecutorLoader();
    project = new Project(1, "testProject");

    File dir = new File("unit/executions/embedded2");
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
   * Tests the basic successful flow run, and also tests all output variables
   * from each job.
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testBasicRun() throws Exception {
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");

    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    ExecutableFlow flow = runner.getExecutableFlow();
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);

    compareStates(expectedStateMap, nodeMap);
    Props joba = nodeMap.get("joba").getInputProps();
    Assert.assertEquals("joba.1", joba.get("param1"));
    Assert.assertEquals("test1.2", joba.get("param2"));
    Assert.assertEquals("test1.3", joba.get("param3"));
    Assert.assertEquals("override.4", joba.get("param4"));
    Assert.assertEquals("test2.5", joba.get("param5"));
    Assert.assertEquals("test2.6", joba.get("param6"));
    Assert.assertEquals("test2.7", joba.get("param7"));
    Assert.assertEquals("test2.8", joba.get("param8"));

    Props joba1 = nodeMap.get("joba1").getInputProps();
    Assert.assertEquals("test1.1", joba1.get("param1"));
    Assert.assertEquals("test1.2", joba1.get("param2"));
    Assert.assertEquals("test1.3", joba1.get("param3"));
    Assert.assertEquals("override.4", joba1.get("param4"));
    Assert.assertEquals("test2.5", joba1.get("param5"));
    Assert.assertEquals("test2.6", joba1.get("param6"));
    Assert.assertEquals("test2.7", joba1.get("param7"));
    Assert.assertEquals("test2.8", joba1.get("param8"));

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob(
        Props.of("output.joba", "joba", "output.override", "joba"));
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    ExecutableNode node = nodeMap.get("jobb");
    Assert.assertEquals(Status.RUNNING, node.getStatus());
    Props jobb = node.getInputProps();
    Assert.assertEquals("override.4", jobb.get("param4"));
    // Test that jobb properties overwrites the output properties
    Assert.assertEquals("moo", jobb.get("testprops"));
    Assert.assertEquals("jobb", jobb.get("output.override"));
    Assert.assertEquals("joba", jobb.get("output.joba"));

    Props jobbInnerJobA = nodeMap.get("jobb:innerJobA").getInputProps();
    Assert.assertEquals("test1.1", jobbInnerJobA.get("param1"));
    Assert.assertEquals("test1.2", jobbInnerJobA.get("param2"));
    Assert.assertEquals("test1.3", jobbInnerJobA.get("param3"));
    Assert.assertEquals("override.4", jobbInnerJobA.get("param4"));
    Assert.assertEquals("test2.5", jobbInnerJobA.get("param5"));
    Assert.assertEquals("test2.6", jobbInnerJobA.get("param6"));
    Assert.assertEquals("test2.7", jobbInnerJobA.get("param7"));
    Assert.assertEquals("test2.8", jobbInnerJobA.get("param8"));
    Assert.assertEquals("joba", jobbInnerJobA.get("output.joba"));

    // 3. jobb:Inner completes
    /// innerJobA completes
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob(
        Props.of("output.jobb.innerJobA", "jobb.innerJobA"));
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);
    Props jobbInnerJobB = nodeMap.get("jobb:innerJobB").getInputProps();
    Assert.assertEquals("test1.1", jobbInnerJobB.get("param1"));
    Assert.assertEquals("override.4", jobbInnerJobB.get("param4"));
    Assert.assertEquals("jobb.innerJobA",
        jobbInnerJobB.get("output.jobb.innerJobA"));
    Assert.assertEquals("moo", jobbInnerJobB.get("testprops"));
    /// innerJobB, C completes
    InteractiveTestJob.getTestJob("jobb:innerJobB").succeedJob(
        Props.of("output.jobb.innerJobB", "jobb.innerJobB"));
    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob(
        Props.of("output.jobb.innerJobC", "jobb.innerJobC"));
    pause(250);
    expectedStateMap.put("jobb:innerJobB", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobC", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerFlow", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    Props jobbInnerJobD = nodeMap.get("jobb:innerFlow").getInputProps();
    Assert.assertEquals("test1.1", jobbInnerJobD.get("param1"));
    Assert.assertEquals("override.4", jobbInnerJobD.get("param4"));
    Assert.assertEquals("jobb.innerJobB",
        jobbInnerJobD.get("output.jobb.innerJobB"));
    Assert.assertEquals("jobb.innerJobC",
        jobbInnerJobD.get("output.jobb.innerJobC"));

    // 4. Finish up on inner flow for jobb
    InteractiveTestJob.getTestJob("jobb:innerFlow").succeedJob(
        Props.of("output1.jobb", "test1", "output2.jobb", "test2"));
    pause(250);
    expectedStateMap.put("jobb:innerFlow", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);
    Props jobbOutput = nodeMap.get("jobb").getOutputProps();
    Assert.assertEquals("test1", jobbOutput.get("output1.jobb"));
    Assert.assertEquals("test2", jobbOutput.get("output2.jobb"));

    // 5. Finish jobc, jobd
    InteractiveTestJob.getTestJob("jobc").succeedJob(
        Props.of("output.jobc", "jobc"));
    pause(250);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    pause(250);
    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    pause(250);
    expectedStateMap.put("jobd:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobd:innerFlow2", Status.SUCCEEDED);
    expectedStateMap.put("jobd", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    Props jobd = nodeMap.get("jobe").getInputProps();
    Assert.assertEquals("test1", jobd.get("output1.jobb"));
    Assert.assertEquals("jobc", jobd.get("output.jobc"));

    // 6. Finish off flow
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    pause(250);
    InteractiveTestJob.getTestJob("jobe").succeedJob();
    pause(250);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.SUCCEEDED);
    expectedStateMap.put("jobf", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobf").succeedJob();
    pause(250);
    expectedStateMap.put("jobf", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());

    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests a flow with Disabled jobs and flows. They should properly SKIP
   * executions
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testDisabledNormal() throws Exception {
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();
    flow.getExecutableNode("jobb").setStatus(Status.DISABLED);
    ((ExecutableFlowBase)flow.getExecutableNode("jobd")).getExecutableNode(
        "innerJobA").setStatus(Status.DISABLED);

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB A COMPLETES SUCCESSFULLY, others should be skipped
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.SKIPPED);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.SKIPPED);
    expectedStateMap.put("jobd:innerFlow2", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.READY);
    expectedStateMap.put("jobb:innerJobB", Status.READY);
    expectedStateMap.put("jobb:innerJobC", Status.READY);
    expectedStateMap.put("jobb:innerFlow", Status.READY);
    compareStates(expectedStateMap, nodeMap);

    // 3. jobb:Inner completes
    /// innerJobA completes
    InteractiveTestJob.getTestJob("jobc").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    pause(250);
    expectedStateMap.put("jobd:innerFlow2", Status.SUCCEEDED);
    expectedStateMap.put("jobd", Status.SUCCEEDED);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobe").succeedJob();
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    pause(250);
    expectedStateMap.put("jobe", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobf", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 4. Finish up on inner flow for jobb
    InteractiveTestJob.getTestJob("jobf").succeedJob();
    pause(250);
    expectedStateMap.put("jobf", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);

    Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests a failure with the default FINISH_CURRENTLY_RUNNING.
   * After the first failure, every job that started should complete, and the
   * rest of the jobs should be skipped.
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testNormalFailure1() throws Exception {
    // Test propagation of KILLED status to embedded flows.
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB A COMPLETES SUCCESSFULLY, others should be skipped
    InteractiveTestJob.getTestJob("joba").failJob();
    pause(250);
    Assert.assertEquals(Status.FAILED_FINISHING, flow.getStatus());
    expectedStateMap.put("joba", Status.FAILED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.CANCELLED);
    expectedStateMap.put("jobc", Status.CANCELLED);
    expectedStateMap.put("jobd", Status.CANCELLED);
    expectedStateMap.put("jobd:innerJobA", Status.READY);
    expectedStateMap.put("jobd:innerFlow2", Status.READY);
    expectedStateMap.put("jobb:innerJobA", Status.READY);
    expectedStateMap.put("jobb:innerFlow", Status.READY);
    expectedStateMap.put("jobe", Status.CANCELLED);
    compareStates(expectedStateMap, nodeMap);

    // 3. jobb:Inner completes
    /// innerJobA completes
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    pause(250);
    expectedStateMap.put("jobf", Status.CANCELLED);
    Assert.assertEquals(Status.FAILED, flow.getStatus());
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Test #2 on the default failure case.
   * @throws Exception
   */
  @Ignore @Test
  public void testNormalFailure2() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB A COMPLETES SUCCESSFULLY, others should be skipped
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("joba1").failJob();
    pause(250);
    expectedStateMap.put("joba1", Status.FAILED);
    compareStates(expectedStateMap, nodeMap);

    // 3. joba completes, everything is killed
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobd:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.CANCELLED);
    expectedStateMap.put("jobb:innerJobC", Status.CANCELLED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobb", Status.KILLED);
    expectedStateMap.put("jobd", Status.KILLED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.FAILED_FINISHING, flow.getStatus());

    InteractiveTestJob.getTestJob("jobc").succeedJob();
    pause(250);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.FAILED, flow.getStatus());

    Assert.assertFalse(thread.isAlive());
  }

  @Ignore @Test
  public void testNormalFailure3() throws Exception {
    // Test propagation of CANCELLED status to embedded flows different branch
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    pause(250);
    expectedStateMap.put("jobb", Status.FAILED_FINISHING);
    expectedStateMap.put("jobb:innerJobB", Status.FAILED);
    Assert.assertEquals(Status.FAILED_FINISHING, flow.getStatus());
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobd:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobd", Status.KILLED);
    expectedStateMap.put("jobb:innerJobC", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    expectedStateMap.put("jobb", Status.FAILED);
    compareStates(expectedStateMap, nodeMap);

    // 3. jobc completes, everything is killed
    InteractiveTestJob.getTestJob("jobc").succeedJob();
    pause(250);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.FAILED, flow.getStatus());

    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests failures when the fail behaviour is FINISH_ALL_POSSIBLE.
   * In this case, all jobs which have had its pre-requisite met can continue
   * to run. Finishes when the failure is propagated to the last node of the
   * flow.
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testFailedFinishingFailure3() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf",
        FailureAction.FINISH_ALL_POSSIBLE);
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    pause(250);
    expectedStateMap.put("jobb", Status.FAILED_FINISHING);
    expectedStateMap.put("jobb:innerJobB", Status.FAILED);
    Assert.assertEquals(Status.FAILED_FINISHING, flow.getStatus());
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb", Status.FAILED);
    expectedStateMap.put("jobd:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobd:innerFlow2", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    pause(250);
    expectedStateMap.put("jobd:innerFlow2", Status.SUCCEEDED);
    expectedStateMap.put("jobd", Status.SUCCEEDED);

    // 3. jobc completes, everything is killed
    InteractiveTestJob.getTestJob("jobc").succeedJob();
    pause(250);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.FAILED, flow.getStatus());
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests the failure condition when a failure invokes a cancel (or killed)
   * on the flow.
   *
   * Any jobs that are running will be assigned a KILLED state, and any nodes
   * which were skipped due to prior errors will be given a CANCELLED state.
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testCancelOnFailure() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf",
        FailureAction.CANCEL_ALL);
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    pause(250);
    expectedStateMap.put("jobb", Status.FAILED);
    expectedStateMap.put("jobb:innerJobB", Status.FAILED);
    expectedStateMap.put("jobb:innerJobC", Status.KILLED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    expectedStateMap.put("jobc", Status.KILLED);
    expectedStateMap.put("jobd", Status.KILLED);
    expectedStateMap.put("jobd:innerJobA", Status.KILLED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);
    compareStates(expectedStateMap, nodeMap);

    Assert.assertFalse(thread.isAlive());
    Assert.assertEquals(Status.FAILED, flow.getStatus());

  }

  /**
   * Tests retries after a failure
   * @throws Exception
   */
  @Ignore @Test
  public void testRetryOnFailure() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();
    flow.getExecutableNode("joba").setStatus(Status.DISABLED);
    ((ExecutableFlowBase)flow.getExecutableNode("jobb")).getExecutableNode(
        "innerFlow").setStatus(Status.DISABLED);

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.SKIPPED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    InteractiveTestJob.getTestJob("jobb:innerJobC").failJob();
    pause(250);
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb", Status.FAILED);
    expectedStateMap.put("jobb:innerJobB", Status.FAILED);
    expectedStateMap.put("jobb:innerJobC", Status.FAILED);
    expectedStateMap.put("jobb:innerFlow", Status.SKIPPED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobd:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobd", Status.KILLED);
    Assert.assertEquals(Status.FAILED_FINISHING, flow.getStatus());
    compareStates(expectedStateMap, nodeMap);

    ExecutableNode node = nodeMap.get("jobd:innerFlow2");
    ExecutableFlowBase base = node.getParentFlow();
    for (String nodeId : node.getInNodes()) {
      ExecutableNode inNode = base.getExecutableNode(nodeId);
      System.out.println(inNode.getId() + " > " + inNode.getStatus());
    }

    runner.retryFailures("me");
    pause(500);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobb:innerFlow", Status.DISABLED);
    expectedStateMap.put("jobd:innerFlow2", Status.RUNNING);
    Assert.assertEquals(Status.RUNNING, flow.getStatus());
    compareStates(expectedStateMap, nodeMap);
    Assert.assertTrue(thread.isAlive());


    InteractiveTestJob.getTestJob("jobb:innerJobB").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    InteractiveTestJob.getTestJob("jobc").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerFlow", Status.SKIPPED);
    expectedStateMap.put("jobb", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobC", Status.SUCCEEDED);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    expectedStateMap.put("jobd", Status.SUCCEEDED);
    expectedStateMap.put("jobd:innerFlow2", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobe").succeedJob();
    pause(250);
    expectedStateMap.put("jobe", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    pause(250);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobf", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobf").succeedJob();
    pause(250);
    expectedStateMap.put("jobf", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests the manual Killing of a flow. In this case, the flow is just fine
   * before the cancel
   * is called.
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testCancel() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf",
        FailureAction.CANCEL_ALL);
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(1000);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    runner.kill("me");
    pause(250);

    expectedStateMap.put("jobb", Status.KILLED);
    expectedStateMap.put("jobb:innerJobB", Status.KILLED);
    expectedStateMap.put("jobb:innerJobC", Status.KILLED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    expectedStateMap.put("jobc", Status.KILLED);
    expectedStateMap.put("jobd", Status.KILLED);
    expectedStateMap.put("jobd:innerJobA", Status.KILLED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);

    Assert.assertEquals(Status.KILLED, flow.getStatus());
    compareStates(expectedStateMap, nodeMap);
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests the manual invocation of cancel on a flow that is FAILED_FINISHING
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testManualCancelOnFailure() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");
    ExecutableFlow flow = runner.getExecutableFlow();
    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobB", Status.FAILED);
    expectedStateMap.put("jobb", Status.FAILED_FINISHING);
    Assert.assertEquals(Status.FAILED_FINISHING, flow.getStatus());
    compareStates(expectedStateMap, nodeMap);

    runner.kill("me");
    pause(1000);

    expectedStateMap.put("jobb", Status.FAILED);
    expectedStateMap.put("jobb:innerJobC", Status.KILLED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    expectedStateMap.put("jobc", Status.KILLED);
    expectedStateMap.put("jobd", Status.KILLED);
    expectedStateMap.put("jobd:innerJobA", Status.KILLED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);

    Assert.assertEquals(Status.KILLED, flow.getStatus());
    compareStates(expectedStateMap, nodeMap);
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests that pause and resume work
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testPause() throws Exception {
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");

    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    ExecutableFlow flow = runner.getExecutableFlow();
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    runner.pause("test");
    InteractiveTestJob.getTestJob("joba").succeedJob();
    // 2.1 JOB A COMPLETES SUCCESSFULLY AFTER PAUSE
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(flow.getStatus(), Status.PAUSED);

    // 2.2 Flow is unpaused
    runner.resume("test");
    pause(250);
    Assert.assertEquals(flow.getStatus(), Status.RUNNING);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 3. jobb:Inner completes
    runner.pause("test");

    /// innerJobA completes, but paused
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob(
        Props.of("output.jobb.innerJobA", "jobb.innerJobA"));
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);

    runner.resume("test");
    pause(250);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    /// innerJobB, C completes
    InteractiveTestJob.getTestJob("jobb:innerJobB").succeedJob(
        Props.of("output.jobb.innerJobB", "jobb.innerJobB"));
    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob(
        Props.of("output.jobb.innerJobC", "jobb.innerJobC"));
    pause(250);
    expectedStateMap.put("jobb:innerJobB", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobC", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerFlow", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 4. Finish up on inner flow for jobb
    InteractiveTestJob.getTestJob("jobb:innerFlow").succeedJob(
        Props.of("output1.jobb", "test1", "output2.jobb", "test2"));
    pause(250);
    expectedStateMap.put("jobb:innerFlow", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);

    // 5. Finish jobc, jobd
    InteractiveTestJob.getTestJob("jobc").succeedJob(
        Props.of("output.jobc", "jobc"));
    pause(250);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    pause(250);
    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    pause(250);
    expectedStateMap.put("jobd:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobd:innerFlow2", Status.SUCCEEDED);
    expectedStateMap.put("jobd", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 6. Finish off flow
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    pause(250);
    InteractiveTestJob.getTestJob("jobe").succeedJob();
    pause(250);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.SUCCEEDED);
    expectedStateMap.put("jobf", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    InteractiveTestJob.getTestJob("jobf").succeedJob();
    pause(250);
    expectedStateMap.put("jobf", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());

    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Test the condition for a manual invocation of a KILL (cancel) on a flow
   * that has been paused. The flow should unpause and be killed immediately.
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testPauseKill() throws Exception {
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf");

    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    ExecutableFlow flow = runner.getExecutableFlow();
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    runner.pause("me");
    pause(250);
    Assert.assertEquals(flow.getStatus(), Status.PAUSED);
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    pause(250);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    expectedStateMap.put("jobd:innerJobA", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);

    runner.kill("me");
    pause(250);
    expectedStateMap.put("joba1", Status.KILLED);
    expectedStateMap.put("jobb:innerJobB", Status.CANCELLED);
    expectedStateMap.put("jobb:innerJobC", Status.CANCELLED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    expectedStateMap.put("jobb", Status.KILLED);
    expectedStateMap.put("jobc", Status.KILLED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobd", Status.KILLED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);

    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.KILLED, flow.getStatus());
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests the case where a failure occurs on a Paused flow. In this case, the
   * flow should stay paused.
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testPauseFail() throws Exception {
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf",
        FailureAction.FINISH_CURRENTLY_RUNNING);

    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    ExecutableFlow flow = runner.getExecutableFlow();
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    runner.pause("me");
    pause(250);
    Assert.assertEquals(flow.getStatus(), Status.PAUSED);
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").failJob();
    pause(250);
    expectedStateMap.put("jobd:innerJobA", Status.FAILED);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(flow.getStatus(), Status.PAUSED);

    runner.resume("me");
    pause(250);
    expectedStateMap.put("jobb:innerJobB", Status.CANCELLED);
    expectedStateMap.put("jobb:innerJobC", Status.CANCELLED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    expectedStateMap.put("jobb", Status.KILLED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobd", Status.FAILED);

    InteractiveTestJob.getTestJob("jobc").succeedJob();
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    pause(250);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobf", Status.CANCELLED);
    expectedStateMap.put("jobe", Status.CANCELLED);

    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.FAILED, flow.getStatus());
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Test the condition when a Finish all possible is called during a pause.
   * The Failure is not acted upon until the flow is resumed.
   *
   * @throws Exception
   */
  @Ignore @Test
  public void testPauseFailFinishAll() throws Exception {
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf",
        FailureAction.FINISH_ALL_POSSIBLE);

    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    ExecutableFlow flow = runner.getExecutableFlow();
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);

    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(250);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    runner.pause("me");
    pause(250);
    Assert.assertEquals(flow.getStatus(), Status.PAUSED);
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").failJob();
    pause(250);
    expectedStateMap.put("jobd:innerJobA", Status.FAILED);
    expectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
    compareStates(expectedStateMap, nodeMap);

    runner.resume("me");
    pause(250);
    expectedStateMap.put("jobb:innerJobB", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobC", Status.RUNNING);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobd", Status.FAILED);

    InteractiveTestJob.getTestJob("jobc").succeedJob();
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobB").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob();
    pause(250);
    InteractiveTestJob.getTestJob("jobb:innerFlow").succeedJob();
    pause(250);
    expectedStateMap.put("jobc", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobB", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerJobC", Status.SUCCEEDED);
    expectedStateMap.put("jobb:innerFlow", Status.SUCCEEDED);
    expectedStateMap.put("jobb", Status.SUCCEEDED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);

    compareStates(expectedStateMap, nodeMap);
    Assert.assertEquals(Status.FAILED, flow.getStatus());
    Assert.assertFalse(thread.isAlive());
  }

  /**
   * Tests the case when a flow is paused and a failure causes a kill. The
   * flow should die immediately regardless of the 'paused' status.
   * @throws Exception
   */
  @Ignore @Test
  public void testPauseFailKill() throws Exception {
    EventCollectorListener eventCollector = new EventCollectorListener();
    FlowRunner runner = createFlowRunner(eventCollector, "jobf",
        FailureAction.CANCEL_ALL);

    Map<String, Status> expectedStateMap = new HashMap<String, Status>();
    Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();

    // 1. START FLOW
    ExecutableFlow flow = runner.getExecutableFlow();
    createExpectedStateMap(flow, expectedStateMap, nodeMap);
    Thread thread = runFlowRunnerInThread(runner);
    pause(250);
    // After it starts up, only joba should be running
    expectedStateMap.put("joba", Status.RUNNING);
    expectedStateMap.put("joba1", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob();
    pause(500);
    expectedStateMap.put("joba", Status.SUCCEEDED);
    expectedStateMap.put("joba1", Status.RUNNING);
    expectedStateMap.put("jobb", Status.RUNNING);
    expectedStateMap.put("jobc", Status.RUNNING);
    expectedStateMap.put("jobd", Status.RUNNING);
    expectedStateMap.put("jobd:innerJobA", Status.RUNNING);
    expectedStateMap.put("jobb:innerJobA", Status.RUNNING);
    compareStates(expectedStateMap, nodeMap);

    runner.pause("me");
    pause(250);
    Assert.assertEquals(flow.getStatus(), Status.PAUSED);
    InteractiveTestJob.getTestJob("jobd:innerJobA").failJob();
    pause(250);
    expectedStateMap.put("jobd:innerJobA", Status.FAILED);
    expectedStateMap.put("jobd:innerFlow2", Status.CANCELLED);
    expectedStateMap.put("jobd", Status.FAILED);
    expectedStateMap.put("jobb:innerJobA", Status.KILLED);
    expectedStateMap.put("jobb:innerJobB", Status.CANCELLED);
    expectedStateMap.put("jobb:innerJobC", Status.CANCELLED);
    expectedStateMap.put("jobb:innerFlow", Status.CANCELLED);
    expectedStateMap.put("jobb", Status.KILLED);
    expectedStateMap.put("jobc", Status.KILLED);
    expectedStateMap.put("jobe", Status.CANCELLED);
    expectedStateMap.put("jobf", Status.CANCELLED);
    expectedStateMap.put("joba1", Status.KILLED);
    compareStates(expectedStateMap, nodeMap);

    Assert.assertEquals(Status.FAILED, flow.getStatus());
    Assert.assertFalse(thread.isAlive());
  }


  private Thread runFlowRunnerInThread(FlowRunner runner) {
    Thread thread = new Thread(runner);
    thread.start();
    return thread;
  }

  private void pause(long millisec) {
    synchronized(this) {
      try {
        wait(millisec);
      }
      catch (InterruptedException e) {
      }
    }
  }

  private void createExpectedStateMap(ExecutableFlowBase flow,
      Map<String, Status> expectedStateMap,
      Map<String, ExecutableNode> nodeMap) {
    for (ExecutableNode node: flow.getExecutableNodes()) {
      expectedStateMap.put(node.getNestedId(), node.getStatus());
      nodeMap.put(node.getNestedId(), node);
      if (node instanceof ExecutableFlowBase) {
        createExpectedStateMap((ExecutableFlowBase)node, expectedStateMap,
            nodeMap);
      }
    }
  }

  private void compareStates(Map<String, Status> expectedStateMap,
      Map<String, ExecutableNode> nodeMap) {
    for (String printedId: expectedStateMap.keySet()) {
      Status expectedStatus = expectedStateMap.get(printedId);
      ExecutableNode node = nodeMap.get(printedId);

      if (expectedStatus != node.getStatus()) {
        Assert.fail("Expected values do not match for " + printedId
            + ". Expected " + expectedStatus + ", instead received "
            + node.getStatus());
      }
    }
  }

  private void prepareProject(Project project, File directory)
      throws ProjectManagerException, IOException {
    DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);
    loader.loadProjectFlow(project, directory);
    if (!loader.getErrors().isEmpty()) {
      for (String error: loader.getErrors()) {
        System.out.println(error);
      }

      throw new RuntimeException("Errors found in setup");
    }

    flowMap = loader.getFlowMap();
    project.setFlows(flowMap);
    FileUtils.copyDirectory(directory, workingDir);
  }

  private FlowRunner createFlowRunner(EventCollectorListener eventCollector,
      String flowName) throws Exception {
    return createFlowRunner(eventCollector, flowName,
        FailureAction.FINISH_CURRENTLY_RUNNING);
  }

  private FlowRunner createFlowRunner(EventCollectorListener eventCollector,
      String flowName, FailureAction action) throws Exception {
    Flow flow = flowMap.get(flowName);

    int exId = id++;
    ExecutableFlow exFlow = new ExecutableFlow(project, flow);
    exFlow.setExecutionPath(workingDir.getPath());
    exFlow.setExecutionId(exId);

    Map<String, String> flowParam = new HashMap<String, String>();
    flowParam.put("param4", "override.4");
    flowParam.put("param10", "override.10");
    flowParam.put("param11", "override.11");
    exFlow.getExecutionOptions().addAllFlowParameters(flowParam);
    exFlow.getExecutionOptions().setFailureAction(action);
    fakeExecutorLoader.uploadExecutableFlow(exFlow);

    FlowRunner runner = new FlowRunner(
        fakeExecutorLoader.fetchExecutableFlow(exId), fakeExecutorLoader,
        fakeProjectLoader, jobtypeManager);

    runner.addListener(eventCollector);

    return runner;
  }

}
