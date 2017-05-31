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

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.executor.ExecutableFlow;
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
import azkaban.project.MockProjectLoader;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class FlowRunnerTest {

  private File workingDir;
  private JobTypeManager jobtypeManager;
  private ProjectLoader fakeProjectLoader;

  public FlowRunnerTest() {

  }

  @Before
  public void setUp() throws Exception {
    System.out.println("Create temp dir");
    synchronized (this) {
      this.workingDir = new File("_AzkabanTestDir_" + System.currentTimeMillis());
      if (this.workingDir.exists()) {
        FileUtils.deleteDirectory(this.workingDir);
      }
      this.workingDir.mkdirs();
    }
    this.jobtypeManager =
        new JobTypeManager(null, null, this.getClass().getClassLoader());
    final JobTypePluginSet pluginSet = this.jobtypeManager.getJobTypePluginSet();
    pluginSet.addPluginClass("java", JavaJob.class);
    pluginSet.addPluginClass("test", InteractiveTestJob.class);
    this.fakeProjectLoader = new MockProjectLoader(this.workingDir);

    InteractiveTestJob.clearTestJobs();
  }

  @After
  public void tearDown() throws IOException {
    System.out.println("Teardown temp dir");
    synchronized (this) {
      if (this.workingDir != null) {
        FileUtils.deleteDirectory(this.workingDir);
        this.workingDir = null;
      }
    }
  }

  @Ignore
  @Test
  public void exec1Normal() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();
    // just making compile. may not work at all.

    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final FlowRunner runner = createFlowRunner(loader, eventCollector, "exec1");

    Assert.assertTrue(!runner.isKilled());
    runner.run();
    final ExecutableFlow exFlow = runner.getExecutableFlow();
    Assert.assertTrue(exFlow.getStatus() == Status.SUCCEEDED);
    compareFinishedRuntime(runner);

    testStatus(exFlow, "job1", Status.SUCCEEDED);
    testStatus(exFlow, "job2", Status.SUCCEEDED);
    testStatus(exFlow, "job3", Status.SUCCEEDED);
    testStatus(exFlow, "job4", Status.SUCCEEDED);
    testStatus(exFlow, "job5", Status.SUCCEEDED);
    testStatus(exFlow, "job6", Status.SUCCEEDED);
    testStatus(exFlow, "job7", Status.SUCCEEDED);
    testStatus(exFlow, "job8", Status.SUCCEEDED);
    testStatus(exFlow, "job10", Status.SUCCEEDED);

    try {
      eventCollector.checkEventExists(new Type[]{Type.FLOW_STARTED,
          Type.FLOW_FINISHED});
    } catch (final Exception e) {
      System.out.println(e.getMessage());

      Assert.fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void exec1Disabled() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final File testDir = new File("unit/executions/exectest1");
    ExecutableFlow exFlow = prepareExecDir(testDir, "exec1", 1);

    // Disable couple in the middle and at the end.
    exFlow.getExecutableNode("job1").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job6").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job5").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job10").setStatus(Status.DISABLED);

    final FlowRunner runner = createFlowRunner(exFlow, loader, eventCollector);

    Assert.assertTrue(!runner.isKilled());
    Assert.assertTrue(exFlow.getStatus() == Status.READY);
    runner.run();

    exFlow = runner.getExecutableFlow();
    compareFinishedRuntime(runner);

    Assert.assertTrue(exFlow.getStatus() == Status.SUCCEEDED);

    testStatus(exFlow, "job1", Status.SKIPPED);
    testStatus(exFlow, "job2", Status.SUCCEEDED);
    testStatus(exFlow, "job3", Status.SUCCEEDED);
    testStatus(exFlow, "job4", Status.SUCCEEDED);
    testStatus(exFlow, "job5", Status.SKIPPED);
    testStatus(exFlow, "job6", Status.SKIPPED);
    testStatus(exFlow, "job7", Status.SUCCEEDED);
    testStatus(exFlow, "job8", Status.SUCCEEDED);
    testStatus(exFlow, "job10", Status.SKIPPED);

    try {
      eventCollector.checkEventExists(new Type[]{Type.FLOW_STARTED,
          Type.FLOW_FINISHED});
    } catch (final Exception e) {
      System.out.println(e.getMessage());

      Assert.fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void exec1Failed() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final File testDir = new File("unit/executions/exectest1");
    final ExecutableFlow flow = prepareExecDir(testDir, "exec2", 1);

    final FlowRunner runner = createFlowRunner(flow, loader, eventCollector);

    runner.run();
    final ExecutableFlow exFlow = runner.getExecutableFlow();
    Assert.assertTrue(!runner.isKilled());
    Assert.assertTrue("Flow status " + exFlow.getStatus(),
        exFlow.getStatus() == Status.FAILED);

    testStatus(exFlow, "job1", Status.SUCCEEDED);
    testStatus(exFlow, "job2d", Status.FAILED);
    testStatus(exFlow, "job3", Status.CANCELLED);
    testStatus(exFlow, "job4", Status.CANCELLED);
    testStatus(exFlow, "job5", Status.CANCELLED);
    testStatus(exFlow, "job6", Status.SUCCEEDED);
    testStatus(exFlow, "job7", Status.CANCELLED);
    testStatus(exFlow, "job8", Status.CANCELLED);
    testStatus(exFlow, "job9", Status.CANCELLED);
    testStatus(exFlow, "job10", Status.CANCELLED);

    try {
      eventCollector.checkEventExists(new Type[]{Type.FLOW_STARTED,
          Type.FLOW_FINISHED});
    } catch (final Exception e) {
      System.out.println(e.getMessage());

      Assert.fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void exec1FailedKillAll() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final File testDir = new File("unit/executions/exectest1");
    final ExecutableFlow flow = prepareExecDir(testDir, "exec2", 1);
    flow.getExecutionOptions().setFailureAction(FailureAction.CANCEL_ALL);

    final FlowRunner runner = createFlowRunner(flow, loader, eventCollector);

    runner.run();
    final ExecutableFlow exFlow = runner.getExecutableFlow();

    Assert.assertTrue(runner.isKilled());

    Assert.assertTrue(
        "Expected flow " + Status.FAILED + " instead " + exFlow.getStatus(),
        exFlow.getStatus() == Status.FAILED);

    try {
      Thread.sleep(500);
    } catch (final InterruptedException e) {
    }

    testStatus(exFlow, "job1", Status.SUCCEEDED);
    testStatus(exFlow, "job2d", Status.FAILED);
    testStatus(exFlow, "job3", Status.CANCELLED);
    testStatus(exFlow, "job4", Status.CANCELLED);
    testStatus(exFlow, "job5", Status.CANCELLED);
    testStatus(exFlow, "job6", Status.KILLED);
    testStatus(exFlow, "job7", Status.CANCELLED);
    testStatus(exFlow, "job8", Status.CANCELLED);
    testStatus(exFlow, "job9", Status.CANCELLED);
    testStatus(exFlow, "job10", Status.CANCELLED);

    try {
      eventCollector.checkEventExists(new Type[]{Type.FLOW_STARTED,
          Type.FLOW_FINISHED});
    } catch (final Exception e) {
      System.out.println(e.getMessage());
      eventCollector.writeAllEvents();
      Assert.fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void exec1FailedFinishRest() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final File testDir = new File("unit/executions/exectest1");
    final ExecutableFlow flow = prepareExecDir(testDir, "exec3", 1);
    flow.getExecutionOptions().setFailureAction(
        FailureAction.FINISH_ALL_POSSIBLE);
    final FlowRunner runner = createFlowRunner(flow, loader, eventCollector);

    runner.run();
    final ExecutableFlow exFlow = runner.getExecutableFlow();
    Assert.assertTrue(
        "Expected flow " + Status.FAILED + " instead " + exFlow.getStatus(),
        exFlow.getStatus() == Status.FAILED);

    try {
      Thread.sleep(500);
    } catch (final InterruptedException e) {
    }

    testStatus(exFlow, "job1", Status.SUCCEEDED);
    testStatus(exFlow, "job2d", Status.FAILED);
    testStatus(exFlow, "job3", Status.SUCCEEDED);
    testStatus(exFlow, "job4", Status.CANCELLED);
    testStatus(exFlow, "job5", Status.CANCELLED);
    testStatus(exFlow, "job6", Status.CANCELLED);
    testStatus(exFlow, "job7", Status.SUCCEEDED);
    testStatus(exFlow, "job8", Status.SUCCEEDED);
    testStatus(exFlow, "job9", Status.SUCCEEDED);
    testStatus(exFlow, "job10", Status.CANCELLED);

    try {
      eventCollector.checkEventExists(new Type[]{Type.FLOW_STARTED,
          Type.FLOW_FINISHED});
    } catch (final Exception e) {
      System.out.println(e.getMessage());
      eventCollector.writeAllEvents();
      Assert.fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void execAndCancel() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final FlowRunner runner = createFlowRunner(loader, eventCollector, "exec1");

    Assert.assertTrue(!runner.isKilled());
    final Thread thread = new Thread(runner);
    thread.start();

    try {
      Thread.sleep(5000);
    } catch (final InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    runner.kill("me");
    Assert.assertTrue(runner.isKilled());

    try {
      Thread.sleep(2000);
    } catch (final InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    final ExecutableFlow exFlow = runner.getExecutableFlow();
    testStatus(exFlow, "job1", Status.SUCCEEDED);
    testStatus(exFlow, "job2", Status.SUCCEEDED);
    testStatus(exFlow, "job5", Status.CANCELLED);
    testStatus(exFlow, "job7", Status.CANCELLED);
    testStatus(exFlow, "job8", Status.CANCELLED);
    testStatus(exFlow, "job10", Status.CANCELLED);
    testStatus(exFlow, "job3", Status.KILLED);
    testStatus(exFlow, "job4", Status.KILLED);
    testStatus(exFlow, "job6", Status.KILLED);

    Assert.assertTrue(
        "Expected FAILED status instead got " + exFlow.getStatus(),
        exFlow.getStatus() == Status.KILLED);

    try {
      eventCollector.checkEventExists(new Type[]{Type.FLOW_STARTED,
          Type.FLOW_FINISHED});
    } catch (final Exception e) {
      System.out.println(e.getMessage());
      eventCollector.writeAllEvents();
      Assert.fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void execRetries() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final FlowRunner runner = createFlowRunner(loader, eventCollector, "exec4-retry");

    runner.run();

    final ExecutableFlow exFlow = runner.getExecutableFlow();
    testStatus(exFlow, "job-retry", Status.SUCCEEDED);
    testStatus(exFlow, "job-pass", Status.SUCCEEDED);
    testStatus(exFlow, "job-retry-fail", Status.FAILED);
    testAttempts(exFlow, "job-retry", 3);
    testAttempts(exFlow, "job-pass", 0);
    testAttempts(exFlow, "job-retry-fail", 2);

    Assert.assertTrue(
        "Expected FAILED status instead got " + exFlow.getStatus(),
        exFlow.getStatus() == Status.FAILED);
  }

  private void testStatus(final ExecutableFlow flow, final String name, final Status status) {
    final ExecutableNode node = flow.getExecutableNode(name);

    if (node.getStatus() != status) {
      Assert.fail("Status of job " + node.getId() + " is " + node.getStatus()
          + " not " + status + " as expected.");
    }
  }

  private void testAttempts(final ExecutableFlow flow, final String name, final int attempt) {
    final ExecutableNode node = flow.getExecutableNode(name);

    if (node.getAttempt() != attempt) {
      Assert.fail("Expected " + attempt + " got " + node.getAttempt()
          + " attempts " + name);
    }
  }

  private ExecutableFlow prepareExecDir(final File execDir, final String flowName,
      final int execId) throws IOException {
    synchronized (this) {
      FileUtils.copyDirectory(execDir, this.workingDir);
    }

    final File jsonFlowFile = new File(this.workingDir, flowName + ".flow");
    final HashMap<String, Object> flowObj =
        (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);

    final Project project = new Project(1, "myproject");
    project.setVersion(2);

    final Flow flow = Flow.flowFromObject(flowObj);
    final ExecutableFlow execFlow = new ExecutableFlow(project, flow);
    execFlow.setExecutionId(execId);
    execFlow.setExecutionPath(this.workingDir.getPath());
    return execFlow;
  }

  private void compareFinishedRuntime(final FlowRunner runner) throws Exception {
    final ExecutableFlow flow = runner.getExecutableFlow();
    for (final String flowName : flow.getStartNodes()) {
      final ExecutableNode node = flow.getExecutableNode(flowName);
      compareStartFinishTimes(flow, node, 0);
    }
  }

  private void compareStartFinishTimes(final ExecutableFlow flow,
      final ExecutableNode node, final long previousEndTime) throws Exception {
    final long startTime = node.getStartTime();
    final long endTime = node.getEndTime();

    // If start time is < 0, so will the endtime.
    if (startTime <= 0) {
      Assert.assertTrue(endTime <= 0);
      return;
    }

    // System.out.println("Node " + node.getJobId() + " start:" + startTime +
    // " end:" + endTime + " previous:" + previousEndTime);
    Assert.assertTrue("Checking start and end times", startTime > 0
        && endTime >= startTime);
    Assert.assertTrue("Start time for " + node.getId() + " is " + startTime
        + " and less than " + previousEndTime, startTime >= previousEndTime);

    for (final String outNode : node.getOutNodes()) {
      final ExecutableNode childNode = flow.getExecutableNode(outNode);
      compareStartFinishTimes(flow, childNode, endTime);
    }
  }

  private FlowRunner createFlowRunner(final ExecutableFlow flow,
      final ExecutorLoader loader, final EventCollectorListener eventCollector) throws Exception {
    return createFlowRunner(flow, loader, eventCollector, new Props());
  }

  private FlowRunner createFlowRunner(final ExecutableFlow flow,
      final ExecutorLoader loader, final EventCollectorListener eventCollector,
      final Props azkabanProps)
      throws Exception {
    // File testDir = new File("unit/executions/exectest1");
    // MockProjectLoader projectLoader = new MockProjectLoader(new
    // File(flow.getExecutionPath()));

    loader.uploadExecutableFlow(flow);
    final FlowRunner runner =
        new FlowRunner(flow, loader, this.fakeProjectLoader, this.jobtypeManager, azkabanProps);

    runner.addListener(eventCollector);

    return runner;
  }

  private FlowRunner createFlowRunner(final ExecutorLoader loader,
      final EventCollectorListener eventCollector, final String flowName) throws Exception {
    return createFlowRunner(loader, eventCollector, flowName, new Props());
  }

  private FlowRunner createFlowRunner(final ExecutorLoader loader,
      final EventCollectorListener eventCollector, final String flowName, final Props azkabanProps)
      throws Exception {
    final File testDir = new File("unit/executions/exectest1");
    final ExecutableFlow exFlow = prepareExecDir(testDir, flowName, 1);
    // MockProjectLoader projectLoader = new MockProjectLoader(new
    // File(exFlow.getExecutionPath()));

    loader.uploadExecutableFlow(exFlow);

    final FlowRunner runner =
        new FlowRunner(exFlow, loader, this.fakeProjectLoader, this.jobtypeManager, azkabanProps);

    runner.addListener(eventCollector);

    return runner;
  }
}
