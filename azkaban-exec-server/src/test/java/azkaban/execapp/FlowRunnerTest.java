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

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import azkaban.test.Utils;
import org.apache.commons.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.execapp.fake.FakeApp;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.MockExecutorLoader;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.jobExecutor.AllJobExecutorTests;
import azkaban.jobtype.JobTypeManager;
import azkaban.jobtype.JobTypePluginSet;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.MockProjectLoader;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class FlowRunnerTest {
  private File workingDir;
  private JobTypeManager jobtypeManager;
  private ProjectLoader fakeProjectLoader;

  private static final File TEST_DIR = new File("../azkaban-test/src/test/resources/azkaban/test/executions/exectest1");

  public FlowRunnerTest() {

  }

  @Before
  public void setUp() throws Exception {
    System.out.println("Create temp dir");
    synchronized (this) {
      // clear interrupted status
      Thread.interrupted();
      workingDir = new File("build/tmp/_AzkabanTestDir_" + System.currentTimeMillis());
      if (workingDir.exists()) {
        FileUtils.deleteDirectory(workingDir);
      }
      workingDir.mkdirs();
    }
    jobtypeManager =
        new JobTypeManager(null, null, this.getClass().getClassLoader());
    JobTypePluginSet pluginSet = jobtypeManager.getJobTypePluginSet();
    pluginSet.setCommonPluginLoadProps(AllJobExecutorTests.setUpCommonProps());
    pluginSet.addPluginClass("test", InteractiveTestJob.class);
    fakeProjectLoader = new MockProjectLoader(workingDir);
    Utils.initServiceProvider();
    AzkabanExecutorServer.setApp(new FakeApp());

    InteractiveTestJob.clearTestJobs();
  }

  @After
  public void tearDown() throws IOException {
    System.out.println("Teardown temp dir");
    synchronized (this) {
      if (workingDir != null) {
        FileUtils.deleteDirectory(workingDir);
        workingDir = null;
      }
    }
  }

  @Test
  public void exec1Normal() throws Exception {
    MockExecutorLoader loader = new MockExecutorLoader();
    // just making compile. may not work at all.

    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    FlowRunner runner = createFlowRunner(loader, eventCollector, "exec1");

    startThread(runner);

    succeedJobs(runner, "job3", "job4", "job6");

    waitFlowFinished(runner);

    ExecutableFlow exFlow = runner.getExecutableFlow();
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
      eventCollector.checkEventExists(new Type[] { Type.FLOW_STARTED,
          Type.FLOW_FINISHED });
    } catch (Exception e) {
      System.out.println(e.getMessage());

      Assert.fail(e.getMessage());
    }
  }

  private void startThread(FlowRunner runner) {
    Assert.assertTrue(!runner.isKilled());
    Thread thread = new Thread(runner);
    thread.start();
  }

  private void succeedJobs(FlowRunner runner, String... jobs) {
    waitJobsStarted(runner, jobs);
    for (String name : jobs) {
      InteractiveTestJob job;
      for (int i = 0; i < 100; i++) {
        synchronized (InteractiveTestJob.testJobs) {
          job = InteractiveTestJob.getTestJob(name);
          if (job != null) {
            job.succeedJob();
            break;
          }
          try {
            InteractiveTestJob.testJobs.wait(50L);
          } catch (InterruptedException e) {
          }
        }
      }

    }
  }

  @Test
  public void exec1Disabled() throws Exception {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    ExecutableFlow exFlow = prepareExecDir(TEST_DIR, "exec1", 1);

    // Disable couple in the middle and at the end.
    exFlow.getExecutableNode("job1").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job6").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job5").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job10").setStatus(Status.DISABLED);

    FlowRunner runner = createFlowRunner(exFlow, loader, eventCollector);

    Assert.assertTrue(!runner.isKilled());
    Assert.assertTrue(exFlow.getStatus() == Status.READY);
    startThread(runner);

    succeedJobs(runner, "job3", "job4");

    waitFlowFinished(runner);

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
      eventCollector.checkEventExists(new Type[] { Type.FLOW_STARTED,
          Type.FLOW_FINISHED });
    } catch (Exception e) {
      System.out.println(e.getMessage());

      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void exec1Failed() throws Exception {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec2", 1);

    FlowRunner runner = createFlowRunner(flow, loader, eventCollector);
    startThread(runner);

    succeedJobs(runner, "job6");

    waitFlowFinished(runner);

    ExecutableFlow exFlow = runner.getExecutableFlow();
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
      eventCollector.checkEventExists(new Type[] { Type.FLOW_STARTED,
          Type.FLOW_FINISHED });
    } catch (Exception e) {
      System.out.println(e.getMessage());

      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void exec1FailedKillAll() throws Exception {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec2", 1);
    flow.getExecutionOptions().setFailureAction(FailureAction.CANCEL_ALL);

    FlowRunner runner = createFlowRunner(flow, loader, eventCollector);

    runner.run();
    ExecutableFlow exFlow = runner.getExecutableFlow();

    Assert.assertTrue(runner.isKilled());

    Assert.assertTrue(
        "Expected flow " + Status.KILLED + " instead " + exFlow.getStatus(),
        exFlow.getStatus() == Status.KILLED);

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
      eventCollector.checkEventExists(new Type[] { Type.FLOW_STARTED,
          Type.FLOW_FINISHED });
    } catch (Exception e) {
      System.out.println(e.getMessage());
      eventCollector.writeAllEvents();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void exec1FailedFinishRest() throws Exception {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec3", 1);
    flow.getExecutionOptions().setFailureAction(
        FailureAction.FINISH_ALL_POSSIBLE);
    FlowRunner runner = createFlowRunner(flow, loader, eventCollector);

    startThread(runner);

    succeedJobs(runner, "job3");

    waitFlowFinished(runner);

    ExecutableFlow exFlow = runner.getExecutableFlow();
    Assert.assertTrue(
        "Expected flow " + Status.FAILED + " instead " + exFlow.getStatus(),
        exFlow.getStatus() == Status.FAILED);

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
      eventCollector.checkEventExists(new Type[] { Type.FLOW_STARTED,
          Type.FLOW_FINISHED });
    } catch (Exception e) {
      System.out.println(e.getMessage());
      eventCollector.writeAllEvents();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void execAndCancel() throws Exception {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    FlowRunner runner = createFlowRunner(loader, eventCollector, "exec1");

    startThread(runner);

    waitJobsStarted(runner, new String[] {"job1", "job2", "job3", "job4", "job6"});

    runner.kill("me");
    Assert.assertTrue(runner.isKilled());

    waitFlowFinished(runner);

    ExecutableFlow exFlow = runner.getExecutableFlow();
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
      eventCollector.checkEventExists(new Type[] { Type.FLOW_STARTED,
          Type.FLOW_FINISHED });
    } catch (Exception e) {
      System.out.println(e.getMessage());
      eventCollector.writeAllEvents();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void execRetries() throws Exception {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    FlowRunner runner = createFlowRunner(loader, eventCollector, "exec4-retry");

    runner.run();

    ExecutableFlow exFlow = runner.getExecutableFlow();
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

  private void testStatus(ExecutableFlow flow, String name, Status status) {
    ExecutableNode node = flow.getExecutableNode(name);

    if (node.getStatus() != status) {
      Assert.fail("Status of job " + node.getId() + " is " + node.getStatus()
          + " not " + status + " as expected.");
    }
  }

  private void testAttempts(ExecutableFlow flow, String name, int attempt) {
    ExecutableNode node = flow.getExecutableNode(name);

    if (node.getAttempt() != attempt) {
      Assert.fail("Expected " + attempt + " got " + node.getAttempt()
          + " attempts " + name);
    }
  }

  private void waitFlowFinished(FlowRunner runner) {
    for (int i = 0; i < 500; i++) {
      if (runner.getExecutableFlow().isFlowFinished() && !runner.isRunnerThreadAlive()) {
        return;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // ignored
      }
    }
    Assert.fail("Flow didn't finish in 5 seconds");
  }

  private void waitJobsStarted(FlowRunner runner, String[] jobs) {
    for (int i = 0; i < 500; i++) {
      if (checkJobsStarted(runner, jobs)) {
        return;
      }
      try {
        // TODO use some kind of wait instead
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // ignored
      }
    }
    Assert.fail("Jobs didn't start in 5 seconds");
  }

  private boolean checkJobsStarted(FlowRunner runner, String[] jobs) {
    ExecutableFlow exFlow = runner.getExecutableFlow();
    for (String name : jobs) {
      ExecutableNode node = exFlow.getExecutableNode(name);
      if (!Status.isStatusFinished(node.getStatus()) && !Status.isStatusRunning(node.getStatus())) {
        return false;
      }
    }
    return true;
  }

  private ExecutableFlow prepareExecDir(File execDir, String flowName,
      int execId) throws IOException {
    synchronized (this) {
      FileUtils.copyDirectory(execDir, workingDir);
    }

    File jsonFlowFile = new File(workingDir, flowName + ".flow");
    @SuppressWarnings("unchecked")
    HashMap<String, Object> flowObj =
        (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);

    Project project = new Project(1, "myproject");
    project.setVersion(2);

    Flow flow = Flow.flowFromObject(flowObj);
    ExecutableFlow execFlow = new ExecutableFlow(project, flow);
    execFlow.setExecutionId(execId);
    execFlow.setExecutionPath(workingDir.getPath());
    return execFlow;
  }

  private void compareFinishedRuntime(FlowRunner runner) throws Exception {
    ExecutableFlow flow = runner.getExecutableFlow();
    for (String flowName : flow.getStartNodes()) {
      ExecutableNode node = flow.getExecutableNode(flowName);
      compareStartFinishTimes(flow, node, 0);
    }
  }

  private void compareStartFinishTimes(ExecutableFlow flow,
      ExecutableNode node, long previousEndTime) throws Exception {
    long startTime = node.getStartTime();
    long endTime = node.getEndTime();

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

    for (String outNode : node.getOutNodes()) {
      ExecutableNode childNode = flow.getExecutableNode(outNode);
      compareStartFinishTimes(flow, childNode, endTime);
    }
  }

  private FlowRunner createFlowRunner(ExecutableFlow flow,
      ExecutorLoader loader, EventCollectorListener eventCollector) throws Exception {
    return createFlowRunner(flow, loader, eventCollector, new Props());
  }

  private FlowRunner createFlowRunner(ExecutableFlow flow,
      ExecutorLoader loader, EventCollectorListener eventCollector, Props azkabanProps)
      throws Exception {
    // MockProjectLoader projectLoader = new MockProjectLoader(new
    // File(flow.getExecutionPath()));

    loader.uploadExecutableFlow(flow);
    FlowRunner runner =
        new FlowRunner(flow, loader, fakeProjectLoader, jobtypeManager, azkabanProps);

    runner.addListener(eventCollector);

    return runner;
  }

  private FlowRunner createFlowRunner(ExecutorLoader loader,
      EventCollectorListener eventCollector, String flowName) throws Exception {
    return createFlowRunner(loader, eventCollector, flowName, new Props());
  }

  private FlowRunner createFlowRunner(ExecutorLoader loader,
      EventCollectorListener eventCollector, String flowName, Props azkabanProps) throws Exception {
    ExecutableFlow exFlow = prepareExecDir(TEST_DIR, flowName, 1);
    // MockProjectLoader projectLoader = new MockProjectLoader(new
    // File(exFlow.getExecutionPath()));

    loader.uploadExecutableFlow(exFlow);

    FlowRunner runner =
        new FlowRunner(exFlow, loader, fakeProjectLoader, jobtypeManager, azkabanProps);

    runner.addListener(eventCollector);

    return runner;
  }
}
