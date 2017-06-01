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

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

import azkaban.execapp.jmx.JmxJobMBeanManager;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import azkaban.test.Utils;
import org.apache.commons.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.InteractiveTestJob;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FlowRunnerTest extends FlowRunnerTestBase {
  private File workingDir;
  private JobTypeManager jobtypeManager;
  private ProjectLoader fakeProjectLoader;
  @Mock private ExecutorLoader loader;

  private static final File TEST_DIR = new File("../azkaban-test/src/test/resources/azkaban/test/executions/exectest1");

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(loader.updateExecutableReference(anyInt(), anyLong())).thenReturn(true);
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
    JmxJobMBeanManager.getInstance().initialize(new Props());

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
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    runner = createFlowRunner(loader, eventCollector, "exec1");

    startThread(runner);
    succeedJobs("job3", "job4", "job6");

    assertFlowStatus(Status.SUCCEEDED);
    assertThreadShutDown();
    compareFinishedRuntime(runner);

    assertStatus("job1", Status.SUCCEEDED);
    assertStatus("job2", Status.SUCCEEDED);
    assertStatus("job3", Status.SUCCEEDED);
    assertStatus("job4", Status.SUCCEEDED);
    assertStatus("job5", Status.SUCCEEDED);
    assertStatus("job6", Status.SUCCEEDED);
    assertStatus("job7", Status.SUCCEEDED);
    assertStatus("job8", Status.SUCCEEDED);
    assertStatus("job10", Status.SUCCEEDED);

    try {
      eventCollector.checkEventExists(new Type[] { Type.FLOW_STARTED,
          Type.FLOW_FINISHED });
    } catch (Exception e) {
      System.out.println(e.getMessage());

      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void exec1Disabled() throws Exception {
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    ExecutableFlow exFlow = prepareExecDir(TEST_DIR, "exec1", 1);

    // Disable couple in the middle and at the end.
    exFlow.getExecutableNode("job1").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job6").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job5").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job10").setStatus(Status.DISABLED);

    runner = createFlowRunner(exFlow, loader, eventCollector);

    Assert.assertTrue(!runner.isKilled());
    assertFlowStatus(Status.READY);

    startThread(runner);
    succeedJobs("job3", "job4");

    assertThreadShutDown();
    compareFinishedRuntime(runner);

    assertFlowStatus(Status.SUCCEEDED);

    assertStatus("job1", Status.SKIPPED);
    assertStatus("job2", Status.SUCCEEDED);
    assertStatus("job3", Status.SUCCEEDED);
    assertStatus("job4", Status.SUCCEEDED);
    assertStatus("job5", Status.SKIPPED);
    assertStatus("job6", Status.SKIPPED);
    assertStatus("job7", Status.SUCCEEDED);
    assertStatus("job8", Status.SUCCEEDED);
    assertStatus("job10", Status.SKIPPED);

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
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec2", 1);

    runner = createFlowRunner(flow, loader, eventCollector);

    startThread(runner);
    succeedJobs("job6");

    Assert.assertTrue(!runner.isKilled());
    assertFlowStatus(Status.FAILED);

    assertStatus("job1", Status.SUCCEEDED);
    assertStatus("job2d", Status.FAILED);
    assertStatus("job3", Status.CANCELLED);
    assertStatus("job4", Status.CANCELLED);
    assertStatus("job5", Status.CANCELLED);
    assertStatus("job6", Status.SUCCEEDED);
    assertStatus("job7", Status.CANCELLED);
    assertStatus("job8", Status.CANCELLED);
    assertStatus("job9", Status.CANCELLED);
    assertStatus("job10", Status.CANCELLED);
    assertThreadShutDown();

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
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec2", 1);
    flow.getExecutionOptions().setFailureAction(FailureAction.CANCEL_ALL);

    runner = createFlowRunner(flow, loader, eventCollector);

    runner.run();

    Assert.assertTrue(runner.isKilled());

    assertFlowStatus(Status.KILLED);

    assertStatus("job1", Status.SUCCEEDED);
    assertStatus("job2d", Status.FAILED);
    assertStatus("job3", Status.CANCELLED);
    assertStatus("job4", Status.CANCELLED);
    assertStatus("job5", Status.CANCELLED);
    assertStatus("job6", Status.KILLED);
    assertStatus("job7", Status.CANCELLED);
    assertStatus("job8", Status.CANCELLED);
    assertStatus("job9", Status.CANCELLED);
    assertStatus("job10", Status.CANCELLED);

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
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec3", 1);
    flow.getExecutionOptions().setFailureAction(
        FailureAction.FINISH_ALL_POSSIBLE);
    runner = createFlowRunner(flow, loader, eventCollector);

    startThread(runner);
    succeedJobs("job3");

    assertFlowStatus(Status.FAILED);

    assertStatus("job1", Status.SUCCEEDED);
    assertStatus("job2d", Status.FAILED);
    assertStatus("job3", Status.SUCCEEDED);
    assertStatus("job4", Status.CANCELLED);
    assertStatus("job5", Status.CANCELLED);
    assertStatus("job6", Status.CANCELLED);
    assertStatus("job7", Status.SUCCEEDED);
    assertStatus("job8", Status.SUCCEEDED);
    assertStatus("job9", Status.SUCCEEDED);
    assertStatus("job10", Status.CANCELLED);
    assertThreadShutDown();

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
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    runner = createFlowRunner(loader, eventCollector, "exec1");

    startThread(runner);

    assertStatus("job1", Status.SUCCEEDED);
    assertStatus("job2", Status.SUCCEEDED);
    waitJobsStarted(runner, "job3", "job4", "job6");

    runner.kill("me");
    Assert.assertTrue(runner.isKilled());

    assertStatus("job5", Status.CANCELLED);
    assertStatus("job7", Status.CANCELLED);
    assertStatus("job8", Status.CANCELLED);
    assertStatus("job10", Status.CANCELLED);
    assertStatus("job3", Status.KILLED);
    assertStatus("job4", Status.KILLED);
    assertStatus("job6", Status.KILLED);
    assertThreadShutDown();

    assertFlowStatus(Status.KILLED);

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
    EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    runner = createFlowRunner(loader, eventCollector, "exec4-retry");

    runner.run();

    assertStatus("job-retry", Status.SUCCEEDED);
    assertStatus("job-pass", Status.SUCCEEDED);
    assertStatus("job-retry-fail", Status.FAILED);
    assertAttempts("job-retry", 3);
    assertAttempts("job-pass", 0);
    assertAttempts("job-retry-fail", 2);

    assertFlowStatus(Status.FAILED);
  }

  private void startThread(FlowRunner runner) {
    Assert.assertTrue(!runner.isKilled());
    Thread thread = new Thread(runner);
    thread.start();
  }

  private void assertAttempts(String name, int attempt) {
    ExecutableNode node = runner.getExecutableFlow().getExecutableNode(name);
    if (node.getAttempt() != attempt) {
      Assert.fail("Expected " + attempt + " got " + node.getAttempt()
          + " attempts " + name);
    }
  }
  
  private ExecutableFlow prepareExecDir(File execDir, String flowName,
      int execId) throws IOException {
    synchronized (this) {
      // clean interrupted status
      Thread.interrupted();
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

    loader.uploadExecutableFlow(exFlow);

    FlowRunner runner =
        new FlowRunner(exFlow, loader, fakeProjectLoader, jobtypeManager, azkabanProps);

    runner.addListener(eventCollector);

    return runner;
  }
}
