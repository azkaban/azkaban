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

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.execapp.jmx.JmxJobMBeanManager;
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
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FlowRunnerTest extends FlowRunnerTestBase {

  private static final File TEST_DIR = ExecutionsTestUtil.getFlowDir("exectest1");
  private File workingDir;
  private JobTypeManager jobtypeManager;

  @Mock
  private ProjectLoader fakeProjectLoader;

  @Mock
  private ExecutorLoader loader;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(this.loader.updateExecutableReference(anyInt(), anyLong())).thenReturn(true);
    System.out.println("Create temp dir");
    this.workingDir = new File("build/tmp/_AzkabanTestDir_" + System.currentTimeMillis());
    if (this.workingDir.exists()) {
      FileUtils.deleteDirectory(this.workingDir);
    }
    this.workingDir.mkdirs();
    this.jobtypeManager =
        new JobTypeManager(null, null, this.getClass().getClassLoader());
    final JobTypePluginSet pluginSet = this.jobtypeManager.getJobTypePluginSet();
    pluginSet.setCommonPluginLoadProps(AllJobExecutorTests.setUpCommonProps());
    pluginSet.addPluginClass("test", InteractiveTestJob.class);
    Utils.initServiceProvider();
    JmxJobMBeanManager.getInstance().initialize(new Props());

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

  @Test
  public void exec1Normal() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    this.runner = createFlowRunner(this.loader, eventCollector, "exec1");

    startThread(this.runner);
    succeedJobs("job3", "job4", "job6");

    assertFlowStatus(Status.SUCCEEDED);
    assertThreadShutDown();
    compareFinishedRuntime(this.runner);

    assertStatus("job1", Status.SUCCEEDED);
    assertStatus("job2", Status.SUCCEEDED);
    assertStatus("job3", Status.SUCCEEDED);
    assertStatus("job4", Status.SUCCEEDED);
    assertStatus("job5", Status.SUCCEEDED);
    assertStatus("job6", Status.SUCCEEDED);
    assertStatus("job7", Status.SUCCEEDED);
    assertStatus("job8", Status.SUCCEEDED);
    assertStatus("job10", Status.SUCCEEDED);

    eventCollector.assertEvents(Type.FLOW_STARTED, Type.FLOW_FINISHED);
  }

  @Test
  public void exec1Disabled() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final ExecutableFlow exFlow = prepareExecDir(TEST_DIR, "exec1", 1);

    // Disable couple in the middle and at the end.
    exFlow.getExecutableNode("job1").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job6").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job5").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job10").setStatus(Status.DISABLED);

    this.runner = createFlowRunner(exFlow, this.loader, eventCollector);

    Assert.assertTrue(!this.runner.isKilled());
    assertFlowStatus(Status.READY);

    startThread(this.runner);
    succeedJobs("job3", "job4");

    assertThreadShutDown();
    compareFinishedRuntime(this.runner);

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

    eventCollector.assertEvents(Type.FLOW_STARTED, Type.FLOW_FINISHED);
  }

  @Test
  public void exec1Failed() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec2", 1);

    this.runner = createFlowRunner(flow, this.loader, eventCollector);

    startThread(this.runner);
    succeedJobs("job6");

    Assert.assertTrue(!this.runner.isKilled());
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

    eventCollector.assertEvents(Type.FLOW_STARTED, Type.FLOW_FINISHED);
  }

  @Test
  public void exec1FailedKillAll() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec2", 1);
    flow.getExecutionOptions().setFailureAction(FailureAction.CANCEL_ALL);

    this.runner = createFlowRunner(flow, this.loader, eventCollector);

    startThread(this.runner);
    assertThreadShutDown();

    Assert.assertTrue(this.runner.isKilled());

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

    eventCollector.assertEvents(Type.FLOW_STARTED, Type.FLOW_FINISHED);
  }

  @Test
  public void exec1FailedFinishRest() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    final ExecutableFlow flow = prepareExecDir(TEST_DIR, "exec3", 1);
    flow.getExecutionOptions().setFailureAction(
        FailureAction.FINISH_ALL_POSSIBLE);
    this.runner = createFlowRunner(flow, this.loader, eventCollector);

    startThread(this.runner);
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

    eventCollector.assertEvents(Type.FLOW_STARTED, Type.FLOW_FINISHED);
  }

  @Test
  public void execAndCancel() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    this.runner = createFlowRunner(this.loader, eventCollector, "exec1");

    startThread(this.runner);

    assertStatus("job1", Status.SUCCEEDED);
    assertStatus("job2", Status.SUCCEEDED);
    waitJobsStarted(this.runner, "job3", "job4", "job6");

    this.runner.kill("me");
    Assert.assertTrue(this.runner.isKilled());

    assertStatus("job5", Status.CANCELLED);
    assertStatus("job7", Status.CANCELLED);
    assertStatus("job8", Status.CANCELLED);
    assertStatus("job10", Status.CANCELLED);
    assertStatus("job3", Status.KILLED);
    assertStatus("job4", Status.KILLED);
    assertStatus("job6", Status.KILLED);
    assertThreadShutDown();

    assertFlowStatus(Status.KILLED);

    eventCollector.assertEvents(Type.FLOW_STARTED, Type.FLOW_FINISHED);
  }

  @Test
  public void execRetries() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(Event.Type.JOB_FINISHED,
        Event.Type.JOB_STARTED, Event.Type.JOB_STATUS_CHANGED);
    this.runner = createFlowRunner(this.loader, eventCollector, "exec4-retry");

    startThread(this.runner);
    assertThreadShutDown();

    assertStatus("job-retry", Status.SUCCEEDED);
    assertStatus("job-pass", Status.SUCCEEDED);
    assertStatus("job-retry-fail", Status.FAILED);
    assertAttempts("job-retry", 3);
    assertAttempts("job-pass", 0);
    assertAttempts("job-retry-fail", 2);

    assertFlowStatus(Status.FAILED);
  }

  private void startThread(final FlowRunner runner) {
    Assert.assertTrue(!runner.isKilled());
    final Thread thread = new Thread(runner);
    thread.start();
  }

  private void assertAttempts(final String name, final int attempt) {
    final ExecutableNode node = this.runner.getExecutableFlow().getExecutableNode(name);
    if (node.getAttempt() != attempt) {
      Assert.fail("Expected " + attempt + " got " + node.getAttempt()
          + " attempts " + name);
    }
  }

  private ExecutableFlow prepareExecDir(final File execDir, final String flowName,
      final int execId) throws IOException {
    FileUtils.copyDirectory(execDir, this.workingDir);

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
    final ExecutableFlow exFlow = prepareExecDir(TEST_DIR, flowName, 1);

    loader.uploadExecutableFlow(exFlow);

    final FlowRunner runner =
        new FlowRunner(exFlow, loader, this.fakeProjectLoader, this.jobtypeManager, azkabanProps);

    runner.addListener(eventCollector);

    return runner;
  }
}
