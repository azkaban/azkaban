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

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.Status;
import azkaban.spi.EventType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlowRunnerTest extends FlowRunnerTestBase {

  private FlowRunnerTestUtil testUtil;

  @Before
  public void setUp() throws Exception {
    this.testUtil = new FlowRunnerTestUtil("exectest1", this.temporaryFolder);
  }

  @Test
  public void exec1Normal() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(EventType.JOB_FINISHED,
        EventType.JOB_STARTED, EventType.JOB_STATUS_CHANGED);
    this.runner = this.testUtil.createFromFlowFile(eventCollector, "exec1");

    FlowRunnerTestUtil.startThread(this.runner);
    succeedJobs("job3", "job4", "job6");

    waitForAndAssertFlowStatus(Status.SUCCEEDED);
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

    eventCollector.assertEvents(EventType.FLOW_STARTED, EventType.FLOW_FINISHED);
  }

  @Test
  public void exec1Disabled() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(EventType.JOB_FINISHED,
        EventType.JOB_STARTED, EventType.JOB_STATUS_CHANGED);

    this.runner = this.testUtil.createFromFlowFile(eventCollector, "exec1");
    final ExecutableFlow exFlow = this.runner.getExecutableFlow();

    // Disable couple in the middle and at the end.
    exFlow.getExecutableNode("job1").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job6").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job5").setStatus(Status.DISABLED);
    exFlow.getExecutableNode("job10").setStatus(Status.DISABLED);

    Assert.assertTrue(!this.runner.isKilled());
    waitForAndAssertFlowStatus(Status.READY);

    FlowRunnerTestUtil.startThread(this.runner);
    succeedJobs("job3", "job4");

    assertThreadShutDown();
    compareFinishedRuntime(this.runner);

    waitForAndAssertFlowStatus(Status.SUCCEEDED);

    assertStatus("job1", Status.SKIPPED);
    assertStatus("job2", Status.SUCCEEDED);
    assertStatus("job3", Status.SUCCEEDED);
    assertStatus("job4", Status.SUCCEEDED);
    assertStatus("job5", Status.SKIPPED);
    assertStatus("job6", Status.SKIPPED);
    assertStatus("job7", Status.SUCCEEDED);
    assertStatus("job8", Status.SUCCEEDED);
    assertStatus("job10", Status.SKIPPED);

    eventCollector.assertEvents(EventType.FLOW_STARTED, EventType.FLOW_FINISHED);
  }

  @Test
  public void exec1Failed() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(EventType.JOB_FINISHED,
        EventType.JOB_STARTED, EventType.JOB_STATUS_CHANGED);

    this.runner = this.testUtil.createFromFlowFile(eventCollector, "exec2");

    FlowRunnerTestUtil.startThread(this.runner);
    succeedJobs("job6");

    Assert.assertTrue(!this.runner.isKilled());
    waitForAndAssertFlowStatus(Status.FAILED);

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

    eventCollector.assertEvents(EventType.FLOW_STARTED, EventType.FLOW_FINISHED);
  }

  @Test
  public void exec1FailedKillAll() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(EventType.JOB_FINISHED,
        EventType.JOB_STARTED, EventType.JOB_STATUS_CHANGED);
    final ExecutionOptions options = new ExecutionOptions();
    options.setFailureAction(FailureAction.CANCEL_ALL);

    this.runner = this.testUtil.createFromFlowFile("exec2", eventCollector, options);

    FlowRunnerTestUtil.startThread(this.runner);
    assertThreadShutDown();

    Assert.assertTrue(this.runner.isKilled());

    waitForAndAssertFlowStatus(Status.KILLED);

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

    eventCollector.assertEvents(EventType.FLOW_STARTED, EventType.FLOW_FINISHED);
  }

  @Test
  public void exec1FailedFinishRest() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(EventType.JOB_FINISHED,
        EventType.JOB_STARTED, EventType.JOB_STATUS_CHANGED);
    final ExecutionOptions options = new ExecutionOptions();
    options.setFailureAction(FailureAction.FINISH_ALL_POSSIBLE);
    this.runner = this.testUtil.createFromFlowFile("exec3", eventCollector, options);

    FlowRunnerTestUtil.startThread(this.runner);
    succeedJobs("job3");

    waitForAndAssertFlowStatus(Status.FAILED);

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

    eventCollector.assertEvents(EventType.FLOW_STARTED, EventType.FLOW_FINISHED);
  }

  @Test
  public void execAndCancel() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(EventType.JOB_FINISHED,
        EventType.JOB_STARTED, EventType.JOB_STATUS_CHANGED);
    this.runner = this.testUtil.createFromFlowFile(eventCollector, "exec1");

    FlowRunnerTestUtil.startThread(this.runner);

    assertStatus("job1", Status.SUCCEEDED);
    assertStatus("job2", Status.SUCCEEDED);
    waitJobsStarted(this.runner, "job3", "job4", "job6");

    InteractiveTestJob.getTestJob("job3").ignoreCancel();
    this.runner.kill("me");
    assertStatus("job3", Status.KILLING);
    assertFlowStatus(this.runner.getExecutableFlow(), Status.KILLING);
    InteractiveTestJob.getTestJob("job3").failJob();

    Assert.assertTrue(this.runner.isKilled());

    assertStatus("job5", Status.CANCELLED);
    assertStatus("job7", Status.CANCELLED);
    assertStatus("job8", Status.CANCELLED);
    assertStatus("job10", Status.CANCELLED);
    assertStatus("job3", Status.KILLED);
    assertStatus("job4", Status.KILLED);
    assertStatus("job6", Status.KILLED);
    assertThreadShutDown();

    waitForAndAssertFlowStatus(Status.KILLED);

    eventCollector.assertEvents(EventType.FLOW_STARTED, EventType.FLOW_FINISHED);
  }

  @Test
  public void execRetries() throws Exception {
    final EventCollectorListener eventCollector = new EventCollectorListener();
    eventCollector.setEventFilterOut(EventType.JOB_FINISHED,
        EventType.JOB_STARTED, EventType.JOB_STATUS_CHANGED);
    this.runner = this.testUtil.createFromFlowFile(eventCollector, "exec4-retry");

    FlowRunnerTestUtil.startThread(this.runner);
    assertThreadShutDown();

    assertStatus("job-retry", Status.SUCCEEDED);
    assertStatus("job-pass", Status.SUCCEEDED);
    assertStatus("job-retry-fail", Status.FAILED);
    assertAttempts("job-retry", 3);
    assertAttempts("job-pass", 0);
    assertAttempts("job-retry-fail", 2);

    waitForAndAssertFlowStatus(Status.FAILED);
  }

  private void assertAttempts(final String name, final int attempt) {
    final ExecutableNode node = this.runner.getExecutableFlow().getExecutableNode(name);
    if (node.getAttempt() != attempt) {
      Assert.fail("Expected " + attempt + " got " + node.getAttempt()
          + " attempts " + name);
    }
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
}
