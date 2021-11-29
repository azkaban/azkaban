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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.Status;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class FlowRunnerTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected FlowRunner runner;

  public static boolean isStarted(final Status status) {
    if (status == Status.QUEUED) {
      return false;
    }
    return Status.isStatusFinished(status) || Status.isStatusRunning(status);
  }

  public void assertThreadShutDown() {
    waitFlowRunner(
        runner -> Status.isStatusFinished(runner.getExecutableFlow().getStatus())
            && !runner.isRunnerThreadAlive());
  }

  public void assertThreadShutDown(final FlowRunner flowRunner) {
    waitFlowRunner(flowRunner,
        runner -> Status.isStatusFinished(runner.getExecutableFlow().getStatus())
            && !runner.isRunnerThreadAlive());
  }

  public void assertThreadRunning() {
    waitFlowRunner(
        runner -> Status.isStatusRunning(runner.getExecutableFlow().getStatus())
            && runner.isRunnerThreadAlive());
  }

  public void waitFlowRunner(final Function<FlowRunner, Boolean> statusCheck) {
    waitFlowRunner(this.runner, statusCheck);
  }

  public void waitFlowRunner(final FlowRunner runner,
      final Function<FlowRunner, Boolean> statusCheck) {
    for (int i = 0; i < 1000; i++) {
      if (statusCheck.apply(runner)) {
        return;
      }
      synchronized (EventCollectorListener.handleEvent) {
        try {
          EventCollectorListener.handleEvent.wait(10L);
        } catch (final InterruptedException e) {
        }
      }
    }
    Assert.fail("Flow didn't reach expected status");
  }

  public void waitJobStatuses(final Function<Status, Boolean> statusCheck,
      final String... jobs) {
    for (int i = 0; i < 1000; i++) {
      if (checkJobStatuses(statusCheck, jobs)) {
        return;
      }
      synchronized (EventCollectorListener.handleEvent) {
        try {
          EventCollectorListener.handleEvent.wait(10L);
        } catch (final InterruptedException e) {
        }
      }
    }
    Assert.fail("Jobs didn't reach expected statuses");
  }

  public void waitJobsStarted(final FlowRunner runner, final String... jobs) {
    waitJobStatuses(FlowRunnerTest::isStarted, jobs);
  }

  public boolean checkJobStatuses(final Function<Status, Boolean> statusCheck,
      final String[] jobs) {
    final ExecutableFlow exFlow = this.runner.getExecutableFlow();
    for (final String name : jobs) {
      final ExecutableNode node = exFlow.getExecutableNodePath(name);
      assertNotNull(name + " wasn't found", node);
      if (!statusCheck.apply(node.getStatus())) {
        return false;
      }
    }
    return true;
  }

  protected void waitForAndAssertFlowStatus(final Status status) {
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    assertFlowStatus(flow, status);
  }

  protected void assertFlowStatus(final ExecutableFlow flow, final Status status) {
    StatusTestUtils.waitForStatus(flow, status);
    printStatuses(status, flow, flow);
    assertEquals(status, flow.getStatus());
  }

  protected void assertStatus(final ExecutableFlow flow, final String name, final Status status) {
    final ExecutableNode node = flow.getExecutableNodePath(name);
    assertNotNull(name + " wasn't found", node);
    StatusTestUtils.waitForStatus(node, status);
    printStatuses(status, node, flow);
    assertEquals("Wrong status for [" + name + "]", status, node.getStatus());
  }

  protected void assertStatus(final String name, final Status status) {
    final ExecutableFlow exFlow = this.runner.getExecutableFlow();
    assertStatus(exFlow, name, status);
  }

  protected void printStatuses(final Status status, final ExecutableNode node,
      final ExecutableFlow flow) {
    if (status != node.getStatus()) {
      printTestJobs(flow);
      printFlowJobs(flow);
    }
  }

  private void printTestJobs(final ExecutableFlow flow) {
    for (final String testJob : InteractiveTestJob.getTestJobNames()) {
      final ExecutableNode testNode = flow.getExecutableNodePath(testJob);
      if (testNode != null) {
        System.err.println("testJob: " + testNode.getNestedId() + " " + testNode.getStatus());
      }
    }
  }

  private void printFlowJobs(final ExecutableFlowBase flow) {
    System.err.println("ExecutableFlow: " + flow.getNestedId() + " " + flow.getStatus());
    for (final ExecutableNode node : flow.getExecutableNodes()) {
      if (node instanceof ExecutableFlowBase) {
        printFlowJobs((ExecutableFlowBase) node);
      } else {
        System.err.println("ExecutableNode: " + node.getNestedId() + " " + node.getStatus());
      }
    }
  }

  protected void succeedJobs(final String... jobs) {
    waitJobsStarted(this.runner, jobs);
    for (final String name : jobs) {
      InteractiveTestJob.getTestJob(name).succeedJob();
    }
  }

  protected void assertFlowVersion(final ExecutableFlow flow, final double flowVersion){
    assertEquals(flow.getAzkabanFlowVersion(), flowVersion, 0.1);
  }

}
