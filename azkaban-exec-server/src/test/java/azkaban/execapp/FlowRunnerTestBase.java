package azkaban.execapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import azkaban.event.Event;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.Status;
import java.util.function.Function;
import org.junit.Assert;

public class FlowRunnerTestBase {

  protected FlowRunner runner;
  protected EventCollectorListener eventCollector;

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

  public void assertThreadRunning() {
    waitFlowRunner(
        runner -> Status.isStatusRunning(runner.getExecutableFlow().getStatus())
            && runner.isRunnerThreadAlive());
  }

  public void waitFlowRunner(final Function<FlowRunner, Boolean> statusCheck) {
    for (int i = 0; i < 1000; i++) {
      if (statusCheck.apply(this.runner)) {
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

  protected void waitEventFired(final String nestedId, final Status status)
      throws InterruptedException {
    for (int i = 0; i < 1000; i++) {
      for (final Event event : this.eventCollector.getEventList()) {
        if (event.getData().getStatus() == status && event.getData().getNestedId()
            .equals(nestedId)) {
          return;
        }
      }
      synchronized (EventCollectorListener.handleEvent) {
        EventCollectorListener.handleEvent.wait(10L);
      }
    }
    fail("Event wasn't fired with [" + nestedId + "], " + status);
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

  protected void assertFlowStatus(final Status status) {
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    StatusTestUtils.waitForStatus(flow, status);
    printStatuses(status, flow);
    assertEquals(status, flow.getStatus());
  }

  protected void assertStatus(final String name, final Status status) {
    final ExecutableFlow exFlow = this.runner.getExecutableFlow();
    final ExecutableNode node = exFlow.getExecutableNodePath(name);
    assertNotNull(name + " wasn't found", node);
    StatusTestUtils.waitForStatus(node, status);
    printStatuses(status, node);
    assertEquals("Wrong status for [" + name + "]", status, node.getStatus());
  }

  protected void printStatuses(final Status status, final ExecutableNode node) {
    if (status != node.getStatus()) {
      printTestJobs();
      printFlowJobs(this.runner.getExecutableFlow());
    }
  }

  private void printTestJobs() {
    for (final String testJob : InteractiveTestJob.getTestJobNames()) {
      final ExecutableNode testNode = this.runner.getExecutableFlow()
          .getExecutableNodePath(testJob);
      System.err.println("testJob: " + testNode.getNestedId() + " " + testNode.getStatus());
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

}
