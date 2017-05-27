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

  public void assertThreadShutDown() {
    waitFlowRunner(runner -> runner.getExecutableFlow().isFlowFinished() && !runner.isRunnerThreadAlive());
  }

  public void assertThreadRunning() {
    waitFlowRunner(runner -> !runner.getExecutableFlow().isFlowFinished() && runner.isRunnerThreadAlive());
  }

  public void waitFlowRunner(Function<FlowRunner, Boolean> statusCheck) {
    for (int i = 0; i < 100; i++) {
      if (statusCheck.apply(runner)) {
        return;
      }
      synchronized (EventCollectorListener.handleEvent) {
        try {
          EventCollectorListener.handleEvent.wait(10L);
        } catch (InterruptedException e) {
        }
      }
    }
    Assert.fail("Flow didn't reach expected status");
  }

  public void waitJobStatuses(Function<Status, Boolean> statusCheck,
      String... jobs) {
    for (int i = 0; i < 100; i++) {
      if (checkJobStatuses(statusCheck, jobs)) {
        return;
      }
      synchronized (EventCollectorListener.handleEvent) {
        try {
          EventCollectorListener.handleEvent.wait(10L);
        } catch (InterruptedException e) {
        }
      }
    }
    Assert.fail("Jobs didn't reach expected statuses");
  }

  public void waitJobsStarted(FlowRunner runner, String... jobs) {
    waitJobStatuses(FlowRunnerTest::isStarted, jobs);
  }

  protected void waitEventFired(String nestedId, Status status) throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      for (Event event : eventCollector.getEventList()) {
        if (event.getData().getStatus() == status && event.getData().getNestedId().equals(nestedId)) {
          return;
        }
      }
      synchronized (EventCollectorListener.handleEvent) {
        EventCollectorListener.handleEvent.wait(10L);
      }
    }
    fail("Event wasn't fired with [" + nestedId + "], " + status);
  }

  public static boolean isStarted(Status status) {
    if (status == Status.QUEUED) {
      return false;
    } else if (!Status.isStatusFinished(status) && !Status.isStatusRunning(status)) {
      return false;
    }
    return true;
  }

  public boolean checkJobStatuses(Function<Status, Boolean> statusCheck,
      String[] jobs) {
    ExecutableFlow exFlow = runner.getExecutableFlow();
    for (String name : jobs) {
      ExecutableNode node = exFlow.getExecutableNodePath(name);
      assertNotNull(name + " wasn't found", node);
      if (!statusCheck.apply(node.getStatus())) {
        return false;
      }
    }
    return true;
  }

  protected void assertFlowStatus(Status status) {
    ExecutableFlow flow = runner.getExecutableFlow();
    for (int i = 0; i < 100; i++) {
      if (flow.getStatus() == status) {
        break;
      }
      synchronized (EventCollectorListener.handleEvent) {
        try {
          EventCollectorListener.handleEvent.wait(10L);
        } catch (InterruptedException e) {
        }
      }
    }
    printStatuses(status, flow);
    assertEquals(status, flow.getStatus());
  }

  protected void assertStatus(String name, Status status) {
    ExecutableFlow exFlow = runner.getExecutableFlow();
    ExecutableNode node = exFlow.getExecutableNodePath(name);
    assertNotNull(name + " wasn't found", node);
    for (int i = 0; i < 100; i++) {
      if (node.getStatus() == status) {
        break;
      }
      synchronized (EventCollectorListener.handleEvent) {
        try {
          EventCollectorListener.handleEvent.wait(10L);
        } catch (InterruptedException e) {
        }
      }
    }
    printStatuses(status, node);
    assertEquals("Wrong status for [" + name + "]", status, node.getStatus());
  }

  protected void printStatuses(Status status, ExecutableNode node) {
    if (status != node.getStatus()) {
      printTestJobs();
      printFlowJobs(runner.getExecutableFlow());
    }
  }

  private void printTestJobs() {
    for (String testJob : InteractiveTestJob.testJobs.keySet()) {
      ExecutableNode testNode = runner.getExecutableFlow().getExecutableNodePath(testJob);
      System.err.println("testJob: " + testNode.getNestedId() + " " + testNode.getStatus());
    }
  }

  private void printFlowJobs(ExecutableFlowBase flow) {
    System.err.println("ExecutableFlow: " + flow.getNestedId() + " " + flow.getStatus());
    for (ExecutableNode node : flow.getExecutableNodes()) {
      if (node instanceof ExecutableFlowBase) {
        printFlowJobs((ExecutableFlowBase) node);
      } else {
        System.err.println("ExecutableNode: " + node.getNestedId() + " " + node.getStatus());
      }
    }
  }

  protected void succeedJobs(String... jobs) {
    waitJobsStarted(runner, jobs);
    for (String name : jobs) {
      InteractiveTestJob.getTestJob(name).succeedJob();
    }
  }

}
