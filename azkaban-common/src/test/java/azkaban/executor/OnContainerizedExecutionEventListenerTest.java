package azkaban.executor;


import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import azkaban.DispatchMethod;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.utils.TestUtils;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FlowUtils.class})
public class OnContainerizedExecutionEventListenerTest {

  private Project project1;
  private Flow flow1;
  private ExecutableFlow originalExFlow;
  private ExecutableFlow retriedExFlow;
  private OnContainerizedExecutionEventListener onContainerizedExecutionEventListener;
  private ExecutorLoader executorLoader;
  private ExecutorManagerAdapter executorManagerAdapter;

  @Before
  public void before() throws Exception {
    this.project1 = mock(Project.class);
    this.flow1 = mock(Flow.class);
    this.originalExFlow = TestUtils.createTestExecutableFlow("exectest1", "exec1",
        DispatchMethod.CONTAINERIZED);
    this.originalExFlow.setFlowRetryRootExecutionID(1000);
    this.originalExFlow.setFlowRetryRootExecutionID(1001);
    this.originalExFlow.setExecutionId(1002);
    this.retriedExFlow = TestUtils.createTestExecutableFlow("exectest1", "exec1",
        DispatchMethod.CONTAINERIZED);
    this.originalExFlow.setExecutionId(1010);
    this.executorLoader = mock(ExecutorLoader.class);
    this.executorManagerAdapter = mock(ExecutorManagerAdapter.class);
    this.onContainerizedExecutionEventListener = new OnContainerizedExecutionEventListener(
        this.executorLoader,
        this.executorManagerAdapter,
        mock(ProjectManager.class)
    );
  }

  @Test
  public void testRestartExecutableFlow() throws ExecutorManagerException, IOException {
    this.originalExFlow.setSystemDefinedRetryCount(1);
    this.originalExFlow.setUserDefinedRetryCount(1);
    // set up mocking
    PowerMockito.mockStatic(FlowUtils.class, invocation -> {
      if (invocation.getMethod().getName().equals("getProject")) {
        return this.project1;
      } else if (invocation.getMethod().getName().equals("getFlow")) {
        return flow1;
      }
      return invocation.callRealMethod();
    });
    when(this.executorManagerAdapter.createExecutableFlow(any(), any())).thenReturn(
        this.retriedExFlow);
    when(this.executorManagerAdapter.submitExecutableFlow(any(), any())).thenReturn("");
    doNothing().when(this.executorLoader).updateExecutableFlow(any());

    // trigger
    this.onContainerizedExecutionEventListener.restartExecutableFlow(this.originalExFlow);

    // validate
    assertEquals(this.originalExFlow.getSystemDefinedRetryCount(),
        this.retriedExFlow.getSystemDefinedRetryCount());
    assertEquals(this.originalExFlow.getUserDefinedRetryCount(),
        this.retriedExFlow.getUserDefinedRetryCount());

    assertEquals(this.originalExFlow.getFlowRetryRootExecutionID(),
        this.retriedExFlow.getFlowRetryRootExecutionID());
    assertEquals(this.originalExFlow.getExecutionId(),
        this.retriedExFlow.getFlowRetryParentExecutionID());

    assertEquals(this.retriedExFlow.getExecutionId(),
        this.originalExFlow.getFlowRetryChildExecutionID());
  }

  @Test
  public void testRestartExecutableFlow_NoRoot() throws ExecutorManagerException, IOException {
    this.originalExFlow.setSystemDefinedRetryCount(1);
    this.originalExFlow.setUserDefinedRetryCount(1);
    // no root execution
    this.originalExFlow.setFlowRetryRootExecutionID(-1);
    // set up mocking
    PowerMockito.mockStatic(FlowUtils.class, invocation -> {
      if (invocation.getMethod().getName().equals("getProject")) {
        return this.project1;
      } else if (invocation.getMethod().getName().equals("getFlow")) {
        return flow1;
      }
      return invocation.callRealMethod();
    });
    when(this.executorManagerAdapter.createExecutableFlow(any(), any())).thenReturn(
        this.retriedExFlow);
    when(this.executorManagerAdapter.submitExecutableFlow(any(), any())).thenReturn("");
    doNothing().when(this.executorLoader).updateExecutableFlow(any());

    // trigger
    this.onContainerizedExecutionEventListener.restartExecutableFlow(this.originalExFlow);

    // validate
    assertEquals(this.originalExFlow.getSystemDefinedRetryCount(),
        this.retriedExFlow.getSystemDefinedRetryCount());
    assertEquals(this.originalExFlow.getUserDefinedRetryCount(),
        this.retriedExFlow.getUserDefinedRetryCount());

    // retry-flow should use original flow id as root
    assertEquals(this.originalExFlow.getExecutionId(),
        this.retriedExFlow.getFlowRetryRootExecutionID());
    assertEquals(this.originalExFlow.getExecutionId(),
        this.retriedExFlow.getFlowRetryParentExecutionID());

    assertEquals(this.retriedExFlow.getExecutionId(),
        this.originalExFlow.getFlowRetryChildExecutionID());
  }

  @Test
  public void testRestartExecutableFlow_failGetProject() throws ExecutorManagerException,
      IOException {
    // set up mocking
    PowerMockito.mockStatic(FlowUtils.class, invocation -> {
      if (invocation.getMethod().getName().equals("getProject")) {
        throw new RuntimeException("ops");
      }
      return invocation.callRealMethod();
    });

    // trigger
    this.onContainerizedExecutionEventListener.restartExecutableFlow(this.originalExFlow);

    // validate
    verifyZeroInteractions(this.executorManagerAdapter);
    verifyZeroInteractions(this.executorLoader);
  }

  @Test
  public void testRestartExecutableFlow_failGetFlow() throws ExecutorManagerException,
      IOException {
    // set up mocking
    PowerMockito.mockStatic(FlowUtils.class, invocation -> {
      if (invocation.getMethod().getName().equals("getProject")) {
        return this.project1;
      } else if (invocation.getMethod().getName().equals("getFlow")) {
        throw new RuntimeException("ops");
      }
      return invocation.callRealMethod();
    });

    // trigger
    this.onContainerizedExecutionEventListener.restartExecutableFlow(this.originalExFlow);

    // validate
    verifyZeroInteractions(this.executorManagerAdapter);
    verifyZeroInteractions(this.executorLoader);
  }

  @Test
  public void testRestartExecutableFlow_failSubmit() throws ExecutorManagerException,
      IOException {
    // set up mocking
    PowerMockito.mockStatic(FlowUtils.class, invocation -> {
      if (invocation.getMethod().getName().equals("getProject")) {
        return this.project1;
      } else if (invocation.getMethod().getName().equals("getFlow")) {
        return flow1;
      }
      return invocation.callRealMethod();
    });
    when(this.executorManagerAdapter.createExecutableFlow(any(), any())).thenReturn(
        this.retriedExFlow);
    when(this.executorManagerAdapter.submitExecutableFlow(any(), any()))
        .thenThrow(new ExecutorManagerException("ops"));
    // trigger
    this.onContainerizedExecutionEventListener.restartExecutableFlow(this.originalExFlow);

    // validate
    verifyZeroInteractions(this.executorLoader);
  }

  @Test
  public void testRestartExecutableFlow_failUpdateOriginalExec() throws ExecutorManagerException,
      IOException {
    // set up mocking
    PowerMockito.mockStatic(FlowUtils.class, invocation -> {
      if (invocation.getMethod().getName().equals("getProject")) {
        return this.project1;
      } else if (invocation.getMethod().getName().equals("getFlow")) {
        return flow1;
      }
      return invocation.callRealMethod();
    });
    when(this.executorManagerAdapter.createExecutableFlow(any(), any())).thenReturn(
        this.retriedExFlow);
    when(this.executorManagerAdapter.submitExecutableFlow(any(), any())).thenReturn("");
    doThrow(new ExecutorManagerException("ops")).when(this.executorLoader)
        .updateExecutableFlow(any());

    // trigger
    this.onContainerizedExecutionEventListener.restartExecutableFlow(this.originalExFlow);
  }

  @Test
  public void testDisableSucceededSkippedJobsInRetryFlow_SimpleFlow()
      throws ExecutorManagerException {
    this.originalExFlow.setStatus(Status.FAILED);
    this.originalExFlow.getExecutableNode("job1").setStatus(Status.SUCCEEDED);
    this.originalExFlow.getExecutableNode("job2").setStatus(Status.FAILED_SUCCEEDED);
    this.originalExFlow.getExecutableNode("job3").setStatus(Status.SKIPPED);
    this.originalExFlow.getExecutableNode("job4").setStatus(Status.SUCCEEDED);
    this.originalExFlow.getExecutableNode("job5").setStatus(Status.DISABLED);
    this.originalExFlow.getExecutableNode("job6").setStatus(Status.FAILED);
    this.originalExFlow.getExecutableNode("job7").setStatus(Status.FAILED);
    this.originalExFlow.getExecutableNode("job8").setStatus(Status.PREPARING);
    this.originalExFlow.getExecutableNode("job10").setStatus(Status.READY);

    // trigger
    OnContainerizedExecutionEventListener.disableSucceededSkippedJobsInRetryFlow(
        this.originalExFlow, this.retriedExFlow);

    // validate
    assertEquals(Status.DISABLED, this.retriedExFlow.getExecutableNode("job1").getStatus());
    assertEquals(Status.DISABLED, this.retriedExFlow.getExecutableNode("job2").getStatus());
    assertEquals(Status.SKIPPED, this.retriedExFlow.getExecutableNode("job3").getStatus());
    assertEquals(Status.DISABLED, this.retriedExFlow.getExecutableNode("job4").getStatus());
    assertEquals(Status.DISABLED, this.retriedExFlow.getExecutableNode("job5").getStatus());
    assertEquals(Status.READY, this.retriedExFlow.getExecutableNode("job6").getStatus());
    assertEquals(Status.READY, this.retriedExFlow.getExecutableNode("job7").getStatus());
    assertEquals(Status.READY, this.retriedExFlow.getExecutableNode("job8").getStatus());
    assertEquals(Status.READY, this.retriedExFlow.getExecutableNode("job10").getStatus());

  }

  @Test
  public void testDisableSucceededSkippedJobsInRetryFlow_EmbeddedFlow()
      throws ExecutorManagerException {
    ExecutableFlow originalFlow = TestUtils.getEmbeddedTestExecutionFlow();
    originalFlow.getExecutableNode("joba").setStatus(Status.SUCCEEDED);

    originalFlow.getExecutableNode("jobb").setStatus(Status.SUCCEEDED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobb")).getExecutableNode("innerJobA")
        .setStatus(Status.SUCCEEDED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobb")).getExecutableNode("innerJobB")
        .setStatus(Status.SKIPPED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobb")).getExecutableNode("innerJobC")
        .setStatus(Status.SKIPPED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobb")).getExecutableNode("innerFlow")
        .setStatus(Status.SUCCEEDED);

    originalFlow.getExecutableNode("jobc").setStatus(Status.FAILED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobc")).getExecutableNode("innerJobA")
        .setStatus(Status.SUCCEEDED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobc")).getExecutableNode("innerJobB")
        .setStatus(Status.DISABLED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobc")).getExecutableNode("innerJobC")
        .setStatus(Status.FAILED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobc")).getExecutableNode("innerFlow")
        .setStatus(Status.FAILED);

    originalFlow.getExecutableNode("jobd").setStatus(Status.SUCCEEDED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobd")).getExecutableNode("innerJobA")
        .setStatus(Status.SUCCEEDED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobd")).getExecutableNode("innerJobB")
        .setStatus(Status.DISABLED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobd")).getExecutableNode("innerJobC")
        .setStatus(Status.DISABLED);
    ((ExecutableFlowBase) originalFlow.getExecutableNode("jobd")).getExecutableNode("innerFlow")
        .setStatus(Status.DISABLED);

    // to be retried flow
    ExecutableFlow retriedFlow = TestUtils.getEmbeddedTestExecutionFlow();

    // trigger
    OnContainerizedExecutionEventListener.disableSucceededSkippedJobsInRetryFlow(
        originalFlow, retriedFlow);

    // validate

    assertEquals(Status.DISABLED, retriedFlow.getExecutableNode("joba").getStatus());

    assertEquals(Status.DISABLED, retriedFlow.getExecutableNode("jobb").getStatus());
    assertEquals(Status.DISABLED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobb")).getExecutableNode("innerJobA")
            .getStatus());
    assertEquals(Status.SKIPPED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobb")).getExecutableNode("innerJobB")
            .getStatus());
    assertEquals(Status.SKIPPED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobb")).getExecutableNode("innerJobC")
            .getStatus());
    assertEquals(Status.DISABLED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobb")).getExecutableNode("innerFlow")
            .getStatus());

    assertEquals(Status.READY, retriedFlow.getExecutableNode("jobc").getStatus());
    assertEquals(Status.DISABLED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobc")).getExecutableNode("innerJobA")
            .getStatus());
    assertEquals(Status.DISABLED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobc")).getExecutableNode("innerJobB")
            .getStatus());
    assertEquals(Status.READY,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobc")).getExecutableNode("innerJobC")
            .getStatus());
    assertEquals(Status.READY,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobc")).getExecutableNode("innerFlow")
            .getStatus());

    assertEquals(Status.DISABLED, retriedFlow.getExecutableNode("jobd").getStatus());
    assertEquals(Status.DISABLED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobd")).getExecutableNode("innerJobA")
            .getStatus());
    assertEquals(Status.DISABLED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobd")).getExecutableNode("innerJobB")
            .getStatus());
    assertEquals(Status.DISABLED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobd")).getExecutableNode("innerJobC")
            .getStatus());
    assertEquals(Status.DISABLED,
        ((ExecutableFlowBase) retriedFlow.getExecutableNode("jobd")).getExecutableNode("innerFlow")
            .getStatus());
  }
}