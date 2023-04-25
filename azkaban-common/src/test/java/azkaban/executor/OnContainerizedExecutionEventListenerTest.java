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
  public void before()  throws Exception {
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
    when(this.executorManagerAdapter.createExecutableFlow(any(), any())).thenReturn(this.retriedExFlow);
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
    when(this.executorManagerAdapter.createExecutableFlow(any(), any())).thenReturn(this.retriedExFlow);
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
    when(this.executorManagerAdapter.createExecutableFlow(any(), any())).thenReturn(this.retriedExFlow);
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
    when(this.executorManagerAdapter.createExecutableFlow(any(), any())).thenReturn(this.retriedExFlow);
    when(this.executorManagerAdapter.submitExecutableFlow(any(), any())).thenReturn("");
    doThrow(new ExecutorManagerException("ops")).when(this.executorLoader).updateExecutableFlow(any());

    // trigger
    this.onContainerizedExecutionEventListener.restartExecutableFlow(this.originalExFlow);
  }
}