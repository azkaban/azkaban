package azkaban.executor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.Constants.FlowParameters;
import azkaban.DispatchMethod;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.DummyContainerizationMetricsImpl;
import azkaban.metrics.MetricsManager;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.codahale.metrics.MetricRegistry;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class ExecutionControllerUtilsRestartFlowTest {
  private Project project;
  private Props props;
  private Flow flow;
  private ExecutableFlow flow1;
  private MockExecutorLoader executorLoader;
  private User user;
  private ContainerizedDispatchManager containerizedDispatchManager;
  private final CommonMetrics commonMetrics = new CommonMetrics(
      new MetricsManager(new MetricRegistry()));
  private ProjectManager projectManager;
  private static final int executionId = 111;
  private static final int projectId = 1;
  private OnExecutionEventListener listener;

  @Before
  public void setup() throws Exception {
    // Set up project and flow
    this.project = new Project(projectId, "testProject");
    this.props = new Props();
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(this.props);
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"));
    this.project.setFlows(loader.getFlowMap());
    this.project.setVersion(1);
    this.flow = FlowUtils.getFlow(project, "jobe");
    this.flow1 = FlowUtils.createExecutableFlow(project, flow);
    final ExecutionOptions executionOptions = new ExecutionOptions();
    final Map<String, String> flowParam = new HashMap<>();
    flowParam.put(FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED, "true");
    flowParam.put(FlowParameters.FLOW_PARAM_DISPATCH_EXECUTION_TO_CONTAINER, "true");
    executionOptions.addAllFlowParameters(flowParam);
    flow1.setExecutionOptions(executionOptions);
    flow1.setDispatchMethod(DispatchMethod.CONTAINERIZED);
    flow1.setExecutionId(executionId);
    this.user = TestUtils.getTestUser();
    flow1.setSubmitUser(user.getUserId());

    this.executorLoader = new MockExecutorLoader();
    executorLoader.uploadExecutableFlow(flow1);

    this.containerizedDispatchManager = new ContainerizedDispatchManager(this.props,
        this.executorLoader, this.commonMetrics, mock(ExecutorApiGateway.class),
        mock(ContainerizedImpl.class),null, null,
        new DummyEventListener(), new DummyContainerizationMetricsImpl());
    this.projectManager = mock(ProjectManager.class);
    when(projectManager.getProject(projectId)).thenReturn(project);

    this.listener = new OnContainerizedExecutionEventListener(executorLoader,
        containerizedDispatchManager, projectManager);
    ExecutionControllerUtils.onExecutionEventListener = listener;
  }

  @Test
  public void testRestartOnExecutionStopped() throws Exception {
    this.flow1.setStatus(Status.EXECUTION_STOPPED);

    ExecutionControllerUtils.restartFlow(flow1);

    final ExecutableFlow restartedExFlow = this.executorLoader.fetchExecutableFlow(-1);
    assertTrue(restartedExFlow.getFlowId().equals(flow1.getFlowId()));
    assertTrue(restartedExFlow.getProjectName().equals(project.getName()));
    assertTrue(restartedExFlow.getSubmitUser().equals(flow1.getSubmitUser()));
    assertTrue(restartedExFlow.getDispatchMethod().equals(DispatchMethod.CONTAINERIZED));

    final ExecutionOptions options1 = flow1.getExecutionOptions();
    assertTrue(options1.isExecutionRetried());
    final ExecutableFlow flow2 = executorLoader.fetchExecutableFlow(executionId);
    final ExecutionOptions options2 = flow1.getExecutionOptions();
    assertTrue(options2.isExecutionRetried());
  }
}
