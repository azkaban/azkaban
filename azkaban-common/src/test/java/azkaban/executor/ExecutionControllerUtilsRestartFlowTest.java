package azkaban.executor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.Constants.FlowParameters;
import azkaban.DispatchMethod;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.logs.ExecutionLogsLoader;
import azkaban.logs.MockExecutionLogsLoader;
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
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class ExecutionControllerUtilsRestartFlowTest {
  private Project project;
  private Props props;
  private Flow flow;
  private ExecutableFlow flow1;
  private ExecutorLoader executorLoader;
  private ExecutionLogsLoader nearlineExecutionLogsLoader;
  private ExecutionLogsLoader offlineExecutionLogsLoader;
  private User user;
  private ContainerizedDispatchManager containerizedDispatchManager;
  private final CommonMetrics commonMetrics = new CommonMetrics(
      new MetricsManager(new MetricRegistry()));
  private ProjectManager projectManager;
  private static final int executionId = 111;
  private static final int projectId = 1;
  private OnExecutionEventListener listener;

  public void setup() throws Exception {
    // Set up project and flow
    this.project = new Project(projectId, "testProject");
    this.props = new Props();
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(this.props);
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"));
    this.project.setFlows(loader.getFlowMap());
    this.project.setVersion(1);
    this.flow = FlowUtils.getFlow(this.project, "jobe");
    this.flow1 = FlowUtils.createExecutableFlow(this.project, this.flow);
    final ExecutionOptions executionOptions = new ExecutionOptions();
    final Map<String, String> flowParam = new HashMap<>();
    flowParam.put(FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED");
    flowParam.put(FlowParameters.FLOW_PARAM_DISPATCH_EXECUTION_TO_CONTAINER, "true");
    executionOptions.addAllFlowParameters(flowParam);
    this.flow1.setExecutionOptions(executionOptions);
    this.flow1.setDispatchMethod(DispatchMethod.CONTAINERIZED);
    this.flow1.setExecutionId(executionId);
    this.user = TestUtils.getTestUser();
    this.flow1.setSubmitUser(this.user.getUserId());

    this.executorLoader = new MockExecutorLoader();
    this.executorLoader.uploadExecutableFlow(this.flow1);
    this.nearlineExecutionLogsLoader = new MockExecutionLogsLoader();

    this.containerizedDispatchManager = new ContainerizedDispatchManager(this.props, null,
        this.executorLoader, this.nearlineExecutionLogsLoader, this.offlineExecutionLogsLoader,
        this.commonMetrics, mock(ExecutorApiGateway.class), mock(ContainerizedImpl.class),null,
        null, new DummyEventListener(), new DummyContainerizationMetricsImpl(), null);
    this.projectManager = mock(ProjectManager.class);
    when(this.projectManager.getProject(projectId)).thenReturn(this.project);

    this.listener = new OnContainerizedExecutionEventListener(this.executorLoader,
        this.containerizedDispatchManager, this.projectManager);
    ExecutionControllerUtils.onExecutionEventListener = this.listener;
  }

  public void testRestartOnExecutionStopped() throws Exception {
    this.flow1.setStatus(Status.EXECUTION_STOPPED);

    ExecutionControllerUtils.restartFlow(this.flow1);

    final ExecutableFlow restartedExFlow = this.executorLoader.fetchExecutableFlow(-1);
    assertTrue(restartedExFlow.getFlowId().equals(this.flow1.getFlowId()));
    assertTrue(restartedExFlow.getProjectName().equals(this.project.getName()));
    assertTrue(restartedExFlow.getSubmitUser().equals(this.flow1.getSubmitUser()));
    assertTrue(restartedExFlow.getDispatchMethod().equals(DispatchMethod.CONTAINERIZED));

    final ExecutionOptions options1 = this.flow1.getExecutionOptions();
    assertTrue(options1.isExecutionRetried());
    final ExecutableFlow flow2 = this.executorLoader.fetchExecutableFlow(executionId);
    final ExecutionOptions options2 = this.flow1.getExecutionOptions();
    assertTrue(options2.isExecutionRetried());
  }
}
