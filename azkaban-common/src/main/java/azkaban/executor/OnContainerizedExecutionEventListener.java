package azkaban.executor;

import azkaban.Constants;
import azkaban.DispatchMethod;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class OnContainerizedExecutionEventListener implements OnExecutionEventListener{

  private static final Logger logger =
      LoggerFactory.getLogger(OnContainerizedExecutionEventListener.class);
  private final ExecutorLoader executorLoader;
  private final ExecutorManagerAdapter executorManagerAdapter;
  private final ProjectManager projectManager;

  @Inject
  public OnContainerizedExecutionEventListener(final ExecutorLoader executorLoader,
      ExecutorManagerAdapter executorManagerAdapter, ProjectManager projectManager) {
    this.executorLoader = executorLoader;
    this.executorManagerAdapter = executorManagerAdapter;
    this.projectManager = projectManager;
  }

  @Override
  public void onExecutionEvent(final ExecutableFlow flow, final String action) {
    if (action.equals(Constants.RESTART_FLOW)) {
      restartExecutableFlow(flow);
    }
  }

  /**
   * A new execution will be dispatched based on the original ExecutableFLow
   * @param exFlow original ExecutableFlow in EXECUTION_STOPPED state
   */
  private void restartExecutableFlow(final ExecutableFlow exFlow) {
    // Enable restartability for containerized execution
    if (exFlow.getDispatchMethod() != DispatchMethod.CONTAINERIZED) return;

    // Create a new ExecutableFlow based on existing flow in EXECUTION_STOPPED state
    final Project project;
    final Flow flow;
    try {
      project = FlowUtils.getProject(this.projectManager, exFlow.getProjectId());
      flow = FlowUtils.getFlow(project, exFlow.getFlowId());
    } catch (final RuntimeException e) {
      logger.error(e.getMessage());
      return;
    }
    final ExecutableFlow executableFlow = FlowUtils.createExecutableFlow(project, flow);
    executableFlow.setSubmitUser(exFlow.getSubmitUser());
    executableFlow.setExecutionSource(Constants.EXECUTION_SOURCE_ADHOC);
    executableFlow.setUploadUser(project.getUploadUser());
    // Set up flow ExecutionOptions
    final ExecutionOptions options = exFlow.getExecutionOptions();
    if(!options.isFailureEmailsOverridden()) {
      options.setFailureEmails(flow.getFailureEmails());
    }
    if (!options.isSuccessEmailsOverridden()) {
      options.setSuccessEmails(flow.getSuccessEmails());
    }
    options.setMailCreator(flow.getMailCreator());
    // Update the flow options so that the flow will be not retried again by Azkaban
    options.setExecutionRetried(true);
    // If a retried flow A gets retried again with a new execution id flow B, the original flow
    // execution id of flow B should be the same as flow A's original flow execution id.
    if (options.getOriginalFlowExecutionIdBeforeRetry() == null) {
      options.setOriginalFlowExecutionIdBeforeRetry(exFlow.getExecutionId());
    }
    executableFlow.setExecutionOptions(options);
    // Submit new flow for execution
    try {
      logger.info("Restarting flow " + project.getName() + "." + executableFlow.getFlowName());
      this.executorManagerAdapter.submitExecutableFlow(executableFlow,
          executableFlow.getSubmitUser());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to restart flow "+ executableFlow.getFlowId() + ". " + e.getMessage());
    }
  }
}
