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
   * @param originalExFlow original ExecutableFlow in EXECUTION_STOPPED state
   */
  void restartExecutableFlow(final ExecutableFlow originalExFlow) {
    // Enable restartability for containerized execution
    if (originalExFlow.getDispatchMethod() != DispatchMethod.CONTAINERIZED) return;

    // Create a new ExecutableFlow based on existing flow in EXECUTION_STOPPED state
    final Project project;
    final Flow flow;
    try {
      project = FlowUtils.getProject(this.projectManager, originalExFlow.getProjectId());
      flow = FlowUtils.getFlow(project, originalExFlow.getFlowId());
    } catch (final RuntimeException e) {
      logger.error(e.getMessage());
      return;
    }
    final ExecutableFlow retryExFlow =
        this.executorManagerAdapter.createExecutableFlow(project, flow);
    retryExFlow.setSubmitUser(originalExFlow.getSubmitUser());
    retryExFlow.setExecutionSource(Constants.EXECUTION_SOURCE_ADHOC);
    retryExFlow.setUploadUser(project.getUploadUser());
    // Set up flow ExecutionOptions
    final ExecutionOptions options = originalExFlow.getExecutionOptions();
    if(!options.isFailureEmailsOverridden()) {
      options.setFailureEmails(flow.getFailureEmails());
    }
    if (!options.isSuccessEmailsOverridden()) {
      options.setSuccessEmails(flow.getSuccessEmails());
    }
    options.setMailCreator(flow.getMailCreator());
    // Update the flow options so that the flow will be not retried again by Azkaban

    // inherent the retry time counters
    retryExFlow.setUserDefinedRetryCount(originalExFlow.getUserDefinedRetryCount());
    retryExFlow.setSystemDefinedRetryCount(originalExFlow.getSystemDefinedRetryCount());

    if (originalExFlow.getFlowRetryRootExecutionID() > 0) {
      retryExFlow.setFlowRetryRootExecutionID(originalExFlow.getFlowRetryRootExecutionID());
    } else {
      retryExFlow.setFlowRetryRootExecutionID(originalExFlow.getExecutionId());
    }
    retryExFlow.setFlowRetryParentExecutionID(originalExFlow.getExecutionId());

    // If a retried flow A gets retried again with a new execution id flow B, the original flow
    // execution id of flow B should be the same as flow A's original flow execution id.
    if (options.getOriginalFlowExecutionIdBeforeRetry() == null) {
      options.setOriginalFlowExecutionIdBeforeRetry(originalExFlow.getExecutionId());
    }
    retryExFlow.setExecutionOptions(options);
    // Submit new flow for execution
    try {
      logger.info("Restarting flow " + project.getName() + "." + retryExFlow.getFlowName());
      this.executorManagerAdapter.submitExecutableFlow(retryExFlow,
          retryExFlow.getSubmitUser());
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to restart flow "+ retryExFlow.getFlowId() + ". " + e.getMessage());
      return;
    }

    // update the original executable-flow with retry-count and child executionID
    try {
      originalExFlow.setFlowRetryChildExecutionID(retryExFlow.getExecutionId());
      this.executorLoader.updateExecutableFlow(originalExFlow);
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to update the original flow after restart"
          + originalExFlow.getFlowId() + ". " + e.getMessage());
    }
    // TODO: consider send out email for this information
    logger.info(String.format("Retry execution [%d] successfully, "
            + "spawning child-execution [%d], and its root-execution was [%d]",
        originalExFlow.getExecutionId(), retryExFlow.getExecutionId(),
        retryExFlow.getFlowRetryRootExecutionID()));
  }
}
