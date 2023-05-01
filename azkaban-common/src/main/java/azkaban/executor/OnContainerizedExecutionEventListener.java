package azkaban.executor;

import static azkaban.Constants.FlowParameters.FLOW_PARAM_RESTART_STRATEGY;

import azkaban.Constants;
import azkaban.Constants.FlowRetryStrategy;
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
    ExecutableFlow retryExFlow =
        this.executorManagerAdapter.createExecutableFlow(project, flow);

    final ExecutionOptions options = originalExFlow.getExecutionOptions();

    final String restartStrategy = options.getFlowParameters().
        getOrDefault(FLOW_PARAM_RESTART_STRATEGY, FlowRetryStrategy.DEFAULT.name()).trim();
    logger.info(String.format("Retry execution of exec Id %d is set to use %s strategy.",
        originalExFlow.getExecutionId(), restartStrategy));

    // default strategy - not applying anything on the new one, so as like a new execution
    if (restartStrategy.isEmpty() || restartStrategy.equals(FlowRetryStrategy.DEFAULT.name())) {
      logger.info(String.format("Use default strategy when restarting the execution %s",
          originalExFlow.getExecutionId()));
    } else {
      // non-default strategies
      if (restartStrategy.equals(FlowRetryStrategy.DISABLE_SUCCEEDED_NODES.name())){
        try {
          disableSucceededSkippedJobsInRetryFlow(originalExFlow, retryExFlow);
        } catch (ExecutorManagerException e){
          logger.error(String.format(
              "Fail to restart execution %s due to error applying %s restart-strategy",
                  originalExFlow.getExecutionId(),
                  FlowRetryStrategy.DISABLE_SUCCEEDED_NODES.name()),
              e);
        }
      }
    }

    retryExFlow.setSubmitUser(originalExFlow.getSubmitUser());
    retryExFlow.setExecutionSource(Constants.EXECUTION_SOURCE_RETRY);
    retryExFlow.setUploadUser(project.getUploadUser());
    // Set up flow ExecutionOptions
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

    try {
      originalExFlow.setFlowRetryChildExecutionID(retryExFlow.getExecutionId());
      this.executorLoader.updateExecutableFlow(originalExFlow);
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to update the original flow after restart"
          + originalExFlow.getFlowId() + ". " + e.getMessage());
    }

    // TODO: consider send out email for this information
    // update the original executable-flow with retry-count and child executionID
    logger.info(String.format("Retry execution [%d] successfully, "
            + "spawning child-execution [%d], and its root-execution was [%d];"
            + "system-defined retry count=%d, user-defined retry-count=%d.",
        originalExFlow.getExecutionId(), retryExFlow.getExecutionId(),
        retryExFlow.getFlowRetryRootExecutionID(),
        retryExFlow.getSystemDefinedRetryCount(), retryExFlow.getUserDefinedRetryCount()));
  }


  /**
   * DFS walk through of the executableFlow, update the retry-flow's job status to disabled if
   * the corresponding job status in origin-flow is considered success/skip/disable.
   */
  public static void disableSucceededSkippedJobsInRetryFlow(
      final ExecutableNode originalFlow,
      final ExecutableNode retryingFlow) throws ExecutorManagerException {
    // validate the 2 input nodes are matching
    if (originalFlow == null && retryingFlow == null){
      return;
    }
    if ((originalFlow == null) != (retryingFlow == null)){
      throw new ExecutorManagerException(
          String.format("Null check failed: input Original flow node = %s, Retrying flow node %s",
              originalFlow, retryingFlow));
    }
    if (!originalFlow.getId().equals(retryingFlow.getId())){
      throw new ExecutorManagerException(
          String.format("Input Original flow node ID %s != Retrying flow node ID %s",
              originalFlow.getId(), retryingFlow.getId()));
    }
    if ((originalFlow instanceof ExecutableFlowBase)
        != (retryingFlow instanceof ExecutableFlowBase)) {
      throw new ExecutorManagerException(
          String.format("Input Original flow node %s is `ExecutableFlowBase` "
                  + "but Retrying flow node %s is not",
              originalFlow.getId(), retryingFlow.getId()));
    }

    // recursively walk through the children if is a "flow or subflow"
    if (originalFlow instanceof ExecutableFlowBase){
      final ExecutableFlowBase originBase = (ExecutableFlowBase) originalFlow;
      final ExecutableFlowBase retryBase = (ExecutableFlowBase) retryingFlow;
      for (ExecutableNode subNode : originBase.getExecutableNodes()) {
        disableSucceededSkippedJobsInRetryFlow(
            subNode, retryBase.getExecutableNode(subNode.getId()));
      }
    }
    // set status DISABLED at the end if all children is good
    switch (originalFlow.getStatus()) {
      case SUCCEEDED:
      case FAILED_SUCCEEDED:
      case DISABLED:
        retryingFlow.setStatus(Status.DISABLED);
        break;
      case SKIPPED:
        retryingFlow.setStatus(Status.SKIPPED);
        // fall through
      default:
    }
  }
}
