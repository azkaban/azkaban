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
  public OnContainerizedExecutionEventListener(ExecutorLoader executorLoader,
      ExecutorManagerAdapter executorManagerAdapter, ProjectManager projectManager) {
    this.executorLoader = executorLoader;
    this.executorManagerAdapter = executorManagerAdapter;
    this.projectManager = projectManager;
  }

  @Override
  public void onExecutionEvent(final ExecutableFlow flow, final String action) {
    if (action.equals("Restart Flow")) {
      if (restartExecutableFlow(flow)) {
        // Update the flow options so that the flow will be not retried by Azkaban
        final ExecutionOptions options = flow.getExecutionOptions();
        options.setExecutionRetried(true);
        flow.setExecutionOptions(options);
        try {
          executorLoader.updateExecutableFlow(flow);
        } catch (ExecutorManagerException e) {
          logger.error("Unable to update flow retry value: " + e.getMessage());
        }
      }
    }
  }

  /**
   * A new execution will be dispatched based on the original ExecutableFLow
   * @param exFlow
   */
  public boolean restartExecutableFlow(final ExecutableFlow exFlow) {
    if (exFlow.getDispatchMethod() == DispatchMethod.CONTAINERIZED) { // Enable restartability
      // for containerized execution
      final Project project;
      final Flow flow;
      try {
        project = FlowUtils.getProject(projectManager, exFlow.getProjectId());
        flow = FlowUtils.getFlow(project, exFlow.getFlowId());
      } catch (final RuntimeException e) {
        logger.error(e.getMessage());
        return false;
      }
      final ExecutableFlow executableFlow = FlowUtils.createExecutableFlow(project, flow);
      executableFlow.setSubmitUser(exFlow.getSubmitUser());
      executableFlow.setExecutionSource(Constants.EXECUTION_SOURCE_ADHOC);

      final ExecutionOptions options = exFlow.getExecutionOptions();
      if(!options.isFailureEmailsOverridden()) {
        options.setFailureEmails(flow.getFailureEmails());
      }
      if (!options.isSuccessEmailsOverridden()) {
        options.setSuccessEmails(flow.getSuccessEmails());
      }
      options.setMailCreator(flow.getMailCreator());
      executableFlow.setExecutionOptions(options);
      try {
        logger.info("Restarting flow " + project.getName() + "." + executableFlow.getFlowName());
        executorManagerAdapter.submitExecutableFlow(executableFlow, executableFlow.getSubmitUser());
      } catch (final ExecutorManagerException e) {
        logger.error("Failed to restart flow "+ executableFlow.getFlowId() + ". " + e.getMessage());
        return false;
      }
      return true;
    }
    return false;
  }
}
