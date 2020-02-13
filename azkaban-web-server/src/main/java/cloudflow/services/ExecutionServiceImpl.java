package cloudflow.services;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionAttempt;
import azkaban.executor.ExecutionFlowDao;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.CommonJobProperties;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import cloudflow.error.CloudFlowException;
import cloudflow.error.CloudFlowNotFoundException;
import cloudflow.error.CloudFlowNotImplementedException;
import cloudflow.models.ExecutionBasicResponse;
import cloudflow.error.CloudFlowValidationException;
import cloudflow.models.JobExecution;
import cloudflow.models.JobExecutionAttempt;
import cloudflow.servlets.Constants;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static cloudflow.servlets.Constants.FLOW_VERSION_KEY;
import static java.lang.String.*;
import static java.util.Objects.*;

public class ExecutionServiceImpl implements ExecutionService {

  private static final String FLOW_ID_PARAM = "flow_id";
  private static final String FLOW_VERSION_PARAM = "flow_version";
  private static final String PROJECT_ID_PARAM = "project_id";
  private static final String EXPERIMENT_ID_PARAM = "experiment_id";

  private static final Logger logger = LoggerFactory.getLogger(ExecutionServiceImpl.class);
  private ExecutorManagerAdapter executorManager;
  private ExecutionFlowDao executionFlowDao;
  private ProjectManager projectManager;

  @Inject
  public ExecutionServiceImpl(ExecutorManagerAdapter executorManager,
      ExecutionFlowDao executionFlowDao, ProjectManager projectManager) {
    this.executorManager = executorManager;
    this.executionFlowDao = executionFlowDao;
    this.projectManager = projectManager;
  }

  private String extractArgumentValue(String argName, String[] argValues) {
    requireNonNull(argName);
    requireNonNull(argValues);
    if (argValues.length != 1) {
      throw new RuntimeException(format("Argument %s should have exactly one value", argName));
    }
    return argValues[0];
  }

  private ExecutionBasicResponse executionResponseFromFlow(ExecutableFlow executableFlow) {
    return new ExecutionBasicResponse(executableFlow);
  }

  @Override
  public List<ExecutionBasicResponse> getAllExecutions(Map<String, String[]> queryParamMap) {
    requireNonNull(queryParamMap, "query param map is null");

    // Currently we support only one value for each of the parameters and return an error otherwise.
    Optional<String> flowId = Optional.empty();
    Optional<String> flowVersion = Optional.empty();
    Optional<String> experimentId = Optional.empty();
    Optional<String> projectId = Optional.empty();

    for (Map.Entry<String, String[]> entry : queryParamMap.entrySet()) {
      String key = entry.getKey();
      String[] value = entry.getValue();
      if (key.equals(FLOW_ID_PARAM)) {
        flowId = Optional.of(extractArgumentValue(key, value));
      } else if (key.equals(FLOW_VERSION_PARAM)) {
        flowVersion = Optional.of(extractArgumentValue(key, value));
      } else if (key.equals(EXPERIMENT_ID_PARAM)) {
        experimentId = Optional.of(extractArgumentValue(key, value));
      } else if (key.equals(PROJECT_ID_PARAM)) {
        projectId = Optional.of(extractArgumentValue(key, value));
      }
    }

    //todo(sshardool): enable filtering by these fields once they are in the db schema
    if (experimentId.isPresent() || flowVersion.isPresent()) {
      throw new CloudFlowException("Filtering by experiment and flow version is currently not "
          + "supported");
    }

    List<ExecutableFlow> executableFlows;
    try {
      executableFlows = executionFlowDao
          .fetchFlowHistory(projectId, flowId, flowVersion, experimentId);
    } catch (ExecutorManagerException e) {
      String errorMessage = "Error fetching executions";
      logger.error(errorMessage, e);
      throw new CloudFlowException(errorMessage, e);
    }
    return executableFlows.stream().map(this::executionResponseFromFlow)
        .collect(Collectors.toList());
  }

  @Override
  public JobExecution getJobExecution(String executionId, String jobDefinitionId, String user)
      throws CloudFlowException {
    requireNonNull(executionId, "execution id is null");
    requireNonNull(jobDefinitionId, "job definition id is null");

    // TODO ypadron: check user permissions

    final Integer execId = Integer.parseInt(executionId); // Azkaban ids are integers
    String errorMessage;
    ExecutableFlow executableFlow;
    try {
      executableFlow = this.executorManager.getExecutableFlow(execId);
    } catch (final ExecutorManagerException e) {
      errorMessage = String.format("Failed to fetch execution with id %d.", execId);
      logger.error(errorMessage, e);
      throw new CloudFlowException(errorMessage);
    }

    if (executableFlow == null) {
      errorMessage = String.format("Execution with id %d wasn't found.", execId);
      logger.error(errorMessage);
      throw new CloudFlowNotFoundException(errorMessage);
    }

    // TODO ypadron: get job and job path given a definition id
    // Examples of job paths in current Azkaban code:
    // 1) jobD -> direct child of the root flow
    // 2) embeddedFlow1:embeddedFlow2:jobE -> deeply nested job
    String jobPath = jobDefinitionId;
    String[] nodesInPath = jobPath.split(":");
    List<ExecutableNode> nodesToScan = executableFlow.getExecutableNodes();
    ExecutableNode node = null;
    for (String nodeId : nodesInPath) {
      int i = 0;
      for (; i < nodesToScan.size(); i++) {
        ExecutableNode en = nodesToScan.get(i);
        if (en.getId().equals(nodeId)) {
          node = en;
          break;
        }
      }
      if (i >= nodesToScan.size()) {
        errorMessage = String.format("Job with id %s and path %s wasn't found in execution %d.",
            jobDefinitionId, jobPath, execId);
        logger.error(errorMessage);
        throw new CloudFlowNotFoundException(errorMessage);
      }
      if (node instanceof ExecutableFlowBase) {
        nodesToScan = ((ExecutableFlowBase) node).getExecutableNodes();
      } else {
        // If we find a job before the end of the path this ensures the next element in
        // the path will not be found in the next iteration. This has no effect with the
        // last node in the path.
        nodesToScan = new ArrayList<>();
      }
    }
    logger.info("Found job with path {} in execution {}.", jobPath, execId);

    JobExecution jobExecution = new JobExecution();
    List<JobExecutionAttempt> attempts = getJobExecutionAttempts(node);
    jobExecution.setAttempts(attempts);

    Optional<JobExecutionAttempt> firstAttempt = attempts.stream().filter(a -> a.getId().equals(0))
        .findFirst();
    Long firstStartTime =
        firstAttempt.isPresent() ? firstAttempt.get().getStartTime() : node.getStartTime();
    jobExecution.setStartTime(firstStartTime);

    jobExecution.setExecutionId(executionId);
    jobExecution.setEndTime(node.getEndTime());
    jobExecution.setStatus(node.getStatus());
    // TODO: set data from job definition
    // TODO: set job properties

    return jobExecution;
  }

  private List<JobExecutionAttempt> getJobExecutionAttempts(ExecutableNode jobNode) {
    List<JobExecutionAttempt> attempts = new ArrayList<>();
    for (Object o : jobNode.getAttemptObjects()) {
      Map<String, Object> attempt = (Map<String, Object>) o;
      Integer id = (Integer) attempt.get(ExecutionAttempt.ATTEMPT_PARAM);
      Long startTime = (Long) attempt.get(ExecutionAttempt.STARTTIME_PARAM);
      Long endTime = (Long) attempt.get(ExecutionAttempt.ENDTIME_PARAM);
      Status status = Status.valueOf((String) attempt.get(ExecutionAttempt.STATUS_PARAM));
      attempts.add(new JobExecutionAttempt(id, startTime, endTime, status));
    }

    JobExecutionAttempt lastAttempt = new JobExecutionAttempt(attempts.size(),
        jobNode.getStartTime(), jobNode.getEndTime(), jobNode.getStatus());
    attempts.add(lastAttempt);
    return attempts;
  }

  @Override
  public String createExecution(ExecutionParameters executionParameters) throws CloudFlowException {
    requireNonNull(executionParameters);
    // TODO ypadron: check user permissions

    // TODO ypadron: obtain flow and project dynamically
    Project project = this.projectManager.getProject("yb-test");
    Flow flow = project.getFlow("flowPriority2");
    final ExecutableFlow exflow = FlowUtils.createExecutableFlow(project, flow);

    setAzkabanExecutionOptions(executionParameters, exflow);

    try {
      this.executorManager.submitExecutableFlow(exflow, executionParameters.getSubmitUser());
    } catch (ExecutorManagerException e) {
      if (e.getReason() != null) {
        throw new CloudFlowValidationException(e.getMessage());
      } else {
        final String errorMessage = String.format("Failed to create execution of flow with id %s "
            + "and version %s.", executionParameters.getFlowId(),
            executionParameters.getFlowVersion());
        logger.error(errorMessage, e);
        throw new CloudFlowException(errorMessage);
      }
    }
    return String.valueOf(exflow.getExecutionId());
  }

  private void setAzkabanExecutionOptions(ExecutionParameters executionParameters,
      ExecutableFlow executableFlow) throws CloudFlowException {

    try {
      executableFlow.setFlowDefinitionId(Integer.parseInt(executionParameters.getFlowId()));
    } catch (NumberFormatException e) {
      throw new CloudFlowValidationException(String.format("Parameter '%s' must be an integer.",
          Constants.FLOW_ID_KEY));
    }

    // TODO ypadron: validate existence of flow version
    if(executionParameters.getFlowVersion() <= 0) {
      throw new CloudFlowValidationException(
          String.format("Parameter '%s' must be a positive number.", FLOW_VERSION_KEY));
    }
    executableFlow.setFlowVersion(executionParameters.getFlowVersion());
    executableFlow.setDescription(executionParameters.getDescription());
    if (!executionParameters.getExperimentId().isEmpty()) {
      try {
        executableFlow.setExperimentId(Integer.parseInt(executionParameters.getExperimentId()));
        // TODO ypadron: validate existence of experiment id
      } catch (NumberFormatException e) {
        throw new CloudFlowValidationException(String.format("Parameter '%s' must be an integer.",
            Constants.EXPERIMENT_ID_KEY));
      }
    }
    executableFlow.setSubmitUser(executionParameters.getSubmitUser());

    ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setFailureAction(ExecutionOptions.FailureAction.valueOf(
        executionParameters.getFailureAction().toString()));

    executionOptions.setNotifyOnFirstFailure(executionParameters.isNotifyOnFirstFailure());
    executionOptions.setNotifyOnLastFailure(executionParameters.isNotifyFailureOnExecutionComplete());

    executionOptions.setConcurrentOption(executionParameters.getConcurrentOption().getName());
    setRuntimeProperties(executionParameters, executionOptions);

    executableFlow.setExecutionOptions(executionOptions);
  }

  private void setRuntimeProperties(ExecutionParameters executionParameters,
      ExecutionOptions executionOptions) {

    if (executionParameters.getProperties().size() == 0) {
      return;
    }
    // supporting only flow level properties overwrite for the POC
    if(executionParameters.getProperties().size() > 1 ||
        !executionParameters.getProperties().containsKey("root")) {
      logger.error("Attempt to overwrite properties of nodes other than the root flow detected: "
          + executionParameters.getProperties());
      throw new CloudFlowNotImplementedException("Overwriting properties of jobs or nested flows is not "
          + "yet supported.");
    }

    Map<String, String> runtimeProps = executionParameters.getProperties().get("root");
    if(runtimeProps.containsKey(CommonJobProperties.FAILURE_EMAILS)) {
      String rawEmails = runtimeProps.get(CommonJobProperties.FAILURE_EMAILS);
      if (!rawEmails.isEmpty()) {
        String[] emails = rawEmails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
        executionOptions.setFailureEmails(Arrays.asList(emails));
        executionOptions.setFailureEmailsOverridden(true);
      }
      runtimeProps.remove(CommonJobProperties.FAILURE_EMAILS);
    }

    if(runtimeProps.containsKey(CommonJobProperties.SUCCESS_EMAILS)) {
      String rawEmails = runtimeProps.get(CommonJobProperties.SUCCESS_EMAILS);
      if (!rawEmails.isEmpty()) {
        String[] emails = rawEmails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
        executionOptions.setSuccessEmails((Arrays.asList(emails)));
        executionOptions.setSuccessEmailsOverridden(true);
      }
      runtimeProps.remove(CommonJobProperties.SUCCESS_EMAILS);
    }
    executionOptions.addAllFlowParameters(runtimeProps);
  }

}
