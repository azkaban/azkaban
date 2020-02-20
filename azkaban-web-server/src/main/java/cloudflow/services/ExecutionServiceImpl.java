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
import cloudflow.services.ExecutionParameters.ConcurrentOption;
import cloudflow.services.ExecutionParameters.FailureAction;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.*;
import static java.util.Objects.*;

public class ExecutionServiceImpl implements ExecutionService {

  private static final String FLOW_ID_PARAM = "flow_id";
  private static final String FLOW_VERSION_PARAM = "flow_version";
  private static final String PROJECT_ID_PARAM = "project_id";
  private static final String EXPERIMENT_ID_PARAM = "experiment_id";

  private static final String RUNTIME_PROPERTIES_ROOT_FLOW_KEYWORD = "root";

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
  public JobExecution getJobExecution(String executionId, String jobDefinitionId, String user) {
    requireNonNull(executionId, "execution id is null");
    requireNonNull(jobDefinitionId, "job definition id is null");

    // TODO ypadron: check user permissions

    final Integer execId = Integer.parseInt(executionId); // Azkaban ids are integers
    String errorMessage;
    ExecutableFlow executableFlow;
    try {
      executableFlow = this.executorManager.getExecutableFlow(execId);
    } catch (final ExecutorManagerException e) {
      errorMessage = format("Failed to fetch execution with id %d.", execId);
      logger.error(errorMessage, e);
      throw new CloudFlowException(errorMessage);
    }

    if (executableFlow == null) {
      errorMessage = format("Execution with id %d wasn't found.", execId);
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
        errorMessage = format("Job with id %s and path %s wasn't found in execution %d.",
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
  public String createExecution(ExecutionParameters executionParameters) {
    requireNonNull(executionParameters);
    // TODO ypadron: check user permissions
    ExecutableFlow execFlow = getExecutableFlow(
        validateFlowDefinitionId(executionParameters.getFlowId()));
    setAzkabanExecutionParameters(executionParameters, execFlow);
    return submitExecutableFlow(execFlow);
  }

  @Override
  public String createRerunExecution(ExecutionParameters executionParameters) {
    requireNonNull(executionParameters);
    // TODO ypadron: check user permissions
    ExecutableFlow execFlow = createExecutableFlowFromPreviousExecutionId(
        validatePreviousExecutionId(executionParameters.getPreviousFlowExecutionId()));
    updateAzkabanExecutionParameters(executionParameters, execFlow);
    return submitExecutableFlow(execFlow);
  }

  private String submitExecutableFlow(ExecutableFlow execFlow) {
    try {
      this.executorManager.submitExecutableFlow(execFlow, execFlow.getSubmitUser());
    } catch (ExecutorManagerException e) {
      if (e.getReason() != null) {
        throw new CloudFlowValidationException(e.getMessage());
      } else {
        final String errorMessage = format("Failed to create execution of flow with id %s "
                + "and version %s.", execFlow.getFlowDefinitionId(), execFlow.getFlowVersion());
        logger.error(errorMessage, e);
        throw new CloudFlowException(errorMessage);
      }
    }
    return String.valueOf(execFlow.getExecutionId());
  }

  private void updateAzkabanExecutionParameters(ExecutionParameters executionParameters,
      ExecutableFlow executableFlow) {
    requireNonNull(executableFlow.getFlowDefinitionId());
    requireNonNull(executableFlow.getFlowVersion());
    if (executionParameters.getFlowVersion() != null) {
      // flow version from params and flow version of previous execution should be compatible
      executableFlow.setFlowVersion(validateFlowVersion(executionParameters.getFlowVersion()));
    }

    if (executionParameters.getDescription() != null) {
      executableFlow.setDescription(executionParameters.getDescription());
    }
    if (executionParameters.getExperimentId() != null) {
      executableFlow.setExperimentId(validateExperimentId(executionParameters.getExperimentId()));
    }

    executableFlow.setSubmitUser(executionParameters.getSubmitUser());

    ExecutionOptions azExecutionOptions = executableFlow.getExecutionOptions();
    if (executionParameters.getFailureAction() != null) {
      azExecutionOptions.setFailureAction(ExecutionOptions.FailureAction.valueOf(
          executionParameters.getFailureAction().toString()));
    }
    if (executionParameters.isNotifyOnFirstFailure() != null) {
      azExecutionOptions.setNotifyOnFirstFailure(executionParameters.isNotifyOnFirstFailure());
    }
    if (executionParameters.isNotifyFailureOnExecutionComplete() != null) {
      azExecutionOptions.setNotifyOnLastFailure(executionParameters.isNotifyFailureOnExecutionComplete());
    }
    if (executionParameters.getConcurrentOption() != null) {
      azExecutionOptions.setConcurrentOption(executionParameters.getConcurrentOption().getName());
    }

    setRuntimeProperties(executionParameters, executableFlow.getExecutionOptions());
  }

  private void setAzkabanExecutionParameters(ExecutionParameters executionParameters,
      ExecutableFlow executableFlow) {

    executableFlow.setFlowVersion(validateFlowVersion(executionParameters.getFlowVersion()));

    String descriptionParam = executionParameters.getDescription();
    if (descriptionParam != null) {
      executableFlow.setDescription(descriptionParam);
    }

    executableFlow.setExperimentId(validateExperimentId(executionParameters.getExperimentId()));
    executableFlow.setSubmitUser(executionParameters.getSubmitUser());

    ExecutionOptions azExecutionOptions = new ExecutionOptions();
    FailureAction failureAction = FailureAction.FINISH_CURRENTLY_RUNNING;
    if (executionParameters.getFailureAction() != null) {
      failureAction = executionParameters.getFailureAction();
    }
    azExecutionOptions.setFailureAction(ExecutionOptions.FailureAction.valueOf(
        failureAction.toString()));

    boolean notifyOnFirstFailure = true;
    if (executionParameters.isNotifyOnFirstFailure() != null &&
        !executionParameters.isNotifyOnFirstFailure()) {
      notifyOnFirstFailure = false;
    }
    azExecutionOptions.setNotifyOnFirstFailure(notifyOnFirstFailure);

    boolean notifyFailureOnExecutionComplete = false;
    if (executionParameters.isNotifyFailureOnExecutionComplete() != null &&
        executionParameters.isNotifyFailureOnExecutionComplete()) {
      notifyFailureOnExecutionComplete = true;
    }
    azExecutionOptions.setNotifyOnLastFailure(notifyFailureOnExecutionComplete);

    ConcurrentOption concurrentOption = ConcurrentOption.CONCURRENT_OPTION_SKIP;
    if (executionParameters.getConcurrentOption() != null) {
      concurrentOption = executionParameters.getConcurrentOption();
    }
    azExecutionOptions.setConcurrentOption(concurrentOption.getName());

    setRuntimeProperties(executionParameters, azExecutionOptions);

    executableFlow.setExecutionOptions(azExecutionOptions);
  }

  private int validateFlowDefinitionId(String flowDefinitionId) {
    if (flowDefinitionId == null || flowDefinitionId.isEmpty()) {
      throw new CloudFlowValidationException("Flow id is required.");
    }

    try {
      return Integer.parseInt(flowDefinitionId);
    } catch (NumberFormatException e) {
      throw new CloudFlowValidationException(format("Invalid flow id: %s", flowDefinitionId));
    }
  }

  private int validateFlowVersion(Integer flowVersion) {
    if(flowVersion == null) {
      throw new CloudFlowValidationException("Flow version is required.");
    }
    // TODO ypadron: validate existence of flow version
    return flowVersion;
  }

  private int validatePreviousExecutionId(String previousExecutionId) {
    if (previousExecutionId == null || previousExecutionId.isEmpty()) {
      throw new CloudFlowValidationException("Previous flow execution id is required.");
    }

    try {
      return Integer.parseInt(previousExecutionId);
    } catch (NumberFormatException e) {
      throw new CloudFlowValidationException(
          format("Invalid previous flow execution id: %s", previousExecutionId));
    }
  }

  private int validateExperimentId(String experimentId) {
    if (experimentId != null && !experimentId.isEmpty()) {
      try {
        // TODO ypadron: validate existence of experiment id
        return Integer.parseInt(experimentId);
      } catch (NumberFormatException e) {
        throw new CloudFlowValidationException(format("Invalid experiment id: %s", experimentId));
      }
    }
    return 0;
  }

  private ExecutableFlow getExecutableFlow(int azFlowDefinitionId) {
    // TODO ypadron: obtain flow and project dynamically
    Project project = this.projectManager.getProject("yb-test");
    Flow flow = project.getFlow("flowPriority2");
    ExecutableFlow executableFlow = FlowUtils.createExecutableFlow(project, flow);
    executableFlow.setFlowDefinitionId(azFlowDefinitionId);
    return executableFlow;
  }

  private ExecutableFlow createExecutableFlowFromPreviousExecutionId(int previousExecutionId) {
    ExecutableFlow previousExecFlow;
    try {
      previousExecFlow = this.executorManager.getExecutableFlow(previousExecutionId);

    } catch (ExecutorManagerException e) {
      String errorMessage = format("Failed to fetch execution with id %d.", previousExecutionId);
      logger.error(errorMessage, e);
      throw new CloudFlowException(errorMessage);
    }
    if (previousExecFlow == null) {
      String errorMessage = format("Execution with id %d wasn't found.", previousExecutionId);
      logger.error(errorMessage);
      throw new CloudFlowNotFoundException(errorMessage);
    }

    Project project = this.projectManager.getProject(previousExecFlow.getProjectId());
    Flow flow = project.getFlow(previousExecFlow.getFlowId());
    ExecutableFlow newExecFlow = FlowUtils.createExecutableFlow(project, flow);
    newExecFlow.setFlowDefinitionId(previousExecFlow.getFlowDefinitionId());
    newExecFlow.setFlowVersion(previousExecFlow.getFlowVersion());
    newExecFlow.setDescription(previousExecFlow.getDescription());
    newExecFlow.setExperimentId(previousExecFlow.getExperimentId());
    previousExecFlow.getExecutionOptions().setPreviousExecutionId(previousExecutionId);
    newExecFlow.setExecutionOptions(previousExecFlow.getExecutionOptions());
    return newExecFlow;
  }

  private void setRuntimeProperties(ExecutionParameters executionParameters,
      ExecutionOptions executionOptions) {

    Map<String, Map<String, Object>> propertiesParam = executionParameters.getProperties();
    if (propertiesParam == null || propertiesParam.size() == 0) {
      return;
    }
    // supporting only flow level properties overwrite for the POC
    if(propertiesParam.size() > 1 || !propertiesParam.containsKey(RUNTIME_PROPERTIES_ROOT_FLOW_KEYWORD)) {
      logger.error("Attempt to overwrite properties of nodes other than the root flow detected: "
          + propertiesParam);
      throw new CloudFlowNotImplementedException("Overwriting properties of jobs or nested flows is not "
          + "yet supported.");
    }

    Map<String, Object> runtimeProps = propertiesParam.get(RUNTIME_PROPERTIES_ROOT_FLOW_KEYWORD);
    if(runtimeProps.containsKey(CommonJobProperties.FAILURE_EMAILS)) {
      Object propValue = runtimeProps.get(CommonJobProperties.FAILURE_EMAILS);
      List<String> emails = getNotificationEmails(propValue);
      executionOptions.setFailureEmails(emails);
      executionOptions.setFailureEmailsOverridden(true);
      runtimeProps.remove(CommonJobProperties.FAILURE_EMAILS);
    }

    if(runtimeProps.containsKey(CommonJobProperties.SUCCESS_EMAILS)) {
      Object propValue = runtimeProps.get(CommonJobProperties.SUCCESS_EMAILS);
      List<String> emails = getNotificationEmails(propValue);
      executionOptions.setSuccessEmails(emails);
      executionOptions.setSuccessEmailsOverridden(true);
      runtimeProps.remove(CommonJobProperties.SUCCESS_EMAILS);
    }

    Map<String,String> azRuntimeProps = runtimeProps.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    executionOptions.addAllFlowParameters(azRuntimeProps);
  }

  private List<String> getNotificationEmails(Object propertyValue) {
    String errorMessage = format("Invalid notification email list: %s", propertyValue.toString());
    if (!(propertyValue instanceof List)) {
      logger.error(errorMessage);
      throw new CloudFlowValidationException(errorMessage);
    } else {
      boolean invalid =
          ((List<Object>) propertyValue).stream().anyMatch( e -> !(e instanceof String));
      if (invalid) {
        logger.error(errorMessage);
        throw new CloudFlowValidationException(errorMessage);
      }
    }
    return (List<String>) propertyValue;
  }

}
