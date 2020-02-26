package cloudflow.services;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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
import cloudflow.error.CloudFlowValidationException;
import cloudflow.models.ExecutionBasicResponse;
import cloudflow.models.ExecutionDetailedResponse;
import cloudflow.models.ExecutionNodeResponse;
import cloudflow.models.ExecutionNodeResponse.ExecutionNodeResponseBuilder;
import cloudflow.models.ExecutionNodeResponse.ExecutionNodeType;
import cloudflow.models.ExecutionNodeResponse.FlowBasicResponse;
import cloudflow.models.JobExecution;
import cloudflow.models.NodeExecutionAttempt;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class ExecutionServiceImpl implements ExecutionService {

  private static final String FLOW_ID_PARAM = "flow_id";
  private static final String FLOW_VERSION_PARAM = "flow_version";
  private static final String PROJECT_ID_PARAM = "project_id";
  private static final String EXPERIMENT_ID_PARAM = "experiment_id";

  private static final int MAX_FLOW_RECURSION_DEPTH = 10;
  private static final String RUNTIME_PROPERTIES_ROOT_FLOW_KEYWORD = "root";

  private static final Logger logger = LoggerFactory.getLogger(ExecutionServiceImpl.class);
  private final ExecutorManagerAdapter executorManager;
  private final ExecutionFlowDao executionFlowDao;
  private final ProjectManager projectManager;

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
      throw new CloudFlowException(format("Argument %s should have exactly one value", argName));
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

  private ExecutionNodeResponse.FlowBasicResponse flowBasicResponseFromFlow(Flow flow) {
    //todo (sshardool): update flow name when available and check the flow version
    // once the "/flow" endpoint is implemented the current flow.getId() will be the flow name.
    return new FlowBasicResponse("defaultId", flow.getId(), Integer.toString(flow.getVersion()));
  }

  private Optional<ExecutionNodeResponse> nodeResponseFromExecutionNode(
      ExecutableNode executableNode,
      int currentDepth, int truncateAfterDepth) {
    requireNonNull(executableNode, "executable nodes is null");

    if (currentDepth == truncateAfterDepth) {
      return Optional.empty();
    }
    if (currentDepth == MAX_FLOW_RECURSION_DEPTH) {
      throw new CloudFlowValidationException(
          "Embedded flows are not supported beyond a depth of " + currentDepth);
    }

    List<NodeExecutionAttempt> executionAttempts =
        requireNonNull(getNodeExecutionAttempts(executableNode));
    checkState(!executionAttempts.isEmpty(), "execution attempt list was empty");

    // Attempt List is assumed to be sorted by execution attempt number.
    // The user visible attempt information is slightly different from how it's captured in
    // ExecutableNode.
    // Start time for this node will be the start time of the first attempt and end time will
    // be for the last attempt in the list (same as the current executable node)
    long nodeStartTime = executionAttempts.get(0).getStartTime();
    long nodeEndTime = executionAttempts.get(executionAttempts.size() - 1).getEndTime();
    checkState(executableNode.getEndTime() == nodeEndTime,
        "attempt list end time was different from current node");

    ExecutionNodeResponseBuilder nodeBuilder = ExecutionNodeResponseBuilder.newBuilder()
        .withNodeId(executableNode.getId())
        .withStartTime(nodeStartTime)
        .withEndTime(nodeEndTime)
        .withStatus(executableNode.getStatus().toString())
        .withUpdateTime(executableNode.getUpdateTime())
        .withCondition(executableNode.getCondition())
        .withNestedId(executableNode.getNestedId())
        .withExecutionAttempt(executionAttempts);

    if (executableNode.getInNodes() != null) {
      nodeBuilder = nodeBuilder.withInputNodeIds(new ArrayList<>(executableNode.getInNodes()));
    }

    if (executableNode instanceof ExecutableFlowBase) {
      ExecutableFlowBase flowBase = (ExecutableFlowBase) executableNode;
      List<Optional<ExecutionNodeResponse>> subNodeList = new ArrayList<>();

      for (ExecutableNode subNode : flowBase.getExecutableNodes()) {
        Optional<ExecutionNodeResponse> subNodeResponse = nodeResponseFromExecutionNode(subNode,
            currentDepth + 1, truncateAfterDepth);
        subNodeList.add(subNodeResponse);
      }

      Project project = projectManager.getProject(flowBase.getProjectId());
      nodeBuilder =
          nodeBuilder
              .withFlowInfo(flowBasicResponseFromFlow(project.getFlow(flowBase.getFlowId())));
      nodeBuilder = nodeBuilder.withNodeList(subNodeList)
          .withBaseFlowId(flowBase.getFlowId())
          .withNodeType(flowBase instanceof ExecutableFlow ? ExecutionNodeType.ROOT_FLOW
              : ExecutionNodeType.EMBEDDED_FLOW);
    } else {
      nodeBuilder = nodeBuilder.withNodeType(ExecutionNodeType.JOB);
    }

    return Optional.of(nodeBuilder.build());
  }


  @Override
  public ExecutionDetailedResponse getExecution(String executionIdString) {
    requireNonNull(executionIdString, "execution id is null");
    int executionId;
    String errorMessage;
    try {
      executionId = Integer.parseInt(executionIdString);
    } catch (NumberFormatException nfe) {
      errorMessage = "Execution Id must be an integer";
      logger.error(errorMessage, nfe);
      throw new CloudFlowValidationException(errorMessage, nfe);
    }

    ExecutableFlow executableFlow;
    try {
      executableFlow = executorManager.getExecutableFlow(executionId);
    } catch (final ExecutorManagerException e) {
      errorMessage = format("Failed to fetch execution with id %d.", executionId);
      logger.error(errorMessage, e);
      throw new CloudFlowException(errorMessage);
    }
    if (executableFlow == null) {
      throw new CloudFlowNotFoundException(format("Execution id %d does not exist", executionId));
    }

    // todo: make truncation-depth user visible and validate it (replaces MAX_VALUE below)
    Optional<ExecutionNodeResponse> rootNode = nodeResponseFromExecutionNode(executableFlow, 0,
        Integer.MAX_VALUE);
    if (!rootNode.isPresent()) {
      throw new CloudFlowException(
          "Execution details not found for root flow " + executableFlow.getId());
    }
    ExecutionDetailedResponse detailedResponse = new ExecutionDetailedResponse(
        executableFlow, rootNode.get());
    return detailedResponse;
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
    List<NodeExecutionAttempt> attempts = getNodeExecutionAttempts(node);
    jobExecution.setAttempts(attempts);

    Optional<NodeExecutionAttempt> firstAttempt = attempts.stream().filter(a -> a.getId() == 0)
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

  private List<NodeExecutionAttempt> getNodeExecutionAttempts(ExecutableNode executableNode) {
    List<NodeExecutionAttempt> attempts = new ArrayList<>();
    for (Object o : executableNode.getAttemptObjects()) {
      Map<String, Object> attempt = (Map<String, Object>) o;
      int id = (Integer) attempt.get(ExecutionAttempt.ATTEMPT_PARAM);
      long startTime = (Long) attempt.get(ExecutionAttempt.STARTTIME_PARAM);
      long endTime = (Long) attempt.get(ExecutionAttempt.ENDTIME_PARAM);
      Status status = Status.valueOf((String) attempt.get(ExecutionAttempt.STATUS_PARAM));
      attempts.add(new NodeExecutionAttempt(id, startTime, endTime, status));
    }

    NodeExecutionAttempt lastAttempt = new NodeExecutionAttempt(attempts.size(),
        executableNode.getStartTime(), executableNode.getEndTime(), executableNode.getStatus());
    attempts.add(lastAttempt);
    return attempts;
  }

  @Override
  public String createExecution(ExecutionParameters executionParameters) {
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
        final String errorMessage = format("Failed to create execution of flow with id %s "
                + "and version %s.", executionParameters.getFlowId(),
            executionParameters.getFlowVersion());
        logger.error(errorMessage, e);
        throw new CloudFlowException(errorMessage);
      }
    }
    return String.valueOf(exflow.getExecutionId());
  }

  private void setAzkabanExecutionOptions(ExecutionParameters executionParameters,
      ExecutableFlow executableFlow) {

    try {
      executableFlow.setFlowDefinitionId(Integer.parseInt(executionParameters.getFlowId()));
    } catch (NumberFormatException e) {
      throw new CloudFlowValidationException("Flow id must be an integer.");
    }

    // TODO ypadron: validate existence of flow version
    if (executionParameters.getFlowVersion() <= 0) {
      throw new CloudFlowValidationException("Flow version must be a positive number.");
    }
    executableFlow.setFlowVersion(executionParameters.getFlowVersion());
    executableFlow.setDescription(executionParameters.getDescription());
    if (!executionParameters.getExperimentId().isEmpty()) {
      try {
        executableFlow.setExperimentId(Integer.parseInt(executionParameters.getExperimentId()));
        // TODO ypadron: validate existence of experiment id
      } catch (NumberFormatException e) {
        throw new CloudFlowValidationException("Experiment id must be an integer.");
      }
    }
    executableFlow.setSubmitUser(executionParameters.getSubmitUser());

    ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setFailureAction(ExecutionOptions.FailureAction.valueOf(
        executionParameters.getFailureAction().toString()));

    executionOptions.setNotifyOnFirstFailure(executionParameters.isNotifyOnFirstFailure());
    executionOptions
        .setNotifyOnLastFailure(executionParameters.isNotifyFailureOnExecutionComplete());

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
    if (executionParameters.getProperties().size() > 1 ||
        !executionParameters.getProperties().containsKey(RUNTIME_PROPERTIES_ROOT_FLOW_KEYWORD)) {
      logger.error("Attempt to overwrite properties of nodes other than the root flow detected: "
          + executionParameters.getProperties());
      throw new CloudFlowNotImplementedException(
          "Overwriting properties of jobs or nested flows is not "
              + "yet supported.");
    }

    Map<String, Object> runtimeProps = executionParameters.getProperties()
        .get(RUNTIME_PROPERTIES_ROOT_FLOW_KEYWORD);
    if (runtimeProps.containsKey(CommonJobProperties.FAILURE_EMAILS)) {
      Object propValue = runtimeProps.get(CommonJobProperties.FAILURE_EMAILS);
      List<String> emails = getNotificationEmails(propValue);
      executionOptions.setFailureEmails(emails);
      executionOptions.setFailureEmailsOverridden(true);
      runtimeProps.remove(CommonJobProperties.FAILURE_EMAILS);
    }

    if (runtimeProps.containsKey(CommonJobProperties.SUCCESS_EMAILS)) {
      Object propValue = runtimeProps.get(CommonJobProperties.SUCCESS_EMAILS);
      List<String> emails = getNotificationEmails(propValue);
      executionOptions.setSuccessEmails(emails);
      executionOptions.setSuccessEmailsOverridden(true);
      runtimeProps.remove(CommonJobProperties.SUCCESS_EMAILS);
    }

    Map<String, String> azRuntimeProps = runtimeProps.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    executionOptions.addAllFlowParameters(azRuntimeProps);
  }

  private List<String> getNotificationEmails(Object propertyValue) {
    String errorMessage = format("Invalid notification email list: %s", propertyValue.toString());
    if (!(propertyValue instanceof List) || ((List) propertyValue).isEmpty()) {
      logger.error(errorMessage);
      throw new CloudFlowValidationException(errorMessage);
    } else {
      boolean invalid =
          ((List<Object>) propertyValue).stream().anyMatch(e -> !(e instanceof String));
      if (invalid) {
        logger.error(errorMessage);
        throw new CloudFlowValidationException(errorMessage);
      }
    }
    return (List<String>) propertyValue;
  }

}
