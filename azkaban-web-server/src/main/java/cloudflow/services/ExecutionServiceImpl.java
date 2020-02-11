package cloudflow.services;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionAttempt;
import azkaban.executor.ExecutionFlowDao;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import cloudflow.error.CloudFlowException;
import cloudflow.error.CloudFlowNotFoundException;
import cloudflow.models.ExecutionBasicResponse;
import cloudflow.models.ExecutionDetailedResponse;
import cloudflow.models.ExecutionNodeResponse;
import cloudflow.models.ExecutionNodeResponse.ExecutionNodeResponseBuilder;
import cloudflow.models.JobExecution;
import cloudflow.models.JobExecutionAttempt;
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

  private static final Logger logger = LoggerFactory.getLogger(ExecutionServiceImpl.class);
  private ExecutorManagerAdapter executorManager;
  private ExecutionFlowDao executionFlowDao;

  @Inject
  public ExecutionServiceImpl(ExecutorManagerAdapter executorManager,
      ExecutionFlowDao executionFlowDao) {
    this.executorManager = executorManager;
    this.executionFlowDao = executionFlowDao;
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

  // todo(sshardool): this should have a check for run-away recursion on malformed input
  private static ExecutionNodeResponse nodeResponseFromExecutionNode(
      ExecutableNode executableNode) {
    ExecutionNodeResponseBuilder nodeBuilder = ExecutionNodeResponseBuilder.newBuilder();
    nodeBuilder = nodeBuilder.withNodeId(executableNode.getId())
        .withStartTime(executableNode.getStartTime())
        .withEndTime(executableNode.getEndTime())
        .withUpdateTime(executableNode.getUpdateTime())
        .withNodeType(executableNode.getType())
        .withCondition(executableNode.getCondition())
        .withNestedId(executableNode.getNestedId());

    if (executableNode.getInNodes() != null) {
      nodeBuilder = nodeBuilder.withInputNodeIds(new ArrayList<>(executableNode.getInNodes()));
    }

    if (executableNode instanceof ExecutableFlowBase) {
      ExecutableFlowBase flowBase = (ExecutableFlowBase) executableNode;
      List<ExecutionNodeResponse> subNodeList = new ArrayList<>();

      for (ExecutableNode subNode : flowBase.getExecutableNodes()) {
        ExecutionNodeResponse subNodeResponse = nodeResponseFromExecutionNode(subNode);
        subNodeList.add(subNodeResponse);
      }

      nodeBuilder = nodeBuilder.withNodeList(subNodeList)
          .withBaseFlowId(flowBase.getFlowId());
    }

    return nodeBuilder.build();
  }


  @Override
  public ExecutionDetailedResponse getSingleExecution(String executionIdString) {
    requireNonNull(executionIdString, "execution id is null");
    int executionId;
    String errorMessage;
    try {
      executionId = Integer.parseInt(executionIdString);
    } catch (NumberFormatException nfe) {
      errorMessage = "Execution Id must be an integer";
      logger.error(errorMessage, nfe);
      throw new CloudFlowException(errorMessage, nfe);
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

    ExecutionNodeResponse rootNode = nodeResponseFromExecutionNode(executableFlow);
    ExecutionDetailedResponse detailedResponse = new ExecutionDetailedResponse(executableFlow,
        rootNode);
    return detailedResponse;
  }

  @Override
  public JobExecution getJobExecution(String executionId, String jobDefinitionId, String user)
      throws CloudFlowException {
    requireNonNull(executionId, "execution id is null");
    requireNonNull(jobDefinitionId, "job definition id is null");

    // TODO: check user permissions

    final Integer execId = Integer.parseInt(executionId); // Azkaban ids are integers
    String errorMessage;
    ExecutableFlow executableFlow = null;
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

    // TODO: get job and job path given a definition id
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
        errorMessage =
            format("Job with id %s and path %s wasn't found in execution %d.", jobDefinitionId,
                jobPath, execId);
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

    JobExecutionAttempt lastAttempt =
        new JobExecutionAttempt(attempts.size(), jobNode.getStartTime(), jobNode.getEndTime(),
            jobNode.getStatus());
    attempts.add(lastAttempt);
    return attempts;
  }
}
