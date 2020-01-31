package cloudflow.services;


import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionAttempt;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import cloudflow.error.CloudFlowError;
import cloudflow.error.CloudFlowException;
import cloudflow.error.ErrorCode;
import cloudflow.models.JobExecution;
import cloudflow.models.JobExecutionAttempt;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class ExecutionServiceImpl implements ExecutionService {

    private static final Logger logger = LoggerFactory.getLogger(ExecutionServiceImpl.class);
    private ExecutorManagerAdapter executorManager;

    @Inject
    public ExecutionServiceImpl(ExecutorManagerAdapter executorManager) {
        this.executorManager = executorManager;
    }

    @Override
    public Optional<JobExecution> getJobExecution (String executionId, String jobDefinitionId,
        String user) throws CloudFlowException {
        // TODO: check user permissions

        final Integer execId = Integer.parseInt(executionId); // Azkaban ids are integers

        ExecutableFlow executableFlow = null;
        try {
            executableFlow = this.executorManager.getExecutableFlow(execId);
        } catch (final ExecutorManagerException e) {
            String errorMsg = String.format("Failed to fetch execution with id %d.", execId);
            logger.error(errorMsg, e);
            CloudFlowError error = new CloudFlowError(ErrorCode.UNEXPECTED_ERROR, errorMsg);
            throw new CloudFlowException(error);
        }

        if( executableFlow == null) {
            logger.info("Execution with id {} wasn't found.", execId);
            return Optional.empty();
        }

        // TODO: get job and job path given a definition id
        String jobPath = jobDefinitionId;
        String[] nodesInPath = jobPath.split(":");
        List<ExecutableNode> nodesToScan = executableFlow.getExecutableNodes();
        ExecutableNode node = null;
        for (String nodeId: nodesInPath) {
            int i=0;
            for (; i < nodesToScan.size(); i++) {
                ExecutableNode en = nodesToScan.get(i);
                if (en.getId().equals(nodeId)) {
                    node = en;
                    break;
                }
            }
            if (i >= nodesToScan.size()) {
                logger.info("Job with id {} and path {} wasn't found in execution {}.",
                    jobDefinitionId, jobPath, execId);
                return Optional.empty();
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
        Long firstStartTime = node.getStartTime();

        List<JobExecutionAttempt> attempts = new ArrayList<>();
        for(Object o: node.getAttemptObjects()) {
            Map<String, Object> attempt = (Map<String, Object>) o;
            Integer id = (Integer) attempt.get(ExecutionAttempt.ATTEMPT_PARAM);
            Long startTime = (Long) attempt.get(ExecutionAttempt.STARTTIME_PARAM);
            Long endTime = (Long) attempt.get(ExecutionAttempt.ENDTIME_PARAM);
            Status status = Status.valueOf((String) attempt.get(ExecutionAttempt.STATUS_PARAM));
            attempts.add(new JobExecutionAttempt(id, startTime, endTime, status));

            if(id == 0) {
                firstStartTime = startTime;
            }
        }

        JobExecutionAttempt lastAttempt = new JobExecutionAttempt(attempts.size(), node.getStartTime(),
        node.getEndTime(), node.getStatus());
        attempts.add(lastAttempt);
        jobExecution.setAttempts(attempts);

        jobExecution.setExecutionId(executionId);
        jobExecution.setStartTime(firstStartTime);
        jobExecution.setEndTime(node.getEndTime());
        jobExecution.setStatus(node.getStatus());
        // TODO: set data from job definition
        // TODO: set job properties

        return Optional.of(jobExecution);
    }

}
