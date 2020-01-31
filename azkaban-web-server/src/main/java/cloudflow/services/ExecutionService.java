package cloudflow.services;

import cloudflow.error.CloudFlowException;
import cloudflow.models.JobExecution;
import java.util.Optional;

public interface ExecutionService {

    Optional<JobExecution> getJobExecution (String executionId, String jobDefinitionId,
        String user) throws CloudFlowException;
}
