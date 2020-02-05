package cloudflow.services;

import cloudflow.models.JobExecution;
import java.util.Optional;

public interface ExecutionService {

    JobExecution getJobExecution (String executionId, String jobDefinitionId,
        String user);
}
