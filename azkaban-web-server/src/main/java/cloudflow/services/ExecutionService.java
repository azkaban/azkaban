package cloudflow.services;

import cloudflow.models.ExecutionBasicResponse;
import cloudflow.models.JobExecution;
import java.util.List;
import java.util.Map;


public interface ExecutionService {
  List<ExecutionBasicResponse> getAllExecutions(Map<String, String[]> queryParamMap);

  JobExecution getJobExecution(String executionId, String jobDefinitionId, String user);

  String createExecution(ExecutionParameters executionParameters);

  String createRerunExecution(ExecutionParameters executionParameters);
}
