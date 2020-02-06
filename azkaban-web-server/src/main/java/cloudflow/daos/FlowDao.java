package cloudflow.daos;

import cloudflow.models.FlowResponse;
import java.util.List;
import java.util.Optional;

public interface FlowDao {
  Optional<FlowResponse> getFlow(String flowId);
  List<FlowResponse> getAllFlows(String projectId, String projectVersion);
}
