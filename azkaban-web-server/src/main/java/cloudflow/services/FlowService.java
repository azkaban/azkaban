package cloudflow.services;

import azkaban.user.User;
import cloudflow.models.FlowResponse;
import java.util.List;
import java.util.Map;

public interface FlowService {
  FlowResponse getFlow(String flowId, Map<String, String[]> queryParamMap);
  List<FlowResponse> getAllFlows(User user, Map<String, String[]> queryParamMap);
}
