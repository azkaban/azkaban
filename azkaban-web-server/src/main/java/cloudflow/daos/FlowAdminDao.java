package cloudflow.daos;

import java.util.List;

public interface FlowAdminDao {
  List<String> findAdminsByFlowId(String flowId);
  void addAdmins(String flowId, List<String> admins);
}

