package cloudflow.daos;

import java.util.List;

public interface ProjectAdminDao {
  List<String> findAdminsByProjectId(String projectId);
  void addAdmins(String projectId, List<String> admins);
}
