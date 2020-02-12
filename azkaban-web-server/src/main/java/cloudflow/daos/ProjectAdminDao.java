package cloudflow.daos;

import java.util.List;

public interface ProjectAdminDao {
  void addAdmins(String projectId, List<String> admins);
  List<String> findAdminsByProjectId(String projectId);
  List<String> findProjectIdsByAdmin(String username);
}
