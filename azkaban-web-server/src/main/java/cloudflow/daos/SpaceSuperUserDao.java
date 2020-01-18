package cloudflow.daos;

import java.util.List;

public interface SpaceSuperUserDao {
  List<String> findAdminsBySpaceId(int spaceId);
  void addAdmins(int spaceId, List<String> admins);
  void addWatchers(int spaceId, List<String> watchers);
  List<String> findWatchersBySpaceId(int spaceId);
}
