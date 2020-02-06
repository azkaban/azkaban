package cloudflow.daos;

import cloudflow.models.FlowVersion;
import java.util.List;
import java.util.Optional;

public interface FlowVersionDao {
  Optional<FlowVersion> getVersion(String flowId, int flowVersion);
  List<FlowVersion> getAllVersions(String flowId);
}
