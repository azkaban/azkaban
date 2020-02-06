package cloudflow.daos;

import azkaban.db.DatabaseOperator;
import cloudflow.models.FlowVersion;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowVersionDaoImpl implements FlowVersionDao {

  private DatabaseOperator dbOperator;
  private static final Logger log = LoggerFactory.getLogger(ProjectDaoImpl.class);

  @Inject
  public FlowVersionDaoImpl(DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  @Override
  public Optional<FlowVersion> getVersion(String flowId, int flowVersion) {
    List<FlowVersion> flowVersions = new ArrayList<>();
    FetchFlowVersionHandler fetchFlowVersionHandler = new FetchFlowVersionHandler();
    try {
      flowVersions = dbOperator.query(FetchFlowVersionHandler.SELECT_VERSION_WITH_FLOW_ID_AND_VERSION_NUMBER,
          fetchFlowVersionHandler, flowId, flowVersion);
    } catch (SQLException e) {
      log.error("Select for flow id: %s, flow version: %s failed: %s", flowId, flowVersion, e);
    }

    return flowVersions.isEmpty() ? Optional.empty() : Optional.of(flowVersions.get(0));
  }


  @Override
  public List<FlowVersion> getAllVersions(String flowId) {
    List<FlowVersion> flowVersions = new ArrayList<>();
    FetchFlowVersionHandler fetchFlowVersionHandler = new FetchFlowVersionHandler();

    try {
      flowVersions = dbOperator.query(FetchFlowVersionHandler.SELECT_ALL_VERSIONS_WITH_FLOW_ID,
          fetchFlowVersionHandler, flowId);

      // TODO: add admins for each version when admin functionality is added.

    } catch (SQLException e) {
      log.error("Get all flow versions for %s failed: %s", flowId, e);
    }

    return flowVersions;
  }


  public static class FetchFlowVersionHandler implements ResultSetHandler<List<FlowVersion>> {

    static String SELECT_VERSION_WITH_FLOW_ID_AND_VERSION_NUMBER =
        "SELECT project_id, project_version, flow_name, flow_version, modified_time, "
            + "flow_file, flow_id, experimental, dsl_version, created_by, locked, "
            + "flow_model_blob, encoding_type FROM project_flow_files WHERE flow_id = ? AND "
            + "flow_version = ?";

    static String SELECT_ALL_VERSIONS_WITH_FLOW_ID =
        "SELECT project_id, project_version, flow_name, flow_version, modified_time, "
            + "flow_file, flow_id, experimental, dsl_version, created_by, locked, "
            + "flow_model_blob, encoding_type FROM project_flow_files WHERE flow_id = ?";

    @Override
    public List<FlowVersion> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      List<FlowVersion> flowVersions = new ArrayList<>();

      do {
        FlowVersion flowVersion = new FlowVersion();
        flowVersion.setProjectId(rs.getString("project_id"));
        flowVersion.setProjectVersion(rs.getString("project_version"));
        flowVersion.setFlowName(rs.getString("flow_name"));
        flowVersion.setFlowVersion(rs.getString("flow_version"));
        flowVersion.setCreateTime(rs.getLong("modified_time"));
        flowVersion.setFlowFileLocation(rs.getString("flow_file"));
        flowVersion.setFlowId(rs.getString("flow_id"));
        flowVersion.setExperimental(rs.getBoolean("experimental"));
        flowVersion.setDslVersion(rs.getFloat("dsl_version"));
        flowVersion.setCreatedBy(rs.getString("created_by"));
        flowVersion.setLocked(rs.getBoolean("locked"));

        flowVersions.add(flowVersion);
      } while (rs.next());

      return flowVersions;
    }
  }
}
