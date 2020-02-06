package cloudflow.daos;

import azkaban.db.DatabaseOperator;
import cloudflow.error.CloudFlowNotFoundException;
import cloudflow.models.FlowResponse;
import cloudflow.models.Project;
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

public class FlowDaoImpl implements FlowDao {

  private DatabaseOperator dbOperator;
  private FlowAdminDaoImpl flowAdminDaoImpl;
  private ProjectDao projectDao;
  private static final Logger log = LoggerFactory.getLogger(ProjectDaoImpl.class);

  @Inject
  public FlowDaoImpl(DatabaseOperator dbOperator, FlowAdminDaoImpl flowAdminDaoImpl,
      ProjectDao projectDao) {
    this.dbOperator = dbOperator;
    this.flowAdminDaoImpl = flowAdminDaoImpl;
    this.projectDao = projectDao;
  }


  @Override
  public Optional<FlowResponse> getFlow(String flowId){
    List<FlowResponse> flows = new ArrayList<>();
    FetchFlowHandler fetchFlowHandler = new FetchFlowHandler();
    try {
      flows = dbOperator.query(FetchFlowHandler.SELECT_FLOW_WITH_FLOW_ID,
          fetchFlowHandler, flowId);
      for(FlowResponse flow : flows) {
        //flow.setAdmins(flowAdminDaoImpl.findAdminsByFlowId(flow.getId()));
      }
    } catch (SQLException ex) {
      log.error("Select for flow id '{}' failed: ", flowId, ex);
    }
    return flows.isEmpty() ? Optional.empty() : Optional.of(flows.get(0));
  }


  @Override
  public List<FlowResponse> getAllFlows(String projectId, String projectVersion) {
    List<FlowResponse> flows = new ArrayList<FlowResponse>();
    FetchFlowHandler fetchFlowHandler = new FetchFlowHandler();

    /* If projectVersion is -1, get the latest project version */
    if (projectVersion.equals("-1")) {
      Optional<Project> project = projectDao.get(projectId);
      if (!project.isPresent()) {
        throw new CloudFlowNotFoundException("");
      }
      projectVersion = project.get().getLatestVersion();
    }

    /* Get all flows for projectId and projectVersion combination */
    try {
      flows =
          dbOperator.query(FetchFlowHandler.SELECT_ALL_FLOWS_BY_PROJECT_ID_AND_PROJECT_VERSION,
              fetchFlowHandler, projectId, projectVersion);

      for(FlowResponse flow : flows) {
        //flow.setAdmins(flowAdminDaoImpl.findAdminsByFlowId(flow.getId()));
      }
    } catch (SQLException e) {
      log.error("Get all projects for project id: %s, project version: %s failed: %s", projectId,
          projectVersion, e);
    }
    return flows;
  }


  public static class FetchFlowHandler implements ResultSetHandler<List<FlowResponse>> {

    static String SELECT_FLOW_WITH_FLOW_ID =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json, "
            + "flow_name, latest_flow_version FROM project_flows WHERE flow_id = ?";

    static String SELECT_FLOW_WITH_FLOW_NAME =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json, flow_name, "
            + "latest_flow_version FROM project_flows WHERE name = ?";

    static String SELECT_ALL_FLOWS_BY_PROJECT_ID_AND_PROJECT_VERSION =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json, "
            + "flow_name, latest_flow_version FROM project_flows WHERE project_id = ? and version"
            + " = ?";

    @Override
    public List<FlowResponse> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      List<FlowResponse> flows = new ArrayList<>();
      do {
        FlowResponse flow = new FlowResponse();
        flow.setId(rs.getString("flow_id"));
        flow.setName(rs.getString("flow_name"));
        flow.setFlowVersionCount(rs.getInt("latest_flow_version"));
        flow.setProjectId(rs.getString("project_id"));
        flow.setProjectVersion(rs.getString("version"));

        flows.add(flow);
      } while (rs.next());

      return flows;
    }
  }
}
