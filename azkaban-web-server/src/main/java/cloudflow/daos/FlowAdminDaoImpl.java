package cloudflow.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import cloudflow.daos.ProjectAdminDaoImpl.ProjectAdminHandler;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowAdminDaoImpl implements FlowAdminDao {

  private DatabaseOperator dbOperator;
  private static final Logger log = LoggerFactory.getLogger(SpaceSuperUserDaoImpl.class);

  static String ADD_ADMIN =
      "INSERT INTO flow_admin (project_id, username) VALUES (?,?)";

  @Inject
  public FlowAdminDaoImpl(DatabaseOperator databaseOperator) {
    this.dbOperator = databaseOperator;
  }

  @Override
  public List<String> findAdminsByFlowId(String flowId) {
    FlowAdminHandler flowAdminHandler = new FlowAdminHandler();
    try {
      return dbOperator.query(FlowAdminHandler.FETCH_ADMINS_BY_FLOW_ID,
          flowAdminHandler, flowId);
    } catch (SQLException e) {
      log.error("Error in fetching admins for flowId '{}': {}", flowId, e);
    }
    return Collections.emptyList();
  }


  /* should run in a transaction */
  @Override
  public void addAdmins(String flowId, List<String> admins) {

    final SQLTransaction<Integer> addAdmins = transOperator -> {
      /* Not sure if this is the best way to do it */
      for(String admin : admins) {
        transOperator.update(ADD_ADMIN, flowId, admin);
      }
      transOperator.getConnection().commit();
      return 1;
    };

    try {
      dbOperator.transaction(addAdmins);
    } catch (SQLException e) {
      log.error("Error while adding admins for flow id '{}': {}", flowId, e);
    }
  }


  public static class FlowAdminHandler implements ResultSetHandler<List<String>> {

    static String FETCH_ADMINS_BY_FLOW_ID =
        "select username from flow_admin where flow_id = ?";

    @Override
    public List<String> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      List<String> admins = new ArrayList<>();
      do {
        String adminId = rs.getString(1);
        admins.add(adminId);
      } while(rs.next());
      return admins;
    }
  }
}
