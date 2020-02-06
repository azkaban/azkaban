package cloudflow.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectAdminDaoImpl implements ProjectAdminDao {

  private DatabaseOperator dbOperator;
  private static final Logger log = LoggerFactory.getLogger(SpaceSuperUserDaoImpl.class);

  static String ADD_ADMIN =
      "INSERT INTO project_admin (project_id, username) VALUES (?,?)";

  @Inject
  public ProjectAdminDaoImpl(DatabaseOperator databaseOperator) {
    this.dbOperator = databaseOperator;
  }

  @Override
  public List<String> findAdminsByProjectId(String projectId) {
    ProjectAdminHandler projectAdminHandler = new ProjectAdminHandler();
    try {
      return dbOperator.query(ProjectAdminHandler.FETCH_ADMINS_BY_PROJECT_ID,
          projectAdminHandler, projectId);
    } catch (SQLException e) {
      log.error("Error in fetching admins for projectId '" + projectId + "':", e);
    }
    return Collections.emptyList();
  }

  /* should run in a transaction */
  @Override
  public void addAdmins(String projectId, List<String> admins) {

    final SQLTransaction<Integer> addAdmins = transOperator -> {
      /* Not sure if this is the best way to do it */
      for(String admin : admins) {
        transOperator.update(ADD_ADMIN, projectId, admin);
      }
      transOperator.getConnection().commit();
      return 1;
    };

    try {
      dbOperator.transaction(addAdmins);
    } catch (SQLException e) {
      log.error("Error while adding admins for project id '" + projectId + "': ", e);
    }
  }


  public static class ProjectAdminHandler implements ResultSetHandler<List<String>> {

    static String FETCH_ADMINS_BY_PROJECT_ID =
        "select username from project_admin where project_id = ?";

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
