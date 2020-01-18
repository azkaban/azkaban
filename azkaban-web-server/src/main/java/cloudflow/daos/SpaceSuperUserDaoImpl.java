package cloudflow.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SpaceSuperUserDaoImpl implements SpaceSuperUserDao {

  private DatabaseOperator databaseOperator;

  private static final Logger log = LoggerFactory.getLogger(SpaceSuperUserDaoImpl.class);

  static String ADD_ADMIN =
      "INSERT INTO space_admin (space_id, username) VALUES (?,?)";

  static String ADD_WATCHER =
      "INSERT INTO space_watcher (space_id, username) VALUES (?,?)";

  @Inject
  public SpaceSuperUserDaoImpl(DatabaseOperator databaseOperator) {
    this.databaseOperator = databaseOperator;
  }
  /*To Explore: Can we avoid this code by using spring-data? */

  public List<String> findAdminsBySpaceId(int spaceId) {
    SpaceSuperUserHandler spaceSuperUserHandler = new SpaceSuperUserHandler();
    try {
      return databaseOperator.query(SpaceSuperUserHandler.FETCH_ADMINS_BY_SPACE_ID,
          spaceSuperUserHandler,
          spaceId);
    } catch (SQLException e) {
      log.error("Error in fetching the admins", e);
    }
    return Collections.emptyList();
  }

  /* should run in a transaction */
  public void addAdmins(int spaceId, List<String> admins) {

    final SQLTransaction<Integer> addAdmins = transOperator -> {
      /* Not sure if this is the best way to do it */
      for(String admin : admins) {
        transOperator.update(ADD_ADMIN, spaceId, admin);
      }
      transOperator.getConnection().commit();
      return 1;
    };

    try {
      databaseOperator.transaction(addAdmins);
    } catch (SQLException e) {
      log.error("Error in fetching the admins", e);
    }
  }

  public void addWatchers(int spaceId, List<String> watchers) {
    final SQLTransaction<Integer> addWatchers = transOperator -> {
      /* Not sure if this is the best way to do it */
      for(String watcher : watchers) {
        transOperator.update(ADD_WATCHER, spaceId, watcher);
      }
      transOperator.getConnection().commit();
      return 1;
    };

    try {
      databaseOperator.transaction(addWatchers);
    } catch (SQLException e) {
      log.error("Error in adding watchers", e);
    }
  }

  public List<String> findWatchersBySpaceId(int spaceId) {
    SpaceSuperUserHandler spaceSuperUserHandler = new SpaceSuperUserHandler();
    try {
      return databaseOperator.query(SpaceSuperUserHandler.FETCH_WATCHERS_BY_SPACE_ID,
          spaceSuperUserHandler,
          spaceId);
    } catch (SQLException e) {
      log.error("Error in finding the watchers", e);
    }
    return Collections.emptyList();
  }

  public static class SpaceSuperUserHandler implements ResultSetHandler<List<String>> {

    static String FETCH_ADMINS_BY_SPACE_ID =
        "select username from space_admin where space_id = ?";

    static String FETCH_WATCHERS_BY_SPACE_ID =
        "select username from space_watcher where space_id = ?";
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
