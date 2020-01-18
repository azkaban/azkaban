package cloudflow.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.user.User;
import cloudflow.models.Space;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SpaceDaoImpl implements SpaceDao {

  private static final Logger log = LoggerFactory.getLogger(SpaceDaoImpl.class);

  private DatabaseOperator databaseOperator;

  private SpaceSuperUserDaoImpl spaceSuperUserDaoImpl;

  static String INSERT_SPACE =
      "insert into  space ( name ,  description ,  created_on ,  created_by ,  modified_on ,  modified_by ) "
          + "values (?, ?, ?, ?, ?, ?)";

  @Inject
  public SpaceDaoImpl(DatabaseOperator operator, SpaceSuperUserDaoImpl spaceSuperUserDaoImpl) {
    this.databaseOperator = operator;
    this.spaceSuperUserDaoImpl = spaceSuperUserDaoImpl;
  }

  /* not the best code possible
     This is just an initial draft
     returns the space id
   */
  public int create(Space space, User user) {
    final SQLTransaction<Long> insertAndGetSpaceId = transOperator -> {
      String currentTime = DateTime.now().toLocalDateTime().toString();
      transOperator.update(INSERT_SPACE, space.getName(), space.getDescription(),
          currentTime, user.getUserId(), currentTime, user.getUserId());
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };

    int spaceId = 0;
    try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
      spaceId = databaseOperator.transaction(insertAndGetSpaceId).intValue();
      spaceSuperUserDaoImpl.addAdmins(spaceId, space.getAdmins());
      spaceSuperUserDaoImpl.addWatchers(spaceId, space.getWatchers());
    } catch (SQLException e) {
      log.error("Unable to create the space records", e);
    }
    return spaceId;
  }

  public Optional<Space> get(int spaceId) {
    List<Space> spaces = new ArrayList<>();
    FetchSpaceHandler fetchSpaceHandler = new FetchSpaceHandler();
    try {
      spaces = databaseOperator.query(FetchSpaceHandler.FETCH_SPACE_WITH_ID, fetchSpaceHandler,
          spaceId);
      for(Space s : spaces) {
        s.setAdmins(spaceSuperUserDaoImpl.findAdminsBySpaceId(s.getId()));
        s.setWatchers(spaceSuperUserDaoImpl.findWatchersBySpaceId(s.getId()));
      }
    } catch (SQLException e) {
      log.error("The record is not found", e);
    }
    return spaces.isEmpty() ? Optional.empty() : Optional.of(spaces.get(0));
  }

  public List<Space> getAll() {
    List<Space> spaces = new ArrayList<>();
    FetchSpaceHandler fetchSpaceHandler = new FetchSpaceHandler();
    try {
      spaces = databaseOperator.query(FetchSpaceHandler.FETCH_ALL_SPACES, fetchSpaceHandler);
    } catch (SQLException e) {
      log.error("Seems no records are found", e);
    }
    return spaces;
  }

  public static class FetchSpaceHandler implements ResultSetHandler<List<Space>> {

    static String FETCH_SPACE_WITH_ID =
        "SELECT id, name, description, created_on ,  created_by ,  modified_on ,  modified_by FROM space WHERE id = ?";
    static String FETCH_ALL_SPACES =
        "SELECT id, name, description, created_on ,  created_by ,  modified_on ,  modified_by "
            + "from space";

    @Override
    public List<Space> handle(ResultSet rs) throws SQLException {

      if (!rs.next()) {
        return Collections.emptyList();
      }
      List<Space> spaces = new ArrayList<>();
      do {
        int id = rs.getInt(1);
        String spaceName = rs.getString(2);
        String description = rs.getString(3);
        String createdOn = rs.getString(4);
        String createdeBy = rs.getString(5);
        String modifiedOn = rs.getString(6);
        String modifiedBy = rs.getString(7);
        Space currentSpace = new Space();
        currentSpace.setId(id);
        currentSpace.setName(spaceName);
        currentSpace.setDescription(description);
        currentSpace.setCreatedOn(createdOn);
        currentSpace.setCreatedBy(createdeBy);
        currentSpace.setModifiedOn(modifiedOn);
        currentSpace.setModifiedBy(modifiedBy);
        spaces.add(currentSpace);
      } while(rs.next());

      return spaces;
    }
  }
}
