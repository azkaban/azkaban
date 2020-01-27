package cloudflow.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.user.User;
import cloudflow.models.Project;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.commons.dbutils.ResultSetHandler;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectDaoImpl implements ProjectDao {

  private DatabaseOperator dbOperator;
  private static final Logger log = LoggerFactory.getLogger(ProjectDaoImpl.class);

  static String INSERT_PROJECT =
      "INSERT INTO project (name, description, space_id, created_by, created_time, "
          + "last_modified_by, modified_time, version,  active, enc_type, settings_blob) values "
          + "(?,?,?,?,?,?,?,?,?,?,?)";

  @Inject
  public ProjectDaoImpl(DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  public String create(Project project, User user) {
    final SQLTransaction<Long> insertAndGetProjectId = transOperator -> {
      String currentTime = DateTime.now().toLocalDateTime().toString();
      transOperator.update(INSERT_PROJECT, project.getName(), project.getDescription(),
          project.getSpaceId(), user.getUserId(), currentTime, user.getUserId(), currentTime,
          1, true, 2, null);
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };

    String projectId = "";
    try {
      projectId = dbOperator.transaction(insertAndGetProjectId).toString();
    } catch (SQLException e) {
      e.printStackTrace();
      // log and throw the exception
    }
    return projectId;
  }

  public Optional<Project> get(String projectId) {
    List<Project> projects = new ArrayList<>();
    FetchProjectHandler fetchProjectHandler = new FetchProjectHandler();
    try {
      projects = dbOperator.query(FetchProjectHandler.FETCH_PROJECT_WITH_ID, fetchProjectHandler,
          projectId);
      for(Project project : projects) {
        //s.setAdmins(spaceSuperUserDaoImpl.findAdminsBySpaceId(s.getId()));
        //s.setWatchers(spaceSuperUserDaoImpl.findWatchersBySpaceId(s.getId()));
      }
    } catch (SQLException e) {
      log.error("The record is not found", e);
    }
    return projects.isEmpty() ? Optional.empty() : Optional.of(projects.get(0));
  }

  public List<Project> getAll() {
    List<Project> projects = new ArrayList<>();
    FetchProjectHandler fetchProjectHandler = new FetchProjectHandler();
    try {
      projects = dbOperator.query(FetchProjectHandler.FETCH_ALL_PROJECTS, fetchProjectHandler);
    } catch (SQLException e) {
      log.error("SQL Exception while fetching all projects: ", e);
    }
    return projects;
  }

  public static class FetchProjectHandler implements ResultSetHandler<List<Project>> {

    static String FETCH_PROJECT_WITH_ID =
        "SELECT id, name, description, space_id, created_by, created_time, last_modified_by, "
            + "modified_time, version,  active, enc_type, settings_blob FROM project WHERE id = ?";

    static String FETCH_ALL_PROJECTS =
        "SELECT id, name, description, space_id, created_by, created_time, last_modified_by, "
            + "modified_time, version,  active, enc_type, settings_blob FROM project";

    @Override
    public List<Project> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      List<Project> projects = new ArrayList<>();
      do {
        Project currentProject = new Project();
        currentProject.setId(rs.getString(1));
        currentProject.setName(rs.getString(2));
        currentProject.setDescription(rs.getString(3));
        currentProject.setSpaceId(rs.getString(4));
        currentProject.setCreatedByUser(rs.getString(5));
        currentProject.setCreatedOn(rs.getString(6));
        currentProject.setLastModifiedByUser(rs.getString(7));
        currentProject.setLastModifiedOn(rs.getString(8));
        currentProject.setLatestVersion(rs.getString(9));

        projects.add(currentProject);
      } while(rs.next());

      return projects;
    }
  }

}
