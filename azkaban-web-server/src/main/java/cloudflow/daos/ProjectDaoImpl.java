package cloudflow.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.project.ProjectManagerException;
import azkaban.user.User;
import cloudflow.models.Project;
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
public class ProjectDaoImpl implements ProjectDao {

  private DatabaseOperator dbOperator;
  private ProjectAdminDaoImpl projectAdminDaoImpl;
  private static final Logger log = LoggerFactory.getLogger(ProjectDaoImpl.class);

  static String INSERT_PROJECT =
      "INSERT INTO project (name, description, space_id, created_by, creation_time, "
          + "last_modified_by, last_modified_time, version,  active, enc_type, settings_blob) "
          + "values "
          + "(?,?,?,?,?,?,?,?,?,?,?)";

  @Inject
  public ProjectDaoImpl(DatabaseOperator dbOperator, ProjectAdminDaoImpl projectAdminDaoImpl) {
    this.dbOperator = dbOperator;
    this.projectAdminDaoImpl = projectAdminDaoImpl;
  }

  @Override
  public String create(Project project, User user) {
    String name = project.getName();
    FetchProjectHandler fetchProjectHandler = new FetchProjectHandler();

    // Check if the same project name exists.
    try {
      final List<Project> projects = this.dbOperator
          .query(FetchProjectHandler.SELECT_PROJECT_WITH_NAME_AND_SPACE_ID, fetchProjectHandler,
              name, project.getSpaceId());
      if (!projects.isEmpty()) {
        throw new ProjectManagerException(
            "Project with name " + name + " already exists.");
      }
    } catch (final SQLException ex) {
      log.error("Check if project '" + name + "' exists failed with: " + ex);
      throw new ProjectManagerException("Check if project '" + name + "' exists failed");
    }

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
      if (projectId.isEmpty()) {
        throw new ProjectManagerException("Insert project for '" + name + "' failed.");
      }
      projectAdminDaoImpl.addAdmins(projectId, project.getAdmins());
    } catch (SQLException ex) {
      log.error(INSERT_PROJECT + " failed with exception:", ex);
      throw new ProjectManagerException("Insert project for '" + name + "' failed.",
          ex);
    }
    return projectId;
  }

  @Override
  public Optional<Project> get(String projectId) {
    List<Project> projects = new ArrayList<>();
    FetchProjectHandler fetchProjectHandler = new FetchProjectHandler();
    try {
      projects = dbOperator.query(FetchProjectHandler.SELECT_PROJECT_WITH_ID, fetchProjectHandler,
          projectId);
      for(Project project : projects) {
        project.setAdmins(projectAdminDaoImpl.findAdminsByProjectId(project.getId()));
      }
    } catch (SQLException ex) {
      log.error("Select for project record with id " + projectId + " failed: ", ex);
    }
    return projects.isEmpty() ? Optional.empty() : Optional.of(projects.get(0));
  }

  @Override
  public List<Project> getAll(User user) {
    List<Project> projects = new ArrayList<>();
    FetchProjectHandler fetchProjectHandler = new FetchProjectHandler();
    try {
      projects = dbOperator.query(FetchProjectHandler.SELECT_ALL_PROJECTS_BY_USER,
          fetchProjectHandler, user.getUserId(), user.getUserId());
      for(Project project : projects) {
        project.setAdmins(projectAdminDaoImpl.findAdminsByProjectId(project.getId()));
      }
    } catch (SQLException ex) {
      log.error("Get all projects for user '" + user.getUserId() + "'failed: ", ex);
    }
    return projects;
  }


  public static class FetchProjectHandler implements ResultSetHandler<List<Project>> {

    static String SELECT_PROJECT_WITH_ID =
        "SELECT id, name, description, space_id, created_by, creation_time, last_modified_by, "
            + "last_modified_time, version,  active, enc_type, settings_blob FROM project WHERE id"
            + " = ?";

    static String SELECT_PROJECT_WITH_NAME_AND_SPACE_ID =
        "SELECT id, name, description, space_id, created_by, creation_time, last_modified_by, "
            + "last_modified_time, version,  active, enc_type, settings_blob FROM project WHERE "
            + "name = ? and space_id = ?";

    static String SELECT_ALL_PROJECTS =
        "SELECT id, name, description, space_id, created_by, creation_time, last_modified_by, "
            + "last_modified_time, version,  active, enc_type, settings_blob FROM project";

    static String SELECT_ALL_PROJECTS_BY_USER =
        "SELECT id, name, description, space_id, created_by, creation_time, last_modified_by, "
            + "last_modified_time, version,  active, enc_type, settings_blob FROM project where "
            + "created_by = ? OR last_modified_by = ?";


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
      } while (rs.next());

      return projects;
    }
  }

}
