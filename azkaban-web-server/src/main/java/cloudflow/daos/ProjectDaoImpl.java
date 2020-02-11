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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ProjectDaoImpl implements ProjectDao {

  private DatabaseOperator dbOperator;
  private ProjectAdminDaoImpl projectAdminDaoImpl;
  private static final Logger log = LoggerFactory.getLogger(ProjectDaoImpl.class);

  /* Use existing "projects" table and add the new columns to it instead of creating a new table
  with new columns and changing names of some of the existing columns to esnure non of the
  existing project queries break */

  static String INSERT_PROJECT =
      "INSERT INTO projects (name, active, modified_time, create_time, version, last_modified_by, "
          + "description, enc_type, settings_blob, space_id, created_by) values (?,?,?,?,?,?,?,?,"
          + "?,?,?)";

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
      long timestamp = System.currentTimeMillis();
      transOperator.update(INSERT_PROJECT,
          project.getName(), // project name
          1, // active
          timestamp, // modified_time
          timestamp, // create_time
          1, // version
          user.getUserId(), // last_modified_by
          project.getDescription(), // description
          2, // enc_type
          null, // settings_blob
          project.getSpaceId(), // space_id
          user.getUserId()); // created_by

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

    /* Get all projects created or modified by user*/
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
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, "
            + "description, enc_type, settings_blob, space_id, created_by FROM projects WHERE id"
            + " = ? and active = 1";

    static String SELECT_PROJECT_WITH_NAME_AND_SPACE_ID =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, "
            + "description, enc_type, settings_blob, space_id, created_by FROM projects"
            + " WHERE name = ? and space_id = ? and active = 1";

    static String SELECT_ALL_PROJECTS_BY_USER =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, "
            + "description, enc_type, settings_blob, space_id, created_by FROM projects WHERE "
            + "created_by = ? OR last_modified_by = ? and active = 1";

    @Override
    public List<Project> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      List<Project> projects = new ArrayList<>();
      do {
        Project currentProject = new Project();
        currentProject.setId(rs.getString("id"));
        currentProject.setName(rs.getString("name"));
        currentProject.setDescription(rs.getString("description"));
        currentProject.setSpaceId(rs.getString("space_id"));
        currentProject.setCreatedByUser(rs.getString("created_by"));
        currentProject.setCreatedOn(rs.getLong("create_time"));
        currentProject.setLastModifiedByUser(rs.getString("last_modified_by"));
        currentProject.setLastModifiedOn(rs.getLong("modified_time"));
        currentProject.setLatestVersion(rs.getString("version"));

        projects.add(currentProject);
      } while (rs.next());

      return projects;
    }
  }

}
