package cloudflow.services;

import static java.util.Objects.requireNonNull;

import azkaban.user.User;
import cloudflow.daos.ProjectAdminDao;
import cloudflow.daos.ProjectDao;
import cloudflow.daos.SpaceDao;
import cloudflow.error.CloudFlowNotFoundException;
import cloudflow.error.CloudFlowValidationException;
import cloudflow.models.Project;
import cloudflow.models.Space;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ProjectServiceImpl implements ProjectService {
  private final ProjectAdminDao projectAdminDao;
  private final ProjectDao projectDao;
  private final SpaceDao spaceDao;
  private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);

  @Inject
  public ProjectServiceImpl (ProjectAdminDao projectAdminDao, ProjectDao projectDao,
      SpaceDao spaceDao) {
    this.projectAdminDao = projectAdminDao;
    this.projectDao = projectDao;
    this.spaceDao = spaceDao;
  }

  public void validateSpaceId(Project project) {
    /* Check spaceId is an Integer */
    String spaceId = project.getSpaceId();
    try {
      Integer.parseInt(spaceId);
    } catch (NumberFormatException e) {
      String errorMsg = String.format("Invalid space id: %s passed by user while creating "
              + "project: %s", spaceId, project.getName());
      log.error(errorMsg);
      throw new CloudFlowValidationException(errorMsg);
    }

    /* Check spaceId exists in the DB */
    int spaceIdInt = Integer.parseInt(spaceId);
    Optional<Space> space = spaceDao.get(spaceIdInt);
    if (!space.isPresent()) {
      String errorMsg = String.format("Space id: %s passed by user while creating project: %s "
          + "does not exist", spaceId, project.getName());
      throw new CloudFlowValidationException(errorMsg);
    }
  }


  public void validateInput(Project project) {
    /* Check required fields are present */
    requireNonNull(project.getSpaceId(), "Space id is null");
    requireNonNull(project.getName(), "Name is null");

    validateSpaceId(project);

    /* Check all the fields that Azkaban will set are empty */
    if (project.getId() != null || project.getLatestVersion() != null ||
        project.getCreatedByUser() != null || project.getLastModifiedByUser() != null ||
        project.getCreatedOn() != 0 || project.getLastModifiedOn() != 0) {
      String errorMsg = String.format("A forbidden field was specified in request body for "
          + "project: %s", project.getName());
      log.error(errorMsg);
      throw new CloudFlowValidationException(errorMsg);
    }

    /* Check if admins list has any duplicates */
    Set<String> adminNames = new HashSet<String>();
    for (String name : project.getAdmins()) {
      if (adminNames.contains(name)) {
        String errorMsg = String.format("Duplicate userid: %s in admins list for project: %s",
            name, project.getName());
        log.error(errorMsg);
        throw new CloudFlowValidationException(errorMsg);
      } else {
        adminNames.add(name);
      }
    }
  }

  @Override
  public String createProject(Project project, User user) {
    validateInput(project);
    return this.projectDao.create(project, user);
  }

  @Override
  public Project getProject(final String projectId) {

    Optional<Project> project = projectDao.get(projectId);
    /* Not the best exception to throw but serves the use-case here.
     */
    if (!project.isPresent()) {
      String errorMsg = String.format("Project record doesn't exist for id: %s", projectId);
      log.error(errorMsg);
      throw new CloudFlowNotFoundException(errorMsg);
    }
    return project.get();
  }

  @Override
  public List<Project> getAllProjects(User user) {
    List<Project> projects = new ArrayList<Project>();

    /* Get all projects created or modified by user*/
    projects.addAll(projectDao.getAll(user));

    /* Get all projects for which user is an admin */
    List<String> projectIds = projectAdminDao.findProjectIdsByAdmin(user.getUserId());
    for (String projectId : projectIds) {
      try {
        Project project = getProject(projectId);
        projects.add(project);
      } catch (CloudFlowNotFoundException cfe) {
        // Project was deleted and hence marked as inactive but entry still exists in the
        // project_admin table. Ignore the error.
      }
    }

    return projects;
  }

}
