package cloudflow.services;

import azkaban.user.User;
import cloudflow.daos.ProjectDao;
import cloudflow.error.CloudFlowNotFoundException;
import cloudflow.models.Project;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ProjectServiceImpl implements ProjectService {

  private final ProjectDao projectDao;
  private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);

  @Inject
  public ProjectServiceImpl (ProjectDao projectDao) {
    this.projectDao = projectDao;
  }

  @Override
  public String createProject(Project project, User user) {
    return this.projectDao.create(project, user);
  }

  @Override
  public Project getProject(final String projectId) {

    Optional<Project> project = projectDao.get(projectId);
    /* Not the best exception to throw but serves the use-case here.
     */
    if (!project.isPresent()) {
      String errorMsg = String.format("Project record doesn't exist for id: %s", projectId);
      log.error("Project record doesn't exist for id: " + projectId);
      throw new CloudFlowNotFoundException(errorMsg);
    }
    return project.get();
  }

  @Override
  public List<Project> getAllProjects(User user) {
    return projectDao.getAll(user);
  }

}
