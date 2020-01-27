package cloudflow.services;

import azkaban.user.User;
import cloudflow.daos.ProjectDao;
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

  public String create(Project project, User user) {
    return this.projectDao.create(project, user);
  }

  public Project get(final String projectId) {

    Optional<Project> project = projectDao.get(projectId);
    /* Not the best exception to throw but serves the use-case here.
     */
    if (!project.isPresent()) {
      log.error("Space record doesn't exist for the spaceId: " + projectId);
      throw new NoSuchElementException();
    }
    return project.get();
  }

  public List<Project> getAll() {
    return projectDao.getAll();
  }

}
