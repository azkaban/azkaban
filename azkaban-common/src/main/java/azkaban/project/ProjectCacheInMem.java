package azkaban.project;

import azkaban.utils.CaseInsensitiveConcurrentHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements ProjectCache and extends AbstractProjectCache to implement a in-memory
 * implementation of project cache where all the active projects are loaded into the main-memory
 * when the web-server starts. This would be replaced in future by guava cache implementation.
 */
@Singleton
public class ProjectCacheInMem extends AbstractProjectCache implements ProjectCache {

  private static final Logger logger = LoggerFactory.getLogger(ProjectCacheInMem.class);

  private final ConcurrentHashMap<Integer, Project> projectsById;

  private final CaseInsensitiveConcurrentHashMap<Project> projectsByName;

  private final ProjectLoader projectLoader;

  @Inject
  public ProjectCacheInMem(final ProjectLoader loader) {
    super(loader);
    this.projectLoader = loader;
    this.projectsById = new ConcurrentHashMap<>();
    this.projectsByName = new CaseInsensitiveConcurrentHashMap<>();
    final long startTime = System.currentTimeMillis();
    init();
    final long elapsedTime = System.currentTimeMillis() - startTime;
    logger.info("Time taken to initalize and load cache in milliseconds: " + elapsedTime);
  }

  /**
   * load all active projects and their corresponding flows into memory.
   */
  @Override
  public void init() {

    final List<Project> projects;
    logger.info("Loading active projects.");
    try {
      projects = this.projectLoader.fetchAllActiveProjects();
    } catch (final ProjectManagerException e) {
      throw new RuntimeException("Could not load projects from store.", e);
    }
    for (final Project proj : projects) {
      putProject(proj);
    }
    logger.info("Loading flows from active projects.");

    loadAllFlowsForAllProjects(projects);
  }

  /**
   * Inserts given project into the cache.
   *
   * @param project Project
   */
  @Override
  public void putProject(final Project project) {
    this.projectsByName.put(project.getName(), project);
    this.projectsById.put(project.getId(), project);
  }

  /**
   * queries an active project by name
   *
   * @param key name of the project
   * @return Project
   */
  @Override
  public Project getProjectByName(final String key) {
    Project project = this.projectsByName.get(key);
    if (project == null) {
      logger.info("No active project with name {} exists in cache or DB.", key);
      try {
        project = this.projectLoader.fetchProjectByName(key);
      } catch (final ProjectManagerException e) {
        logger.error("Could not load project from store.", e);
      }
    } else {
      logger.info("Project {} not found in cache, fetched from DB.", key);
    }
    return project;
  }

  /**
   * Queries any project active/inactive by project id
   *
   * @param key Project id
   * @return Project
   */
  @Override
  public Project getProjectById(final Integer key) {
    Project project = this.projectsById.get(key);
    if (project == null) {
      try {
        project = this.projectLoader.fetchProjectById(key);
      } catch (final ProjectManagerException e) {
        logger.error("Could not load project from store.", e);
      }
    }
    return project;
  }

  /**
   * Invlidated the given project from cache.
   */
  @Override
  public void removeProject(final Project project) {
    this.projectsByName.remove(project.getName());
    this.projectsById.remove(project.getId());
  }

  /**
   * fetches names for all the active projects;
   */
  @Override
  public List<String> getAllProjectNames() {
    return this.projectsByName.getKeys();
  }

  /**
   * returns id by querying the project name map
   */
  @Override
  public Integer getProjectId(final String name) {
    return this.projectsByName.get(name).getId();
  }

}
