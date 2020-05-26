package azkaban.project;

import java.util.Collection;
import java.util.List;

public interface ProjectCache {

  /**
   * Initialize the cache with projects and their corresponding flows fetched from DB.
   */
  void init();

  /**
   * Put the project in cache
   *
   * @param project Project
   */
  void putProject(Project project);

  /**
   * Gets the active project from cache if present else queries DB
   *
   * @param key name of the project
   * @return Project for it
   */
  Project getProjectByName(final String key);

  /**
   * Returns project corresponding to id if present in cache else queries DB to get it
   *
   * @param id Project id
   * @return Project
   */
  Project getProjectById(final Integer id);

  /**
   * invalidates the project from the cache
   *
   * @param project
   */
  void removeProject(Project project);

  /**
   * Queries DB to get all active projects present.
   *
   * @return list of all active projects;
   */
  public Collection<Project> getAllProjects();

  /**
   * @return list of names of all active projects
   */
  List<String> getAllProjectNames();

  /**
   * returns id corresponding to name;
   */
  Integer getProjectId(String name);
}
