/*
 * Copyright 2020 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.project;

import azkaban.utils.CaseInsensitiveConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements ProjectCache and extends AbstractProjectCache to implement a in-memory
 * implementation of project cache where all the active projects are loaded into the main-memory
 * when the web-server starts. This would be replaced in future by guava cache implementation.
 * <p>
 * The present cache consists of two mappings :  name to project  AND   id to project. In this
 * implementation both the maps contain all the project entities. In future implementations
 * name-to-project mapping will be replaced by name-to-id mapping containing all the active
 * projects' name-id and fixed size cache to store project entities.
 */
@Singleton
public class InMemoryProjectCache extends AbstractProjectCache implements ProjectCache {

  private static final Logger logger = LoggerFactory.getLogger(InMemoryProjectCache.class);

  private final ConcurrentHashMap<Integer, Project> projectsById;

  private final CaseInsensitiveConcurrentHashMap<Project> projectsByName;


  @Inject
  public InMemoryProjectCache(final ProjectLoader loader) {
    super(loader);
    this.projectsById = new ConcurrentHashMap<>();
    this.projectsByName = new CaseInsensitiveConcurrentHashMap<>();
    final long startTime = System.currentTimeMillis();
    init();
    final long elapsedTime = System.currentTimeMillis() - startTime;
    logger.info("Time taken to initialize and load cache in milliseconds: " + elapsedTime);
  }

  /**
   * load all active projects and their corresponding flows into memory. Queries from database only
   * returns a high level project object. Need to explicitly load flows for the project objects.
   */
  private void init() {
    final List<Project> projects = super.getActiveProjects();
    logger.info("Loading active projects.");
    for (final Project proj : projects) {
      putProject(proj);
    }
    logger.info("Loading flows from active projects.");
    loadAllFlows(projects);

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
   * Queries an active project by name. Fetches from database if not present in cache.
   *
   * @param key name of the project
   * @return Project
   */
  @Override
  public Optional<Project> getProjectByName(final String key) {
    Project project = this.projectsByName.get(key);
    if (project == null) {
      logger.info("No active project with name {} exists in cache, fetching from DB.", key);
      try {
        project = fetchProjectByName(key);
      } catch (final ProjectManagerException e) {
        logger.error("Could not load project from store.", e);
      }
    }
    return Optional.ofNullable(project);
  }

  /**
   * Fetch active/inactive project by project id. If active project not present in cache, fetches
   * from DB. Fetches inactive project from DB.
   *
   * @param key Project id
   * @return Project
   */
  @Override
  public Optional<Project> getProjectById(final Integer key) throws ProjectManagerException {
    Project project = this.projectsById.get(key);
    if (project == null) {
      logger.error("Project not found in cache, fetching from DB");
      project = fetchProjectById(key);
    }
    return Optional.ofNullable(project);
  }

  /**
   * Invalidates the given project from cache.
   */
  @Override
  public void removeProject(final Project project) {
    this.projectsByName.remove(project.getName());
    this.projectsById.remove(project.getId());
  }

  /**
   * @param pattern
   * @return List of Projects matching to given pattern.
   */
  @Override
  public List<Project> getProjectsWithSimilarNames(final Pattern pattern) {
    final List<Project> matches = new ArrayList<>();
    final ArrayList<String> names = new ArrayList<>(this.projectsByName.getKeys());
    for (final String projName : names) {
      if (pattern.matcher(projName).find()) {
        matches.add(this.projectsByName.get(projName));
      }
    }
    return matches;
  }

  /**
   * Returns all the projects from the in-memory cache map.
   */
  @Override
  public List<Project> getActiveProjects() {
    return new ArrayList<>(this.projectsById.values());
  }


}
