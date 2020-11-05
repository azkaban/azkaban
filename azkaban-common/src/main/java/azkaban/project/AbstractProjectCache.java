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

import azkaban.flow.Flow;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract Class implementing common methods for Project Cache Implementation
 */

public abstract class AbstractProjectCache implements ProjectCache {

  private final ProjectLoader projectLoader;

  private static final Logger logger = LoggerFactory.getLogger(AbstractProjectCache.class);

  public AbstractProjectCache(final ProjectLoader loader) {
    this.projectLoader = loader;
  }

  /**
   * loadAllFlowsForAllProjects  : To load all flows corresponding to projects from the database
   *
   * @param projects list of Projects to fetch flows for.
   */
  protected void loadAllFlows(final List<Project> projects) {
    try {
      final Map<Project, List<Flow>> projectToFlows = this.projectLoader
          .fetchAllFlowsForProjects(projects);

      // Load the flows into the project objects
      for (final Map.Entry<Project, List<Flow>> entry : projectToFlows.entrySet()) {
        final Project project = entry.getKey();
        final List<Flow> flows = entry.getValue();

        final Map<String, Flow> flowMap = new HashMap<>();
        for (final Flow flow : flows) {
          flowMap.put(flow.getId(), flow);
        }

        project.setFlows(flowMap);
      }
    } catch (final ProjectManagerException e) {
      logger.error("Could not load projects flows from store.", e);
      throw new RuntimeException("Could not load projects flows from store.", e);
    }
  }

  /**
   * get all active projects from database.
   *
   * @return List of projects;
   */
  @Override
  public List<Project> getActiveProjects() {
    List<Project> result = Collections.emptyList();
    try {
      result = this.projectLoader.fetchAllActiveProjects();
    } catch (final ProjectManagerException e) {
      logger.error("Could not load projects flows from store.", e);
      throw new RuntimeException("Could not load projects from store.", e);
    }
    return result;
  }

  /**
   * Returns project object for given name using methods of the private projectLoader.
   *
   * @param key
   * @throws ProjectManagerException
   */
  protected Project fetchProjectByName(final String key) throws ProjectManagerException {
    final Project result = this.projectLoader.fetchProjectByName(key);
    return result;
  }

  /**
   * Returns project object for given id using methods of the private projectLoader.
   *
   * @param id
   * @throws ProjectManagerException
   */
  protected Project fetchProjectById(final Integer id) throws ProjectManagerException {
    final Project result = this.projectLoader.fetchProjectById(id);
    return result;
  }
}
