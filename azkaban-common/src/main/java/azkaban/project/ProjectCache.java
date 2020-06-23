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

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public interface ProjectCache {

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
  Optional<Project> getProjectByName(final String key);

  /**
   * Returns project corresponding to id if present in cache else queries DB to get it
   *
   * @param id Project id
   * @return Project
   */
  Optional<Project> getProjectById(final Integer id);

  /**
   * invalidates the project from the cache
   *
   * @param project
   */
  void removeProject(Project project);

  /**
   * returns list of all active projects;
   */
  List<Project> getActiveProjects();

  /**
   * Returns matching project names to given regex pattern.
   */
  List<Project> getProjectsWithSimilarNames(Pattern pattern);
}
