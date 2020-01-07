/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.project.validator;

import azkaban.project.Project;
import azkaban.utils.Props;
import java.io.File;


/**
 * Interface to be implemented by plugins which are to be registered with Azkaban as project
 * validators that validate a project before uploaded into Azkaban.
 */
public interface ProjectValidatorCacheable extends ProjectValidator {
  /**
   * Get a hash representing a context state for a project. Adding or removing JARs should
   * not change this state. The state should only be based on text files. Two different projects
   * for which the validator returns the same cacheKey should have IDENTICAL validation results
   * for a given jar. I.e. if coollib-1.0.0.jar is included in Proj1 and is also in Proj2, so long
   * as the validator returns the same cacheKey for both Proj1 and Proj2 it should also return the
   * same validation result for the coollib-1.0.0.jar present in both projects.
   *
   * @param project project to get the cache key for
   * @param projectDir directory of the uncompressed project
   * @param additionalProps additional project-specific props (some validators may use this, others may not)
   *
   * @return Hash representing cache key for project
   */
  String getCacheKey(Project project, File projectDir, Props additionalProps);
}
