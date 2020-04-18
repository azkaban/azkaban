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
package azkaban.executor;

import com.sun.istack.NotNull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Map Object of Executable Ramp Dependency, Map.key = dependencyId
 */
public final class ExecutableRampDependencyMap extends BaseRefreshableMap<String, ExecutableRampDependency> {

  private ExecutableRampDependencyMap() {
    super();
  }

  public static ExecutableRampDependencyMap createInstance() {
    return new ExecutableRampDependencyMap();
  }

  /**
   * Add new dependency default setting
   *
   * @param dependency dependency
   * @param defaultValue default dependency value
   * @param jobTypes job types
   * @return this
   */
  synchronized public ExecutableRampDependencyMap add(@NotNull final String dependency,
      final String defaultValue, final String jobTypes) {

    this.add(
        dependency,
        ExecutableRampDependency
            .createInstance()
            .setDefaultValue(defaultValue)
            .setAssociatedJobTypes(jobTypes)
    );
    return this;
  }

  /**
   * Get Default Value
   * @param dependency dependency name
   * @return default dependency value
   */
  public String getDefaultValue(@NotNull final String dependency) {
    return Optional.ofNullable(this.get(dependency))
        .map(ExecutableRampDependency::getDefaultValue)
        .orElse(null);
  }

  /**
   * Get Map of default values by the given set of dependencies
   * @param dependencies dependencies
   * @return Map of Default Values
   */
  public Map<String, String> getDefaultValues(@NotNull final Set<String> dependencies) {
    return dependencies.stream().collect(Collectors.toMap(
        dependency -> dependency,
        dependency -> getDefaultValue(dependency)
    ));
  }

  /**
   * Check if the dependency is associated with the particular job type
   * @param dependency dependency name
   * @param jobType jobtype name
   * @return true/false
   */
  synchronized public boolean isValidJobType(@NotNull final String dependency,
      @NotNull final String jobType) {
    // If no specified job type associated, it means the ramp is valid for all job types
    return Optional.ofNullable(this.get(dependency))
        .map(dp -> Optional.ofNullable(dp.getAssociatedJobTypes())
            .map(set -> set.contains(jobType))
            .orElse(true))
        .orElse(false);
  }

  @Override
  public ExecutableRampDependencyMap clone() {
    return (ExecutableRampDependencyMap) super.clone();
  }
}
