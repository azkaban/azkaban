/*
 * Copyright 2017 LinkedIn Corp.
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

import azkaban.executor.ExecutionOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;

/**
 * FlowTrigger is the logical representation of a trigger.
 * It couldn't be changed once gets constructed.
 * It will be used to create running trigger instance.
 */
public class FlowTrigger implements Serializable {

  private static final long serialVersionUID = 5613379236523054097L;
  private final Map<String, FlowTriggerDependency> dependencies;
  private final CronSchedule schedule;
  private final Duration maxWaitDuration;
  private final ExecutionOptions executionOptions;

  /**
   * @throws IllegalArgumentException if illegal argument is found or there is duplicate
   * dependency name or duplicate dependency type and params
   */
  public FlowTrigger(final CronSchedule schedule, final List<FlowTriggerDependency> dependencies,
      @Nullable final Duration maxWaitDuration, final ExecutionOptions executionOptions) {
    // will perform some basic validation here, and further validation will be performed on
    // parsing time when NodeBeanLoader parses the XML to flow trigger.
    Preconditions.checkNotNull(schedule, "schedule cannot be null");
    Preconditions.checkNotNull(dependencies, "dependency cannot be null");
    Preconditions.checkArgument(dependencies.isEmpty() || maxWaitDuration != null, "max wait "
        + "time cannot be null unless no dependency is defined");

    validateDependencies(dependencies);
    this.schedule = schedule;
    final ImmutableMap.Builder builder = new Builder();
    dependencies.forEach(dep -> builder.put(dep.getName(), dep));
    this.dependencies = builder.build();
    this.maxWaitDuration = maxWaitDuration;
    this.executionOptions = executionOptions;
  }

  /**
   * check uniqueness of dependency.name
   */
  private void validateDepNameUniqueness(final List<FlowTriggerDependency> dependencies) {
    final Set<String> seen = new HashSet<>();
    for (final FlowTriggerDependency dep : dependencies) {
      // set.add() returns false when there exists duplicate
      Preconditions.checkArgument(seen.add(dep.getName()), String.format("duplicate dependency"
          + ".name %s found, dependency.name should be unique", dep.getName()));
    }
  }

  @Override
  public String toString() {
    return "FlowTrigger{" +
        "schedule=" + this.schedule +
        ", maxWaitDurationInMins=" + this.maxWaitDuration +
        ", executionOptions=" + this.executionOptions +
        "\n " + StringUtils.join(this.dependencies.values(), "\n") + '}';
  }

  /**
   * check uniqueness of dependency type and params
   */
  private void validateDepDefinitionUniqueness(final List<FlowTriggerDependency> dependencies) {
    final Set<String> seen = new HashSet<>();
    for (final FlowTriggerDependency dep : dependencies) {
      final Map<String, String> props = dep.getProps();
      // set.add() returns false when there exists duplicate
      Preconditions.checkArgument(seen.add(dep.getType() + ":" + props.toString()), String.format
          ("duplicate dependency config %s found, dependency config should be unique",
              dep.getName()));
    }
  }

  private void validateDependencies(final List<FlowTriggerDependency> dependencies) {
    validateDepNameUniqueness(dependencies);
    validateDepDefinitionUniqueness(dependencies);
  }

  public FlowTriggerDependency getDependencyByName(final String name) {
    return this.dependencies.get(name);
  }

  public Collection<FlowTriggerDependency> getDependencies() {
    return this.dependencies.values();
  }

  public Optional<Duration> getMaxWaitDuration() {
    return Optional.ofNullable(this.maxWaitDuration);
  }

  public CronSchedule getSchedule() {
    return this.schedule;
  }

  public ExecutionOptions getExecutionOptions() {
    return this.executionOptions;
  }
}
