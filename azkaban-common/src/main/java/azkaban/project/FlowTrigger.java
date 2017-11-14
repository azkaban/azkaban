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

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FlowTrigger is the logical representation of a trigger.
 * It couldn't be changed once gets constructed.
 * It will be used to create running trigger instance.
 */
public class FlowTrigger {

  private final List<FlowTriggerDependency> dependencies;
  private final CronSchedule schedule;
  private final Duration maxWaitDuration;

  /**
   * @throws IllegalArgumentException if any of the argument is null or there is duplicate
   * dependency name or duplicate dependency type and params
   */
  public FlowTrigger(final CronSchedule schedule,
      final List<FlowTriggerDependency> dependencies, final Duration maxWaitDuration) {
    Preconditions.checkArgument(schedule != null);
    Preconditions.checkArgument(dependencies != null);
    Preconditions.checkArgument(maxWaitDuration != null);
    Preconditions.checkArgument(!maxWaitDuration.isNegative());
    validateDependencies(dependencies);
    this.schedule = schedule;
    this.dependencies = Collections.unmodifiableList(dependencies);
    this.maxWaitDuration = maxWaitDuration;
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

  /**
   * check uniqueness of dependency type and params
   */
  private void validateDepDefinitionUniqueness(final List<FlowTriggerDependency> dependencies) {
    final Set<String> seen = new HashSet<>();
    for (final FlowTriggerDependency dep : dependencies) {
      final Map<String, String> props = dep.getProps();
      // set.add() returns false when there exists duplicate
      Preconditions.checkArgument(seen.add(dep.getType() + ":" + props.toString()), String.format
          ("duplicate "
              + "dependency"
              + "config %s found, dependency config should be unique", dep.getName()));
    }
  }

  private void validateDependencies(final List<FlowTriggerDependency> dependencies) {
    validateDepNameUniqueness(dependencies);
    validateDepDefinitionUniqueness(dependencies);
  }

  @Override
  public String toString() {
    return "FlowTrigger{" +
        "dependencies=" + this.dependencies +
        ", schedule=" + this.schedule +
        ", maxWaitDuration=" + this.maxWaitDuration +
        '}';
  }

  public List<FlowTriggerDependency> getDependencies() {
    return this.dependencies;
  }

  public Duration getMaxWaitDuration() {
    return this.maxWaitDuration;
  }

  public CronSchedule getSchedule() {
    return this.schedule;
  }
}
