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

import azkaban.utils.Props;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * FlowTrigger is an immutable class
 * which contains a list of FlowTriggerDependency and a schedule.
 */
public class FlowTrigger {

  private final List<FlowTriggerDependency> dependencies;
  private final FlowTriggerSchedule schedule;

  private FlowTrigger(final FlowTriggerSchedule schedule,
      final List<FlowTriggerDependency> dependencies) {
    validateDependencies(dependencies);
    this.schedule = schedule;
    this.dependencies = Collections.unmodifiableList(dependencies);
  }


  /**
   * check uniqueness of dependency.name
   */
  private void validateDepNameUniqueness() {
    final Set<String> seen = new HashSet<>();
    for (final FlowTriggerDependency dep : this.dependencies) {
      // set.add() returns false when there exists duplicate
      Preconditions.checkArgument(seen.add(dep.getName()), String.format("duplicate dependency"
          + ".name %s found, dependency.name should be unique", dep.getName()));
    }
  }

  /**
   * check uniqueness of dependency type and params
   */
  private void validateDepDefinitionUniqueness() {
    final Set<String> seen = new HashSet<>();
    for (final FlowTriggerDependency dep : this.dependencies) {
      final Props props = dep.getPropsCopy();
      // set.add() returns false when there exists duplicate
      Preconditions.checkArgument(seen.add(dep.getType() + ":" + props.toString()), String.format
          ("duplicate "
              + "dependency"
              + "config %s found, dependency config should be unique", dep.getName()));
    }
  }

  private void validateDependencies(final List<FlowTriggerDependency> dependencies) {
    Preconditions.checkNotNull(dependencies);
    validateDepNameUniqueness();
    validateDepDefinitionUniqueness();
  }

  @Override
  public String toString() {
    return "FlowTrigger{" +
        "dependencies=" + this.dependencies +
        ", schedule=" + this.schedule +
        '}';
  }

  public List<FlowTriggerDependency> getDependencies() {
    return this.dependencies;
  }

  public static class FlowTriggerBuilder {

    private final List<FlowTriggerDependency> dependencies;
    private final FlowTriggerSchedule schedule;

    public FlowTriggerBuilder(final FlowTriggerSchedule schedule) {
      this.schedule = schedule;
      this.dependencies = new ArrayList<>();
    }

    public FlowTriggerBuilder addDependency(final FlowTriggerDependency dep) {
      this.dependencies.add(dep);
      return this;
    }

    public FlowTrigger build() {
      return new FlowTrigger(this.schedule, this.dependencies);
    }
  }
}
