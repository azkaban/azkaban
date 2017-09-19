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

import azkaban.Constants;
import azkaban.utils.Props;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

/**
 * FlowTrigger is an immutable class which holds
 * all the data and properties of a flow trigger.
 */

public class FlowTrigger {

  private final List<FlowTriggerDependency> dependencies;
  private final Props props; // trigger level props

  private FlowTrigger(final Props props, final List<FlowTriggerDependency> dependencies) {
    validateProps(props);
    validateDependencies(dependencies);
    this.props = new Props(props.getParent(), props);
    this.dependencies = Collections.unmodifiableList(dependencies);
  }

  private void validateProps(final Props props) {
    Preconditions.checkNotNull(props, "props shouldn't be null");
    final String MISSING_REQUIRED_ERROR = "missing required param: %s";

    final Set<String> requiredParam = ImmutableSet.of(Constants.TriggerProperties.SCHEDULE_TYPE,
        Constants.TriggerProperties.SCHEDULE_VALUE,
        Constants.TriggerProperties.SCHEDULE_MAX_WAIT_TIME);

    for (final String param : requiredParam) {
      Preconditions.checkArgument(props.containsKey(param), String.format(MISSING_REQUIRED_ERROR,
          param));
    }

    Preconditions.checkArgument(props.size() == 3, String.format("invalid param found, allowed "
        + "params: %s", StringUtils.join(requiredParam, ",")));

    //todo chengren311: validate schedule type, value, and max wait time
  }

  private void validateDependencies(final List<FlowTriggerDependency> dependencies) {
    Preconditions.checkNotNull(dependencies);
    // at least one dependency for a trigger
    Preconditions.checkArgument(!dependencies.isEmpty(), "no dependencies found");

    // check uniqueness of dependency.name
    Set<String> seen = Sets.newHashSet();
    for (final FlowTriggerDependency dep : dependencies) {
      Preconditions.checkArgument(seen.add(dep.getName()), String.format("duplicate dependency"
          + ".name %s found, dependency.name should be unique", dep.getName()));
    }

    // check uniqueness of dependency config
    seen = Sets.newHashSet();
    for (final FlowTriggerDependency dep : dependencies) {
      final Props props = dep.getProps();
      props.removeLocal(Constants.DependencyProperties.DEPENDENCY_NAME);
      Preconditions.checkArgument(seen.add(props.toString()), String.format("duplicate dependency"
          + "config %s found, dependency config should be unique", dep.getName()));
    }
  }

  @Override
  public String toString() {
    return "FlowTrigger{" +
        "dependencies=" + this.dependencies +
        ", props=" + this.props +
        '}';
  }

  public List<FlowTriggerDependency> getDependencies() {
    return this.dependencies;
  }

  public static class FlowTriggerBuilder {

    private final List<FlowTriggerDependency> dependencies;
    private final Props props; // trigger level props

    public FlowTriggerBuilder(final Props props) {
      this.props = props;
      this.dependencies = Lists.newArrayList();
    }

    public FlowTriggerBuilder addDependency(final FlowTriggerDependency dep) {
      this.dependencies.add(dep);
      return this;
    }

    public FlowTrigger build() {
      return new FlowTrigger(this.props, this.dependencies);
    }
  }
}
