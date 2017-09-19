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


import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.Constants;
import azkaban.utils.Props;
import java.util.UUID;
import org.junit.Test;


public class FlowTriggerTest {

  private FlowTriggerDependency createUniqueTestDependency(final String type) {
    final UUID uuid = UUID.randomUUID();
    return createTestDependency(type, uuid.toString());
  }

  private FlowTriggerDependency createTestDependency(final String type, final String name) {
    final Props depProps = new Props();
    depProps.put(Constants.DependencyProperties.DEPENDENCY_NAME, name);
    depProps.put(Constants.DependencyProperties.DEPENDENCY_TYPE, type);
    final FlowTriggerDependency dep = new FlowTriggerDependency(depProps);
    return dep;
  }

  private FlowTrigger.FlowTriggerBuilder initTrigger() {
    final Props triggerProps = new Props();
    triggerProps.put(Constants.TriggerProperties.SCHEDULE_TYPE, "cron");
    triggerProps.put(Constants.TriggerProperties.SCHEDULE_VALUE, "*");
    triggerProps.put(Constants.TriggerProperties.SCHEDULE_MAX_WAIT_TIME, "1");
    final FlowTrigger.FlowTriggerBuilder builder = new FlowTrigger.FlowTriggerBuilder(triggerProps);
    return builder;
  }

  @Test
  public void testDuplicateDependencies() {
    final Props depProps = new Props();
    depProps.put(Constants.DependencyProperties.DEPENDENCY_NAME, "testdep");
    final FlowTriggerDependency dep = createUniqueTestDependency("intime");

    final FlowTrigger.FlowTriggerBuilder builder = initTrigger();

    builder.addDependency(dep);
    builder.addDependency(dep);

    assertThatThrownBy(() -> builder.build()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dependency.name should be unique");
  }

  @Test
  public void testDifferentDepNameSameDepConfig() {
    final Props depProps = new Props();
    depProps.put(Constants.DependencyProperties.DEPENDENCY_NAME, "testdep");
    final FlowTriggerDependency dep1 = createTestDependency("intime", "dep1");
    final FlowTriggerDependency dep2 = createTestDependency("intime", "dep2");

    final FlowTrigger.FlowTriggerBuilder builder = initTrigger();

    builder.addDependency(dep1);
    builder.addDependency(dep2);

    assertThatThrownBy(() -> builder.build()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dependency config should be unique");
  }
}
