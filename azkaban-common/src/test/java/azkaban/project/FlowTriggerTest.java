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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.scheduler.Schedule;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.joda.time.DateTimeZone;
import org.junit.Test;


public class FlowTriggerTest {

  private FlowTriggerDependency createUniqueTestDependency(final String type) {
    final UUID uuid = UUID.randomUUID();
    return createTestDependency(type, uuid.toString());
  }

  private FlowTriggerDependency createTestDependency(final String type, final String name) {
    final FlowTriggerDependency dep = new FlowTriggerDependency(name, type, new HashMap<>());
    return dep;
  }

  @Test
  public void testScheduleArgumentValidation() {
    assertThatThrownBy(() -> new CronSchedule(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFlowTriggerArgumentValidation() {
    final CronSchedule validSchedule = new CronSchedule("* * * * ? *");
    final CronSchedule nullSchedule = null;
    final List<FlowTriggerDependency> emptyDependencyList = new ArrayList<>();
    final List<FlowTriggerDependency> nullDependencyList = null;

    final List<FlowTriggerDependency> nonEmptyDependencyList = Arrays.asList(createTestDependency
        ("type", "dep1"));

    final Duration validDuration = Duration.ofMinutes(10);
    final Duration nullDuration = null;

    assertThatThrownBy(() -> new FlowTrigger(nullSchedule, nonEmptyDependencyList, validDuration))
        .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> new FlowTrigger(validSchedule, nullDependencyList, validDuration))
        .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> new FlowTrigger(validSchedule, nonEmptyDependencyList, nullDuration))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatCode(() -> new FlowTrigger(validSchedule, emptyDependencyList, nullDuration))
        .doesNotThrowAnyException();
  }

  @Test
  public void testDuplicateDependencies() {
    final FlowTriggerDependency dep = createUniqueTestDependency("type");

    final CronSchedule schedule = new CronSchedule("* * * * ? *");
    final List<FlowTriggerDependency> dependencyList = new ArrayList<>();
    dependencyList.add(dep);
    dependencyList.add(dep);

    assertThatThrownBy(() -> new FlowTrigger(schedule, dependencyList, Duration.ofMinutes(10)))
        .isInstanceOf
            (IllegalArgumentException
                .class)
        .hasMessageContaining("dependency.name should be unique");
  }

  @Test
  public void testTimeZone() {
    TimeZone timezone1 = TimeZone.getTimeZone("EST");
    TimeZone timezone2 = TimeZone.getTimeZone("PST");
    String cronExpression = "* * * * ? *";
    final CronSchedule schedule = new CronSchedule(cronExpression, timezone1.getID());
    final List<FlowTriggerDependency> dependencyList = new ArrayList<>();
    assertThat(schedule.getTimeZone()).isEqualTo(timezone1.getID());
    assertThat(
        schedule.equals(new CronSchedule(cronExpression, timezone1.getID()))
    ).isEqualTo(true);
    assertThat(
        schedule.equals(new CronSchedule(cronExpression, timezone2.getID()))
    ).isEqualTo(false);

    assertThatCode(() -> new FlowTrigger(schedule, dependencyList, Duration.ofMinutes(10)))
        .doesNotThrowAnyException();
  }

  @Test
  public void testTimeZoneSchedule() {
    DateTimeZone timezone1 = DateTimeZone.forTimeZone(TimeZone.getTimeZone("EST"));
    DateTimeZone timezone2 = DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"));
    long time = System.currentTimeMillis() + 30;
    String cronExpression = "* * * * ? *";
    final Schedule schedule1 = new Schedule(3, 3, "anotherTestProject", "anotherFlow", "ready",
        time, time, timezone1, null, time,
        time, time, "testUser3", null,
        cronExpression);
    final Schedule schedule2 = new Schedule(3, 3, "anotherTestProject", "anotherFlow", "ready",
        time, time, timezone2, null, time,
        time, time, "testUser3", null,
        cronExpression);
    assertThat(schedule1.getTimezone().getID()).isEqualTo(timezone1.getID());
    assertThat(schedule1.equals(schedule2)).isEqualTo(false);
    assertThat(schedule1.toString().contains(cronExpression)).isEqualTo(true);
    assertThat(schedule1.toString().contains(timezone1.getID())).isEqualTo(true);
  }

  @Test
  public void testDifferentDepNameSameDepConfig() {
    final FlowTriggerDependency dep1 = createTestDependency("type", "dep1");
    final FlowTriggerDependency dep2 = createTestDependency("type", "dep2");

    final CronSchedule schedule = new CronSchedule("* * * * ? *");
    final List<FlowTriggerDependency> dependencyList = new ArrayList<>();
    dependencyList.add(dep1);
    dependencyList.add(dep2);

    assertThatThrownBy(() -> new FlowTrigger(schedule, dependencyList, Duration.ofMinutes(10)))
        .isInstanceOf
            (IllegalArgumentException
                .class)
        .hasMessageContaining("dependency config should be unique");
  }
}
