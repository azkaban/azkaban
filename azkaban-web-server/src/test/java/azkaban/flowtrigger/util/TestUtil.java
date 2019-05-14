/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.flowtrigger.util;

import azkaban.flowtrigger.testplugin.TestDependencyCheck;
import azkaban.project.CronSchedule;
import azkaban.project.FlowTrigger;
import azkaban.project.FlowTriggerDependency;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtil {

  public static FlowTriggerDependency createTestDependency(final String name, final long
      runtimeInSec, final boolean boundToFail) {
    final Map<String, String> props = new HashMap<>();
    props.put(TestDependencyCheck.RUN_TIME, String.valueOf(runtimeInSec));
    props.put(TestDependencyCheck.FAILURE_FLAG, String.valueOf(boundToFail));
    return new FlowTriggerDependency(name, "TestDependencyCheck", props);
  }

  public static FlowTrigger createTestFlowTrigger(final List<FlowTriggerDependency> deps,
      final Duration maxWaitDuration) {
    return new FlowTrigger(
        new CronSchedule("* * * * ? *"), deps, maxWaitDuration, null);
  }
}
