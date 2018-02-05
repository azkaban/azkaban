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
import java.util.Map;
import org.assertj.core.util.Lists;

public class TestUtil {

  public static FlowTriggerDependency createTestDependency(final String name, final long
      runtimeInSec, final boolean boundToFail) {
    final Map<String, String> props = new HashMap<>();
    props.put(TestDependencyCheck.RUN_TIME, String.valueOf(runtimeInSec));
    props.put(TestDependencyCheck.FAILURE_FLAG, String.valueOf(boundToFail));
    return new FlowTriggerDependency(name, "TestDependencyCheck", props);
  }

  public static FlowTrigger createTestFlowTrigger(final int maxWaitMin) {
    final FlowTrigger flowTrigger = new FlowTrigger(
        new CronSchedule("* * * * ? *"),
        Lists.newArrayList(createTestDependency("10secs", 10, false), createTestDependency
            ("65secs", 65, false), createTestDependency("66secs", 66, false)),
        Duration.ofMinutes(maxWaitMin)
    );
    return flowTrigger;
  }

  public static FlowTrigger createFailedTestFlowTrigger(final int maxWaitMin) {
    final FlowTrigger flowTrigger = new FlowTrigger(
        new CronSchedule("* * * * ? *"),
        Lists.newArrayList(createTestDependency("10secs", 10, false), createTestDependency
                ("65secs", 65, false), createTestDependency("66secs", 66, false),
            createTestDependency("15secs", 15, true)),
        Duration.ofMinutes(maxWaitMin)
    );
    return flowTrigger;
  }
}
