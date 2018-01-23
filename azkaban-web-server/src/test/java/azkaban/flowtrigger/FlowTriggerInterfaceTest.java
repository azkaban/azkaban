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

package azkaban.flowtrigger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class FlowTriggerInterfaceTest {

  private final Map<String, String> testHashMap;

  {
    this.testHashMap = new HashMap<>();
    this.testHashMap.put("key1", "value1");
    this.testHashMap.put("key2", "value2");
  }


  @Test
  public void testDependencyInstanceRuntimeProps() {
    final DependencyInstanceRuntimeProps runtimeProps = new DependencyInstanceRuntimePropsImpl
        (this.testHashMap);
    assertThat(runtimeProps.get("key1")).isEqualTo("value1");
    assertThat(runtimeProps.get("key2")).isEqualTo("value2");
    assertThat(runtimeProps.get("nonexistingkey")).isNull();
  }

  @Test
  public void testDependencyPluginConfig() {
    final DependencyPluginConfig pluginConfig = new DependencyPluginConfigImpl(this.testHashMap);
    assertThat(pluginConfig.get("key1")).isEqualTo("value1");
    assertThat(pluginConfig.get("key2")).isEqualTo("value2");
    assertThat(pluginConfig.get("nonexistingkey")).isNull();
  }

  @Test
  public void testDependencyInstanceConfig() {
    final DependencyInstanceConfig instanceConfig = new DependencyInstanceConfigImpl(this
        .testHashMap);
    assertThat(instanceConfig.get("key1")).isEqualTo("value1");
    assertThat(instanceConfig.get("key2")).isEqualTo("value2");
    assertThat(instanceConfig.get("nonexistingkey")).isNull();
  }
}
