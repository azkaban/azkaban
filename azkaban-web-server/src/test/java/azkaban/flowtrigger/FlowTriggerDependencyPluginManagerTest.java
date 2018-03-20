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

import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginManager;
import java.net.URL;
import org.junit.BeforeClass;
import org.junit.Test;


public class FlowTriggerDependencyPluginManagerTest {

  private static final String pluginDir = "dependencyplugin";
  private static FlowTriggerDependencyPluginManager pluginManager;

  @BeforeClass
  public static void setup() throws Exception {
    final URL url = FlowTriggerDependencyPluginManagerTest.class.getClassLoader()
        .getResource(pluginDir);
    pluginManager = new FlowTriggerDependencyPluginManager(url.getPath());
    pluginManager.loadAllPlugins();
  }


  @Test
  public void testPluginLoading() {
    assertThat(pluginManager.getDependencyCheck("test")).isNotNull();
    assertThat(pluginManager.getDependencyCheck("test2")).isNotNull();

    // class loader of pluginManager.getDependencyCheck("test") is different from
    // loader of FakeDependencyCheck1, so
    // assertThat(pluginManager.getDependencyCheck("test")).isInstanceOf(FakeDependencyCheck1)
    // will return false.
    assertThat(pluginManager.getDependencyCheck("test").getClass().getCanonicalName()).isEqualTo
        ("azkaban.flowtrigger.testplugin.FakeDependencyCheck1");

    assertThat(pluginManager.getDependencyCheck("test2").getClass().getCanonicalName()).isEqualTo
        ("azkaban.flowtrigger.testplugin.FakeDependencyCheck2");

    //verify the dependency check contains the specified properties
    assertThat(pluginManager.getDependencyCheck("test").toString()).contains("kafka.url=123");
    assertThat(pluginManager.getDependencyCheck("test").toString()).contains("kafka.port=1234");
    //verify the dependency check contains the specified private properties
    assertThat(pluginManager.getDependencyCheck("test2").toString()).contains("password=1234");
  }
}


