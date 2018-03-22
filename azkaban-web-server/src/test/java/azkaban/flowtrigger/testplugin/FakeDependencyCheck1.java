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

package azkaban.flowtrigger.testplugin;

import azkaban.flowtrigger.DependencyCheck;
import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyInstanceRuntimeProps;
import azkaban.flowtrigger.DependencyPluginConfig;

/**
 * todo chengren311:
 * test-dependency-plugin.jar in resource folder is generated from
 *
 * @see azkaban.flowtrigger.testplugin.FakeDependencyCheck1
 * @see azkaban.flowtrigger.testplugin.FakeDependencyCheck2
 * @see azkaban.flowtrigger.testplugin.FakeDependencyInstanceContext1
 * @see azkaban.flowtrigger.testplugin.FakeDependencyInstanceContext2
 *
 * But we need to find out a way to auto generate this jar while building.
 */
public class FakeDependencyCheck1 implements DependencyCheck {

  private DependencyPluginConfig config;

  @Override
  public DependencyInstanceContext run(final DependencyInstanceConfig config,
      final DependencyInstanceRuntimeProps runtimeProps,
      final DependencyInstanceCallback callback) {
    return new FakeDependencyInstanceContext1(config, runtimeProps, callback);
  }

  @Override
  public void shutdown() {
  }

  @Override
  public String toString() {
    return this.config.toString();
  }

  @Override
  public void init(final DependencyPluginConfig config) {
    this.config = config;
  }
}

