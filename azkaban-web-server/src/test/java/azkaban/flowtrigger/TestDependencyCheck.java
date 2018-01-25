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

package azkaban.flowtrigger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDependencyCheck implements DependencyCheck {

  private static final Logger logger = LoggerFactory.getLogger(TestDependencyCheck.class);

  @Override
  public DependencyInstanceContext run(final DependencyInstanceConfig config,
      final DependencyInstanceRuntimeProps runtimeProps,
      final DependencyInstanceCallback callback) {
    logger.info("running TestDependencyCheck with config:" + config);
//    try {
//      Thread.sleep(1000 * 30);
//    } catch (final Exception ex) {
//    }
    logger.info("done with TestDependencyCheck with config:" + config);
    return new TestDependencyInstanceContext(config, runtimeProps, callback);
  }

  @Override
  public void shutdown() {
  }

  @Override
  public void init(final DependencyPluginConfig config) {
    //logger.info("initing TestDependencyCheck with config:" + config);
    System.out.println("initing TestDependencyCheck with config:" + config);
  }
}

