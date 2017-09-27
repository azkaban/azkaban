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

public interface DependencyCheck {


  /**
   * Non-blocking run of dependency check
   *
   * @return context of the running dependency.
   */
  DependencyInstanceContext run(DependencyInstanceConfig config);

  /**
   * Kill the dependency instance
   */
  void kill(DependencyInstanceContext depContext);

  /**
   * Shutdown the dependency plugin. Clean up resource if needed.
   */
  void shutdown();

  /**
   * Initialize the dependency plugin.
   *
   * @param config dependency plugin config.
   * @param successCallback callback to invoke when the check succeeds.
   */
  void init(DependencyPluginConfig config, SuccessCallback successCallback);
}
