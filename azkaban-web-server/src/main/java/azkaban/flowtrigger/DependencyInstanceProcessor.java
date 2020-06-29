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

import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DependencyInstanceProcessor {

  private final FlowTriggerInstanceLoader flowTriggerInstanceLoader;

  @Inject
  public DependencyInstanceProcessor(final FlowTriggerInstanceLoader depLoader) {
    this.flowTriggerInstanceLoader = depLoader;
  }

  /**
   * Process status update of dependency instance
   */
  public void processStatusUpdate(final DependencyInstance depInst) {
    //this is blocking call, might offload it to another thread if necessary.
    this.flowTriggerInstanceLoader.updateDependencyExecutionStatus(depInst);
  }
}
