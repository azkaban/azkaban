/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.jmx;

import azkaban.executor.container.ContainerizedExecutionManager;

public class JmxContainerizedExecutionManager implements JmxContainerizedExecutionManagerMBean {

  private final ContainerizedExecutionManager containerizedExecutionManager;

  public JmxContainerizedExecutionManager(final ContainerizedExecutionManager containerizedExecutionManager) {
    this.containerizedExecutionManager = containerizedExecutionManager;
  }
  @Override
  public int getNumRunningFlows() {
    return this.containerizedExecutionManager.getRunningFlows().size();
  }

  @Override
  public String getRunningFlows() {
    return this.containerizedExecutionManager.getRunningFlowIds().toString();
  }

  @Override
  public boolean isQueueProcessorActive() {
    return this.containerizedExecutionManager.isQueueProcessorThreadActive();
  }

  @Override
  public String getQueuedFlows() {
    return this.containerizedExecutionManager.getQueuedFlowIds().toString();
  }

  @Override
  public String getQueueProcessorThreadState() {
    return this.containerizedExecutionManager.getQueueProcessorThreadState().toString();
  }
}

