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

import azkaban.executor.container.ContainerizedDispatchImpl;

/**
 * JMX for Containerized execution manager to monitor executions which are dispatched on containers.
 */
public class JmxContainerizedExecutionManager implements JmxContainerizedExecutionManagerMBean {

  private final ContainerizedDispatchImpl containerizedDispatchImpl;

  public JmxContainerizedExecutionManager(final ContainerizedDispatchImpl containerizedDispatchImpl) {
    this.containerizedDispatchImpl = containerizedDispatchImpl;
  }
  @Override
  public int getNumRunningFlows() {
    return this.containerizedDispatchImpl.getRunningFlows().size();
  }

  @Override
  public String getRunningFlows() {
    return this.containerizedDispatchImpl.getRunningFlowIds().toString();
  }

  @Override
  public boolean isQueueProcessorActive() {
    return this.containerizedDispatchImpl.isQueueProcessorThreadActive();
  }

  @Override
  public String getQueuedFlows() {
    return this.containerizedDispatchImpl.getQueuedFlowIds().toString();
  }

  @Override
  public String getQueueProcessorThreadState() {
    return this.containerizedDispatchImpl.getQueueProcessorThreadState().toString();
  }
}

