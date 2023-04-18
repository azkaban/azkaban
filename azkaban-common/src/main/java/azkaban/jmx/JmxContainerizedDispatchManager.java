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

import azkaban.executor.container.ContainerizedDispatchManager;

/**
 * JMX for Containerized execution manager to monitor executions which are dispatched on containers.
 */
public class JmxContainerizedDispatchManager implements JmxContainerizedDispatchManagerMBean {

  private final ContainerizedDispatchManager containerizedDispatchManager;

  public JmxContainerizedDispatchManager(final ContainerizedDispatchManager containerizedDispatchManager) {
    this.containerizedDispatchManager = containerizedDispatchManager;
  }
  @Override
  public int getNumRunningFlows() {
    return this.containerizedDispatchManager.getRunningFlowIds().size();
  }

  @Override
  public String getRunningFlows() {
    return this.containerizedDispatchManager.getRunningFlowIds().toString();
  }

  @Override
  public boolean isQueueProcessorActive() {
    return this.containerizedDispatchManager.isQueueProcessorThreadActive();
  }

  @Override
  public String getQueuedFlows() {
    return this.containerizedDispatchManager.getQueuedFlowIds().toString();
  }

  @Override
  public String getQueueProcessorThreadState() {
    return this.containerizedDispatchManager.getQueueProcessorThreadState().toString();
  }
}

