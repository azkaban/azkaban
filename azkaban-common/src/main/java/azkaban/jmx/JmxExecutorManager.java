/*
 * Copyright 2014 LinkedIn Corp.
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

import azkaban.executor.ExecutorManager;
import java.util.ArrayList;
import java.util.List;

public class JmxExecutorManager implements JmxExecutorManagerMBean {

  private final ExecutorManager manager;

  public JmxExecutorManager(final ExecutorManager manager) {
    this.manager = manager;
  }

  @Override
  public int getNumRunningFlows() {
    return this.manager.getRunningFlowIds().size();
  }

  @Override
  public String getExecutorThreadState() {
    return this.manager.getExecutorManagerThreadState().toString();
  }

  @Override
  public String getExecutorThreadStage() {
    return this.manager.getExecutorThreadStage();
  }

  @Override
  public boolean isThreadActive() {
    return this.manager.isExecutorManagerThreadActive();
  }

  @Override
  public Long getLastThreadCheckTime() {
    return this.manager.getLastExecutorManagerThreadCheckTime();
  }

  @Override
  public List<String> getPrimaryExecutorHostPorts() {
    return new ArrayList<>(this.manager.getPrimaryServerHosts());
  }

  @Override
  public String getRunningFlows() {
    return this.manager.getRunningFlowIds().toString();
  }

  @Override
  public boolean isQueueProcessorActive() {
    return this.manager.isQueueProcessorThreadActive();
  }

  @Override
  public String getQueuedFlows() {
    return this.manager.getQueuedFlowIds();
  }

  @Override
  public String getQueueProcessorThreadState() {
    return this.manager.getQueueProcessorThreadState().toString();
  }

  @Override
  public List<String> getAvailableExecutorComparatorNames() {
    return new ArrayList<>(this.manager.getAvailableExecutorComparatorNames());
  }

  @Override
  public List<String> getAvailableExecutorFilterNames() {
    return new ArrayList<>(this.manager.getAvailableExecutorFilterNames());
  }

  @Override
  public long getLastSuccessfulExecutorInfoRefresh() {
    return this.manager.getLastSuccessfulExecutorInfoRefresh();
  }

}
