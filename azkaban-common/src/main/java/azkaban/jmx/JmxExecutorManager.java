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

import java.util.ArrayList;
import java.util.List;

import azkaban.executor.ExecutorManager;

public class JmxExecutorManager implements JmxExecutorManagerMBean {
  private ExecutorManager manager;

  public JmxExecutorManager(ExecutorManager manager) {
    this.manager = manager;
  }

  @Override
  public int getNumRunningFlows() {
    return this.manager.getRunningFlows().size();
  }

  @Override
  public String getExecutorThreadState() {
    return manager.getExecutorManagerThreadState().toString();
  }

  @Override
  public String getExecutorThreadStage() {
    return manager.getExecutorThreadStage();
  }

  @Override
  public boolean isThreadActive() {
    return manager.isExecutorManagerThreadActive();
  }

  @Override
  public Long getLastThreadCheckTime() {
    return manager.getLastExecutorManagerThreadCheckTime();
  }

  @Override
  public List<String> getPrimaryExecutorHostPorts() {
    return new ArrayList<String>(manager.getPrimaryServerHosts());
  }

  @Override
  public String getRunningFlows() {
    return manager.getRunningFlowIds();
  }

  @Override
  public boolean isQueueProcessorActive() {
    return manager.isQueueProcessorThreadActive();
  }

  @Override
  public String getQueuedFlows() {
    return manager.getQueuedFlowIds();
  }

  @Override
  public String getQueueProcessorThreadState() {
    return manager.getQueueProcessorThreadState().toString();
  }

  @Override
  public List<String> getAvailableExecutorComparatorNames() {
    return new ArrayList<String>(manager.getAvailableExecutorComparatorNames());
  }

  @Override
  public List<String> getAvailableExecutorFilterNames() {
    return new ArrayList<String>(manager.getAvailableExecutorFilterNames());
  }

  @Override
  public long getLastSuccessfulExecutorInfoRefresh() {
    return manager.getLastSuccessfulExecutorInfoRefresh();
  }

}
