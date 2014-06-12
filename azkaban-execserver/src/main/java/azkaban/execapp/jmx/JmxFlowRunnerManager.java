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

package azkaban.execapp.jmx;

import azkaban.execapp.FlowRunnerManager;

public class JmxFlowRunnerManager implements JmxFlowRunnerManagerMBean {
  private FlowRunnerManager manager;

  public JmxFlowRunnerManager(FlowRunnerManager manager) {
    this.manager = manager;
  }

  @Override
  public long getLastCleanerThreadCheckTime() {
    return manager.getLastCleanerThreadCheckTime();
  }

  @Override
  public long getLastSubmitterThreadCheckTime() {
    return manager.getLastSubmitterThreadCheckTime();
  }

  @Override
  public boolean isSubmitterThreadActive() {
    return manager.isSubmitterThreadActive();
  }

  @Override
  public boolean isCleanerThreadActive() {
    return manager.isCleanerThreadActive();
  }

  @Override
  public String getSubmitterThreadState() {
    return manager.getSubmitterThreadState().toString();
  }

  @Override
  public String getCleanerThreadState() {
    return manager.getCleanerThreadState().toString();
  }

  @Override
  public boolean isExecutorThreadPoolShutdown() {
    return manager.isExecutorThreadPoolShutdown();
  }

  @Override
  public int getNumExecutingFlows() {
    return manager.getNumExecutingFlows();
  }

  @Override
  public int countTotalNumRunningJobs() {
    return manager.getNumExecutingJobs();
  }

  @Override
  public String getRunningFlows() {
    return manager.getRunningFlowIds();
  }

}
