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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import azkaban.executor.ExecutorManagerAdapter;

public class JmxExecutorManagerAdapter implements
    JmxExecutorManagerAdapterMBean {
  private ExecutorManagerAdapter manager;

  public JmxExecutorManagerAdapter(ExecutorManagerAdapter manager) {
    this.manager = manager;
  }

  @Override
  public int getNumRunningFlows() {
    try {
      return this.manager.getRunningFlows().size();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return 0;
    }
  }

  @Override
  public String getExecutorManagerThreadState() {
    return manager.getExecutorManagerThreadState().toString();
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    return manager.isExecutorManagerThreadActive();
  }

  @Override
  public Long getLastExecutorManagerThreadCheckTime() {
    return manager.getLastExecutorManagerThreadCheckTime();
  }

  @Override
  public List<String> getPrimaryExecutorHostPorts() {
    return new ArrayList<String>(manager.getPrimaryServerHosts());
  }

}
