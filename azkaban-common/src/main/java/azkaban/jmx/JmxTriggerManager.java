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

import azkaban.trigger.TriggerManagerAdapter;
import azkaban.trigger.TriggerManagerAdapter.TriggerJMX;

public class JmxTriggerManager implements JmxTriggerManagerMBean {
  private TriggerJMX jmxStats;

  public JmxTriggerManager(TriggerManagerAdapter manager) {
    this.jmxStats = manager.getJMX();
  }

  @Override
  public long getLastRunnerThreadCheckTime() {
    return jmxStats.getLastRunnerThreadCheckTime();
  }

  @Override
  public boolean isRunnerThreadActive() {
    return jmxStats.isRunnerThreadActive();
  }

  @Override
  public String getPrimaryTriggerHostPort() {
    return jmxStats.getPrimaryServerHost();
  }

  // @Override
  // public List<String> getAllTriggerHostPorts() {
  // return new ArrayList<String>(manager.getAllActiveTriggerServerHosts());
  // }

  @Override
  public int getNumTriggers() {
    return jmxStats.getNumTriggers();
  }

  @Override
  public String getTriggerSources() {
    return jmxStats.getTriggerSources();
  }

  @Override
  public String getTriggerIds() {
    return jmxStats.getTriggerIds();
  }

  @Override
  public long getScannerIdleTime() {
    return jmxStats.getScannerIdleTime();
  }

  @Override
  public String getScannerThreadStage() {
    return jmxStats.getScannerThreadStage();
  }
}
