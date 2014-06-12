/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.trigger;

import java.util.List;
import java.util.Map;

public interface TriggerManagerAdapter {

  public void insertTrigger(Trigger t, String user)
      throws TriggerManagerException;

  public void removeTrigger(int id, String user) throws TriggerManagerException;

  public void updateTrigger(Trigger t, String user)
      throws TriggerManagerException;

  public List<Trigger> getAllTriggerUpdates(long lastUpdateTime)
      throws TriggerManagerException;

  public List<Trigger> getTriggerUpdates(String triggerSource,
      long lastUpdateTime) throws TriggerManagerException;

  public List<Trigger> getTriggers(String trigegerSource);

  public void start() throws TriggerManagerException;

  public void shutdown();

  public void registerCheckerType(String name,
      Class<? extends ConditionChecker> checker);

  public void registerActionType(String name,
      Class<? extends TriggerAction> action);

  public TriggerJMX getJMX();

  public interface TriggerJMX {
    public long getLastRunnerThreadCheckTime();

    public boolean isRunnerThreadActive();

    public String getPrimaryServerHost();

    public int getNumTriggers();

    public String getTriggerSources();

    public String getTriggerIds();

    public long getScannerIdleTime();

    public Map<String, Object> getAllJMXMbeans();

    public String getScannerThreadStage();
  }

}
