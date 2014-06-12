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

package azkaban.trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockTriggerLoader implements TriggerLoader {

  Map<Integer, Trigger> triggers = new HashMap<Integer, Trigger>();
  int triggerCount = 0;

  @Override
  public synchronized void addTrigger(Trigger t) throws TriggerLoaderException {
    t.setTriggerId(triggerCount);
    t.setLastModifyTime(System.currentTimeMillis());
    triggers.put(t.getTriggerId(), t);
    triggerCount++;
  }

  @Override
  public synchronized void removeTrigger(Trigger s)
      throws TriggerLoaderException {
    triggers.remove(s);
  }

  @Override
  public synchronized void updateTrigger(Trigger t)
      throws TriggerLoaderException {
    t.setLastModifyTime(System.currentTimeMillis());
    triggers.put(t.getTriggerId(), t);
  }

  @Override
  public synchronized List<Trigger> loadTriggers()
      throws TriggerLoaderException {
    return new ArrayList<Trigger>(triggers.values());
  }

  @Override
  public synchronized Trigger loadTrigger(int triggerId)
      throws TriggerLoaderException {
    return triggers.get(triggerId);
  }

  @Override
  public List<Trigger> getUpdatedTriggers(long lastUpdateTime)
      throws TriggerLoaderException {
    // TODO Auto-generated method stub
    return null;
  }

}
