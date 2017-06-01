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

  Map<Integer, Trigger> triggers = new HashMap<>();
  int triggerCount = 0;

  @Override
  public synchronized void addTrigger(final Trigger t) throws TriggerLoaderException {
    t.setTriggerId(this.triggerCount);
    t.setLastModifyTime(System.currentTimeMillis());
    this.triggers.put(t.getTriggerId(), t);
    this.triggerCount++;
  }

  @Override
  public synchronized void removeTrigger(final Trigger s)
      throws TriggerLoaderException {
    this.triggers.remove(s.getTriggerId());
  }

  @Override
  public synchronized void updateTrigger(final Trigger t)
      throws TriggerLoaderException {
    t.setLastModifyTime(System.currentTimeMillis());
    this.triggers.put(t.getTriggerId(), t);
  }

  @Override
  public synchronized List<Trigger> loadTriggers()
      throws TriggerLoaderException {
    return new ArrayList<>(this.triggers.values());
  }

  @Override
  public synchronized Trigger loadTrigger(final int triggerId)
      throws TriggerLoaderException {
    return this.triggers.get(triggerId);
  }

  @Override
  public List<Trigger> getUpdatedTriggers(final long lastUpdateTime)
      throws TriggerLoaderException {
    // TODO Auto-generated method stub
    return null;
  }

}
