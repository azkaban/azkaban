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

package azkaban.event;

public class Event {
  public enum Type {
    FLOW_STARTED,
    FLOW_FINISHED,
    JOB_STARTED,
    JOB_FINISHED,
    JOB_STATUS_CHANGED,
    EXTERNAL_FLOW_UPDATED,
    EXTERNAL_JOB_UPDATED
  }

  private final Object runner;
  private final Type type;
  private final Object eventData;
  private final long time;
  private final boolean shouldUpdate;

  private Event(Object runner, Type type, Object eventData, boolean shouldUpdate) {
    this.runner = runner;
    this.type = type;
    this.eventData = eventData;
    this.time = System.currentTimeMillis();
    this.shouldUpdate = shouldUpdate;
  }

  public Object getRunner() {
    return runner;
  }

  public Type getType() {
    return type;
  }

  public long getTime() {
    return time;
  }

  public Object getData() {
    return eventData;
  }

  public static Event create(Object runner, Type type) {
    return new Event(runner, type, null, true);
  }

  public static Event create(Object runner, Type type, Object eventData) {
    return new Event(runner, type, eventData, true);
  }

  public static Event create(Object runner, Type type, Object eventData,
      boolean shouldUpdate) {
    return new Event(runner, type, eventData, shouldUpdate);
  }

  public boolean isShouldUpdate() {
    return shouldUpdate;
  }
}
