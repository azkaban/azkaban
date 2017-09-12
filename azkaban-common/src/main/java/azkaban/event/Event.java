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

import azkaban.spi.EventType;
import com.google.common.base.Preconditions;

public class Event {

  private final Object runner;
  private final EventType type;
  private final EventData eventData;
  private final long time;

  private Event(final Object runner, final EventType type, final EventData eventData) {
    this.runner = runner;
    this.type = type;
    this.eventData = eventData;
    this.time = System.currentTimeMillis();
  }

  /**
   * Creates a new event.
   *
   * @param runner runner.
   * @param type type.
   * @param eventData EventData, null is not allowed.
   * @return New Event instance.
   * @throws NullPointerException if EventData is null.
   */
  public static Event create(final Object runner, final EventType type, final EventData eventData)
      throws NullPointerException {
    Preconditions.checkNotNull(eventData, "EventData was null");
    return new Event(runner, type, eventData);
  }

  public Object getRunner() {
    return this.runner;
  }

  public EventType getType() {
    return this.type;
  }

  public long getTime() {
    return this.time;
  }

  public EventData getData() {
    return this.eventData;
  }


}
