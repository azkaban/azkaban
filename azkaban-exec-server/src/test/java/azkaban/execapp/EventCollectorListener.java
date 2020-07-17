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

package azkaban.execapp;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.spi.EventType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class EventCollectorListener implements EventListener<Event> {

  public static final Object handleEvent = new Object();
  // CopyOnWriteArrayList allows concurrent iteration and modification
  private final List<Event> eventList = new CopyOnWriteArrayList<>();
  private final HashSet<EventType> filterOutTypes = new HashSet<>();

  public void setEventFilterOut(final EventType... types) {
    this.filterOutTypes.addAll(Arrays.asList(types));
  }

  @Override
  public void handleEvent(final Event event) {
    synchronized (handleEvent) {
      handleEvent.notifyAll();
    }
    if (!this.filterOutTypes.contains(event.getType())) {
      this.eventList.add(event);
    }
  }

  public List<Event> getEventList() {
    return this.eventList;
  }

  public boolean checkOrdering() {
    final long time = 0;
    for (final Event event : this.eventList) {
      if (time > event.getTime()) {
        return false;
      }
    }

    return true;
  }

  public void assertEvents(final EventType... expected) {
    final Object[] captured = this.eventList.stream()
        .filter(event -> event.getRunner() != null)
        .map(event -> event.getType())
        .toArray();
    assertThat(captured).isEqualTo(expected);
  }
}
