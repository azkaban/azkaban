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

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.event.EventListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class EventCollectorListener implements EventListener {

  private final ArrayList<Event> eventList = new ArrayList<>();
  private final HashSet<Event.Type> filterOutTypes = new HashSet<>();

  public void setEventFilterOut(final Event.Type... types) {
    this.filterOutTypes.addAll(Arrays.asList(types));
  }

  @Override
  public void handleEvent(final Event event) {
    if (!this.filterOutTypes.contains(event.getType())) {
      this.eventList.add(event);
    }
  }

  public ArrayList<Event> getEventList() {
    return this.eventList;
  }

  public void writeAllEvents() {
    for (final Event event : this.eventList) {
      System.out.print(event.getType());
      System.out.print(",");
    }
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

  public void checkEventExists(final Type[] types) {
    int index = 0;
    for (final Event event : this.eventList) {
      if (event.getRunner() == null) {
        continue;
      }

      if (index >= types.length) {
        throw new RuntimeException("More events than expected. Got "
            + event.getType());
      }
      final Type type = types[index++];

      if (type != event.getType()) {
        throw new RuntimeException("Got " + event.getType() + ", expected "
            + type + " index:" + index);
      }
    }

    if (types.length != index) {
      throw new RuntimeException("Not enough events.");
    }
  }
}
