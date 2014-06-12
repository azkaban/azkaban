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

import java.util.ArrayList;
import java.util.HashSet;

public class EventHandler {
  private HashSet<EventListener> listeners = new HashSet<EventListener>();

  public EventHandler() {
  }

  public void addListener(EventListener listener) {
    listeners.add(listener);
  }

  public void fireEventListeners(Event event) {
    ArrayList<EventListener> listeners =
        new ArrayList<EventListener>(this.listeners);
    for (EventListener listener : listeners) {
      listener.handleEvent(event);
    }
  }

  public void removeListener(EventListener listener) {
    listeners.remove(listener);
  }
}
