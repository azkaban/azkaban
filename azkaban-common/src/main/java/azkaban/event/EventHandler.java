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

  private final HashSet<EventListener> listeners = new HashSet<>();

  public EventHandler() {
  }

  public EventHandler addListener(final EventListener listener) {
    this.listeners.add(listener);
    return this;
  }

  public EventHandler addListeners(final EventListener... listeners) {
    for (int i = listeners.length - 1; i >= 0; i--) {
      this.listeners.add(listeners[i]);
    }
    return this;
  }

  public void fireEventListeners(final Event event) {
    final ArrayList<EventListener> listeners =
        new ArrayList<>(this.listeners);
    for (final EventListener listener : listeners) {
      listener.handleEvent(event);
    }
  }

  public void removeListener(final EventListener listener) {
    this.listeners.remove(listener);
  }
}
