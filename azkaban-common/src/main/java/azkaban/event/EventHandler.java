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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHandler<T> {

  private final HashSet<EventListener<T>> listeners = new HashSet<>();
  private static final Logger logger = LoggerFactory.getLogger(EventHandler.class);

  public EventHandler() {
  }

  public EventHandler addListener(final EventListener<T> listener) {
    this.listeners.add(listener);
    return this;
  }

  public EventHandler addListeners(final EventListener<T>... listeners) {
    for (int i = listeners.length - 1; i >= 0; i--) {
      this.listeners.add(listeners[i]);
    }
    return this;
  }

  public void fireEventListeners(final T event) {
    final ArrayList<EventListener> listeners =
        new ArrayList<>(this.listeners);
    for (final EventListener listener : listeners) {
      try {
        listener.handleEvent(event);
      } catch (RuntimeException e) {
        logger.warn("Error while calling handleEvent for: " + listener.getClass());
        logger.warn(e.getMessage(), e);
      }
    }
  }

  public void removeListener(final EventListener<T> listener) {
    this.listeners.remove(listener);
  }
}
