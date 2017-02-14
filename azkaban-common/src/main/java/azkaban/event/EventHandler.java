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

import java.util.HashSet;

/**
 * Due to History legacy, this interface is being used by many other modules, and we can not
 * entirely refactor this interface.
 * Having default interface methods helps implementation classes easily write necessary logics.
 *
 * TODO: Refactor return type in getListeners method.
 */
public interface EventHandler {

  HashSet<EventListener> getListeners();

  default void addListener(EventListener listener) {
    getListeners().add(listener);
  }

  default void fireEventListeners(Event event) {

    synchronized (this) {
      for (EventListener listener : getListeners()) {
        listener.handleEvent(event);
      }
    }
  }

  default void removeListener(EventListener listener) {
    getListeners().remove(listener);
  }
}
