/*
 * Copyright 2017 LinkedIn Corp.
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
import java.util.HashMap;
import java.util.Map;


/**
 * This class MultitonListenerSet, leveraging multiton design pattern, is aimed at wrapping
 * varieties of Event listeners as a singleton style. The goal is to have multiple singleton
 * listeners.
 *
 * TODO: AS {@link EventHandler} interface can not be wholly changed, we have to let listeners
 * as a type of HashSet<EventListener>. We could refactor them in future.
 */
public class MultitonListenerSet {

  /**
   * Served for recognizing which listener Type.
   */
  public enum ListenerType {
    WEB, EXEC, COMMON
  }

  private static final Map<ListenerType, MultitonListenerSet> _instances = new HashMap<>();
  private HashSet<EventListener> listeners = new HashSet<>();

  private MultitonListenerSet(EventListener listener) {

    /**
     * TODO: Add other listen type if needed. Then the arguments could ba a list of listener.
     * Not sure what other listener type could be added fow now.
     */
    listeners.add(listener);
  }

  public static synchronized MultitonListenerSet getInstance(ListenerType key, EventListener listener) {
    MultitonListenerSet instance = _instances.get(key);

    if (instance == null) {
      // Lazily create instance
      instance = new MultitonListenerSet(listener);

      // Add it to map
      _instances.put(key, instance);
    }
    return instance;
  }

  public HashSet<EventListener> getListeners() {
    return listeners;
  }
}
