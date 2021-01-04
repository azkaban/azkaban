/*
 * Copyright 2018 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A concurrent hash map with a case-insensitive string key.
 *
 * @param <V> the value type
 */
public class CaseInsensitiveConcurrentHashMap<V> {

  private final ConcurrentHashMap<String, V> map = new ConcurrentHashMap<>();

  public V put(final String key, final V value) {
    return this.map.put(key.toLowerCase(), value);
  }

  public V get(final String key) {
    return this.map.get(key.toLowerCase());
  }

  public boolean containsKey(final String key) {
    return this.map.containsKey(key.toLowerCase());
  }

  public V remove(final String key) {
    return this.map.remove(key.toLowerCase());
  }

  public List<String> getKeys() {
    return new ArrayList<>(this.map.keySet());
  }
}
