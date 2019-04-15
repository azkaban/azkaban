/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.execapp.lockingcache;

import java.util.Map;

/**
 * Used by the {@link LockingCache} to load values, and do any additional cleanup when a key/value
 * is removed from the cache.
 *
 * @param <K> class type for the key
 * @param <V> class type for the value
 */
public interface LockingCacheLoader<K, V> {

  /**
   * Loads the value for the specified key.
   *
   * @param key the key
   * @return the value
   */
  public V load(K key) throws Exception;

  /**
   * @return a map of initial key/values and their sizes.
   */
  public Map<K, V> loadAll() throws Exception;

  /**
   * Called when a key/value is removed from the cache. This should do any additional cleanup
   * needed when an entry is removed from the cache.
   *
   * @param key the key
   * @param value the value
   */
  public void remove(K key, V value) throws Exception;
}
