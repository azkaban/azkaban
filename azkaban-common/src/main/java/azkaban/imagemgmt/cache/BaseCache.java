/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.imagemgmt.cache;

/**
 * This base interface defines some of the base methods of cache.
 *
 * @param <K> Represents key of the cache
 * @param <V> Represents value of the cache
 */
public interface BaseCache<K, V> {

  /**
   * Method for getting the value from the cache based on key.
   *
   * @param key
   */
  public V get(K key);

  /**
   * Method to populate the cache with given key and value.
   *
   * @param key
   * @param value
   */
  public void add(K key, V value);

  /**
   * Method to remove the key from the cache.
   *
   * @param key
   */
  public void remove(K key);
}
