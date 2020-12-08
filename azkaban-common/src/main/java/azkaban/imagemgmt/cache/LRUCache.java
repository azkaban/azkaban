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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This abstract class defines LRU cache base implementation. The LRU cache is created based on
 * LinkedHashMap implementation. The concrete cache implementation should extend this class to
 * provide complete implementation of the required cache by supplying required metadata for cache.
 *
 * @param <K>
 * @param <V>
 */
public abstract class LRUCache<K, V> implements BaseCache<K, V> {

  private final int initialCapacity;
  private final int maximumCapacity;
  private static final int DEFAULT_INITIAL_CAPACITY = 16;
  private static final float DEFAULT_LOAD_FACTOR = .99f;
  private static final int DEFAULT_MAXIMUM_CAPACITY = 100;

  private final Map<K, V> cache;

  /**
   * Constructor for creating cache based on supplied initialCapacity, loadFactor and
   * maximumCapacity.
   *
   * @param initialCapacity
   * @param loadFactor
   * @param maximumCapacity
   */
  public LRUCache(final int initialCapacity, final float loadFactor, final int maximumCapacity) {
    this.initialCapacity = initialCapacity;
    this.maximumCapacity = maximumCapacity;
    /**
     * Initialize cache based on LinkedHashMap. Remove the oldest entry from the cache if the
     * cache size exceeds maximum capacity.
     */
    this.cache = new LinkedHashMap<K, V>(initialCapacity, loadFactor, true) {
      @Override
      protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        return size() > maximumCapacity;
      }
    };
  }

  /**
   * Constructor for creating cache based on maximumCapacity.
   *
   * @param maximumCapacity
   */
  public LRUCache(final int maximumCapacity) {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, maximumCapacity);
  }

  /**
   * Default constructor.
   */
  public LRUCache() {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_MAXIMUM_CAPACITY);
  }

  @Override
  public V get(final K key) {
    return this.cache.get(key);
  }

  @Override
  public void add(final K key, final V value) {
    this.cache.put(key, value);
  }

  @Override
  public void remove(final K key) {
    this.cache.remove(key);
  }

  /**
   * Gets the size of the internal cache.
   *
   * @return int
   */
  protected int size() {
    return this.cache.size();
  }

  /**
   * Gets the internal cache reference.
   *
   * @return Map<K, V>
   */
  protected Map<K, V> getCache() {
    return this.cache;
  }
}
