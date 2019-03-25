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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A locking, loading, concurrent cache. Entries are returned with a read lock, as
 * {@link LockingCacheEntry}. While holding the read lock, users are guaranteed that
 * the value is value. Values can be read concurrently by mulitple readers.
 * If a value isn't already in the cache, then it will be loaded with the specified
 * {@link LockingCacheLoader}. There is an option provided for asynchronous cleanup,
 * which will try to maintain the cache between a min and max size, approximately.
 * Entries are only removed if they are not locked (no threads currently holding a read or
 * write lock). {@link LockingCacheSizer} is used to get the size of values, which are used to
 * calculate the size of the cache.
 *
 * @param <K> the class type for the key
 * @param <V> the class type for the value
 * @param <L> the class type for the {@link LockingCacheLoader}
 * @param <S> the class type for the {@link LockingCacheSizer}
 */
public class LockingCache<K, V, L extends LockingCacheLoader<K,V>, S extends LockingCacheSizer<V>> {
  private final ConcurrentHashMap<K, LockingCacheEntry<V>> cacheMap;
  private final AtomicLong cacheSize;
  private final L cacheLoader;
  private final S sizer;
  private final ScheduledExecutorService cleanupPool;
  private static final Logger log = LoggerFactory.getLogger(LockingCache.class);

  /**
   * Constructor, without automated cleanup.
   *
   * @param cacheLoader the {@link LockingCacheLoader} for loading values by key
   * @param sizer the {@link LockingCacheSizer} for getting the size of a value
   */
  public LockingCache(L cacheLoader, S sizer) {
    this(cacheLoader, sizer, 0, 0, 0);
   }

   /**
    * Constructor, with separate auto cleaning thread.
    * @param cacheLoader the {@link LockingCacheLoader} for loading values by key
    * @param sizer the {@link LockingCacheSizer} for getting the size of a value
    * @param minSize the threshold for stopping cleanup
    * @param maxSize the threshold for starting cleanup
    * @param intervalSec the interval in seconds for running cleanup
    */
  public LockingCache(L cacheLoader, S sizer, long minSize, long maxSize, long intervalSec) {
    this.cacheMap = new ConcurrentHashMap<>();
    this.cacheSize = new AtomicLong(0);
    this.cacheLoader = cacheLoader;
    this.sizer = sizer;
    if (intervalSec > 0) {
      // create the cleanup thread
      this.cleanupPool = Executors.newScheduledThreadPool(1);
      cleanupPool.scheduleAtFixedRate(new Cleaner(minSize, maxSize), intervalSec, intervalSec,
          TimeUnit.SECONDS);
    }
    else {
      this.cleanupPool = null;
    }
  }

  /** @return the current size of the cache */
  public long getCacheSize() { return cacheSize.get(); }

  /**
   * Load any initial entries. This assumes that no entries are in the cache already, and
   * that no entries are added during initialization. If concurrent access is needed during
   * initialization, then further work is needed for this method.
   */
  public void initialize() {
    final Map<K, V> initialEntries = cacheLoader.loadAll();
    initialEntries.forEach((key, entry) ->
    {
      long size = sizer.getSize(entry);
      cacheMap.putIfAbsent(key, new LockingCacheEntry(entry, size));

      // note that adding the size would be inaccurate if the entry already exists in the cache
      cacheSize.addAndGet(size);
    });
  }

  /** stop cleanup */
  public void shutdownCleanup() {
    if (cleanupPool != null) {
      cleanupPool.shutdown();
    }
  }

  /**
   * Try to remove an entry, if it is not currently locked.
   *
   * @param key the key
   * @return true if the entry was removed, false otherwise
   */
  public boolean tryRemove(K key) throws Exception {
    LockingCacheEntry<V> entry = cacheMap.get(key);
    if (entry == null) {
      // entry is already gone from the cache
      return true;
    }
    ReentrantReadWriteLock lock = entry.getLock();
    if (lock.writeLock().tryLock()) {
      // remove the entry from the in memory map and loader
      try {
        cacheMap.remove(key);
        cacheLoader.remove(key, entry.getValue());
        cacheSize.addAndGet(-1 * entry.getSize());
        entry.invalidate();
      } finally {
        lock.writeLock().unlock();
      }
      return true;
    }
    return false;
  }

  /**
   * Gets the entry with a read lock. Users must call {@link LockingCacheEntry#close()} on
   * the returned entry when done using the value, in order to release the read lock.
   * Alternatively, try with resources can be used, so that it will be closed automatically.
   *
   * @param key the key
   * @return the read locked project cache entry, with the associated value
   */
  public LockingCacheEntry<V> get(K key) throws Exception {
    LockingCacheEntry<V> entry = null;

    while (entry == null) {
      entry = cacheMap.computeIfAbsent(key, k -> new LockingCacheEntry());
      ReentrantReadWriteLock lock = entry.getLock();
      lock.readLock().lock();
      if (entry.isEntryInvalidated()) {
        // this project was removed. Try getting the entry again.
        entry = null;
        continue;
      }
      if (entry.isEntryUninitialized()) {
        // need to load the entry
        lock.readLock().unlock();
        if (lock.writeLock().tryLock()) {
          try {
            // double check to make sure no one has modified the entry in between releasing
            // the read lock and acquiring the write lock
            if (entry.isEntryInvalidated()) {
              // the project was removed, so retry getting/loading it
              entry = null;
              continue;
            } else if (entry.isEntryValid()) {
              // another thread has already loaded the entry, so downgrade to a read lock
              lock.readLock().lock();
            } else {
              // need to initialize/download the project
              V value = cacheLoader.load(key);
              entry.set(value, sizer.getSize(value));
              lock.readLock().lock();
              cacheSize.addAndGet(entry.getSize());
            }
          } finally {
            lock.writeLock().unlock();
          }
        } else {
          // unable to get the write lock, so another thread must have the write lock and is
          // either creating the entry or removing it
          entry = null;
          continue;
        }
      }
    }
    return entry;
  }

  /**
   * Cleanup the cache.
   *
   * @param targetSize the size of the cache for stopping cleanup
   */
  public void cleanup(long targetSize) {
    if (cacheSize.get() < targetSize) {
      return;
    }
    // get the list of projects, sorted by last access time ascending.
    List<Entry<K,LockingCacheEntry<V>>> entries = new ArrayList(cacheMap.entrySet());
    Collections.sort(entries, (x, y) -> {
      if (x.getValue().getLastAccessTime() < y.getValue().getLastAccessTime()) {
        return -1;
      } else if (x.getValue().getLastAccessTime() == y.getValue().getLastAccessTime()) {
        return 0;
      } else {
        return 1;
      }
    });

    for(Entry<K,LockingCacheEntry<V>> entry : entries) {
      if (cacheSize.get() < targetSize) {
        // cache is below the min threshold, so stop
        break;
      }
      try {
        tryRemove(entry.getKey());
      } catch (Exception e) {
        // don't want to stop the cleanup thread, so just log any exceptions.
        log.error("error encountered during cleanup for " + entry.getKey(), e);
      }
    }
  }

  /**
   * Cleaner for the project cache. Cleanup is initiated if the cache size exceeds
   * maxSizeBytes, and continues until the cache size is below minSizeBytes. Locked
   * entries that are currently in use will not be removed. Cache cleanup may
   * stop before the cache size is below minSizeBytes if the total size of locked
   * entries is greater.
   */
  private class Cleaner implements Runnable {
    private final long minSizeBytes;
    private final long maxSizeBytes;

    /**
     * Constructor.
     *
     * @param minSizeBytes the threshold for stopping cleanup
     * @param maxSizeBytes the threshold for starting cleanup
     */
    Cleaner(long minSizeBytes, long maxSizeBytes) {
      this.minSizeBytes = minSizeBytes;
      this.maxSizeBytes = maxSizeBytes;
     }

    @Override
    public void run()
    {
      if (cacheSize.get() > maxSizeBytes) {
        cleanup(minSizeBytes);
      }
    }
  }
}
