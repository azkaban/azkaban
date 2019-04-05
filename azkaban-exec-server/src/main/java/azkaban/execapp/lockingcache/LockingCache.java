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

import com.google.common.base.Preconditions;
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
 */
public class LockingCache<K, V> {
  private final ConcurrentHashMap<K, LockingCacheEntry<V>> cacheMap;
  private final AtomicLong cacheSize;
  private final LockingCacheLoader<K, V> cacheLoader;
  private final LockingCacheSizer<V> sizer;
  private final ScheduledExecutorService cleanupPool;
  private volatile long minSizeBytes;
  private volatile long maxSizeBytes;
  private static final Logger log = LoggerFactory.getLogger(LockingCache.class);

  /**
   * Constructor, without automated cleanup.
   *
   * @param cacheLoader the {@link LockingCacheLoader} for loading values by key
   * @param sizer the {@link LockingCacheSizer} for getting the size of a value
   */
  public LockingCache(LockingCacheLoader<K, V> cacheLoader, LockingCacheSizer<V> sizer) {
    this(cacheLoader, sizer, 0, 0, 0);
   }

   /**
    * Constructor, with separate auto cleaning thread.
    * @param cacheLoader the {@link LockingCacheLoader} for loading values by key
    * @param sizer the {@link LockingCacheSizer} for getting the size of a value
    * @param minSizeBytes the threshold for stopping cleanup
    * @param maxSizeBytes the threshold for starting cleanup
    * @param intervalSec the interval in seconds for running cleanup
    */
   @SuppressWarnings("FutureReturnValueIgnored")
  public LockingCache(LockingCacheLoader<K, V> cacheLoader, LockingCacheSizer<V> sizer,
      long minSizeBytes, long maxSizeBytes, long intervalSec) {
     Preconditions.checkArgument(minSizeBytes >= 0, "minSizeBytes cannot be negative");
     Preconditions.checkArgument(maxSizeBytes >= 0, "maxSizeBytes cannot be negative");
     Preconditions.checkArgument(minSizeBytes <= maxSizeBytes, "minSizeBytes must be less than or"
         + " equal to maxSizeBytes");
     Preconditions.checkArgument(intervalSec >= 0, "intervalSec cannot be negative");
     this.cacheLoader = Preconditions.checkNotNull(cacheLoader);
     this.sizer = Preconditions.checkNotNull(sizer);
    this.cacheMap = new ConcurrentHashMap<>();
    this.cacheSize = new AtomicLong(0);
    this.minSizeBytes = minSizeBytes;
    this.maxSizeBytes = maxSizeBytes;
    if (intervalSec > 0) {
      // create the cleanup thread
      this.cleanupPool = Executors.newScheduledThreadPool(1);
      cleanupPool.scheduleAtFixedRate(() -> {
            if (cacheSize.get() > this.maxSizeBytes) {
              cleanup(this.minSizeBytes);
            }
          }, intervalSec, intervalSec, TimeUnit.SECONDS);
    }
    else {
      this.cleanupPool = null;
    }
  }

  /** @return the current size of the cache */
  public long getCacheSize() { return cacheSize.get(); }

  /** @return the theshold for stopping cache cleanup */
  public long getMinSizeBytes() {
    return minSizeBytes;
  }

  /** set the threshold for stopping cache cleanup */
  public void setMinSizeBytes(long minSizeBytes) {
    Preconditions.checkArgument(minSizeBytes >= 0, "minSizeBytes cannot be negative");
    Preconditions.checkArgument(minSizeBytes <= maxSizeBytes, "minSizeBytes must be less than or"
        + " equal to maxSizeBytes");
    this.minSizeBytes = minSizeBytes;
  }

  /** @return the threshold for starting cache cleanup */
  public long getMaxSizeBytes() {
    return maxSizeBytes;
  }

  /** set the threshold for starting cache cleanup */
  public void setMaxSizeBytes(long maxSizeBytes) {
    Preconditions.checkArgument(maxSizeBytes >= 0, "maxSizeBytes cannot be negative");
    Preconditions.checkArgument(minSizeBytes <= maxSizeBytes, "minSizeBytes must be less than or"
        + " equal to maxSizeBytes");
    this.maxSizeBytes = maxSizeBytes;
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
              // need to initialize/download the value
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
    if (cacheSize.get() <= targetSize) {
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
      if (cacheSize.get() <= targetSize) {
        // cache is below the min threshold, so stop
        break;
      }
      try {
        if (tryRemove(entry.getKey())) {
          log.info("removed from cache: " + entry.getKey());
        }
      } catch(InterruptedException e) {
        log.error("cleanup interrupted");
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        // don't want to stop cleanup, so just log any exceptions.
        log.error("error encountered during cleanup for " + entry.getKey(), e);
      }
    }
  }
}
