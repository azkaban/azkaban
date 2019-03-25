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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry in the {@link LockingCache}.
 *
 * @param <V> class type for the value
 */
public class LockingCacheEntry<V>  implements AutoCloseable{
  private V value;
  private long size;
  private long lastAccessTime;
  private final ReentrantReadWriteLock lock;
  private static final Logger log = LoggerFactory.getLogger(LockingCache.class);

  /**
   * Constructor for creating an uninitialized loading cache entry.
   */
  LockingCacheEntry() {
    this(null, 0);
  }

  /**
   * Constructor for creating an initialized loading cache entry.
   *
   * @param value the value
   * @param size the size of the value
   */
  LockingCacheEntry(V value, long size) {
    this.value = value;
    this.size = size;
    if (size > 0) {
      this.lastAccessTime = System.currentTimeMillis();
    } else {
      this.lastAccessTime = 0;
    }
    this.lock = new ReentrantReadWriteLock();
  }

  /** @return the size of the value */
  public long getSize() {
    Preconditions.checkState(isLocked(), "must hold lock to get size");
    Preconditions.checkState(!isEntryInvalidated(), "entry must be valid");
    return this.size;
  }

  /** @return the value */
  public V getValue() {
    Preconditions.checkState(isLocked(), "must hold lock to get value");
    Preconditions.checkState(!isEntryInvalidated(), "entry must be valid");
    Preconditions.checkNotNull(value);
    lastAccessTime = System.currentTimeMillis();
    return value;
  }

  /** @return the last access time for the entry */
  long getLastAccessTime() { return lastAccessTime; }

  @Override
  public void close() { lock.readLock().unlock(); }

  /** @return the lock */
  ReentrantReadWriteLock getLock() { return lock; }

  /**
   * Set the value and size for the entry.
   *
   * @param value the value
   * @param size the size of the value
   */
  void set(V value, long size) {
    Preconditions.checkNotNull(value);
    Preconditions.checkArgument(size > 0, "size must be positive");
    Preconditions.checkState(lock.isWriteLocked(), "must have write lock to modify element");
    Preconditions.checkState(!isEntryInvalidated(), "entry must be valid");
    this.value = value;
    this.size = size;
    this.lastAccessTime = System.currentTimeMillis();
  }

  /** @return true if the cache entry has been invalidated, false otherwise */
  boolean isEntryInvalidated() { return size == -1; }

  /** @return true if the cache has a valid initialized value, false otherwise */
  boolean isEntryValid() { return value != null && this.size > 0; }

  /** @return true if the cache entry is uninitialized */
  boolean isEntryUninitialized() { return size == 0; }

  /** Invalidate the cache entry. */
  void invalidate() {
    Preconditions.checkState(lock.isWriteLocked(), "must hold write lock to invalidate");
    lastAccessTime = -1;
    size = -1;
    value = null;
  }

  /** @return true if the thread is holding a read or write lock on the entry */
  private boolean isLocked() {
    return lock.isWriteLockedByCurrentThread() || (lock.getReadHoldCount() > 0);
  }
}
