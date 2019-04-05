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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

/** Test the {@link LockingCacheEntry} */
public class LockingCacheEntryTest {

  @Test
  public void testUninitalizedEntryState() {
    LockingCacheEntry<String> entry = new LockingCacheEntry<>();
    assertThat(entry.isEntryUninitialized()).isTrue();
    assertThat(entry.isEntryInvalidated()).isFalse();
    assertThat(entry.isEntryValid()).isFalse();
    entry.getLock().readLock().lock();
    assertThatThrownBy(() -> entry.getValue()).isInstanceOf(NullPointerException.class);
    assertThat(entry.getSize()).isEqualTo(0);
    assertThat(entry.getLastAccessTime()).isEqualTo(0);
    entry.getLock().readLock().unlock();
  }

  @Test
  public void testInitializedEntryState() throws InterruptedException {
    final String val = "testing";
    final long size = 7;

    LockingCacheEntry<String> entry = new LockingCacheEntry<>(val, size);
    assertThat(entry.isEntryUninitialized()).isFalse();
    assertThat(entry.isEntryInvalidated()).isFalse();
    assertThat(entry.isEntryValid()).isTrue();
    long initialTime = entry.getLastAccessTime();
    entry.getLock().readLock().lock();
    Thread.sleep(2);

    // calling getValue should update the access time
    assertThat(entry.getValue()).isEqualTo(val);
    assertThat(entry.getSize()).isEqualTo(size);
    assertThat(entry.getLastAccessTime()).isGreaterThan(initialTime);
    assertThat(entry.getLastAccessTime()).isLessThanOrEqualTo(System.currentTimeMillis());
    entry.getLock().readLock().unlock();
  }

  @Test
  public void testSettingEntry() {
    LockingCacheEntry<String> entry = new LockingCacheEntry<>();
    final String val = "hi there";
    final long size = 8;

    // must have write lock to set the entry
    assertThatThrownBy(() -> entry.set(val, size)).isInstanceOf(IllegalStateException.class);

    // read lock is insufficient
    entry.getLock().readLock().lock();
    assertThatThrownBy(() -> entry.set(val, size)).isInstanceOf(IllegalStateException.class);
    entry.getLock().readLock().unlock();

    // now get the write lock
    entry.getLock().writeLock().lock();

    // can't set value to null
    assertThatThrownBy(() -> entry.set(null, size)).isInstanceOf(NullPointerException.class);

    // can't set size to 0
    assertThatThrownBy(() -> entry.set(val, 0)).isInstanceOf(IllegalArgumentException.class);

    entry.set(val, size);
    assertThat(entry.getValue()).isEqualTo(val);
    assertThat(entry.getSize()).isEqualTo(size);
    assertThat(entry.getLastAccessTime()).isLessThanOrEqualTo(System.currentTimeMillis());
    entry.getLock().writeLock().unlock();
    assertThat(entry.isEntryUninitialized()).isFalse();
    assertThat(entry.isEntryInvalidated()).isFalse();
    assertThat(entry.isEntryValid()).isTrue();

    // must have read lock to read the entry
    assertThatThrownBy(() -> entry.getValue()).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> entry.getSize()).isInstanceOf(IllegalStateException.class);

    // try again with read lock
    entry.getLock().readLock().lock();
    assertThat(entry.getValue()).isEqualTo(val);
    assertThat(entry.getSize()).isEqualTo(size);
    assertThat(entry.getLastAccessTime()).isLessThanOrEqualTo(System.currentTimeMillis());
    entry.getLock().readLock().unlock();
  }

  @Test
  public void testInvalidation() {
    LockingCacheEntry<String> entry = new LockingCacheEntry<>("more testing", 13);

    // must be write locked
    assertThatThrownBy(() -> entry.invalidate()).isInstanceOf(IllegalStateException.class);
    entry.getLock().readLock().lock();
    assertThatThrownBy(() -> entry.invalidate()).isInstanceOf(IllegalStateException.class);
    entry.getLock().readLock().unlock();

    entry.getLock().writeLock().lock();
    entry.invalidate();
    assertThatThrownBy(() -> entry.getValue()).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> entry.getSize()).isInstanceOf(IllegalStateException.class);

    entry.getLock().writeLock().unlock();
    assertThat(entry.isEntryUninitialized()).isFalse();
    assertThat(entry.isEntryInvalidated()).isTrue();
    assertThat(entry.isEntryValid()).isFalse();
  }
}
