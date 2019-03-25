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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class LockingCacheTest {

  private static class TestLoader implements LockingCacheLoader<Integer, String> {
    protected final ConcurrentHashMap<Integer, AtomicInteger> loadCountMap = new
        ConcurrentHashMap<>();
    protected final ConcurrentHashMap<Integer, AtomicInteger> removeCountMap = new
        ConcurrentHashMap<>();
    protected int loadInterval;
    protected int removeInterval;

    TestLoader() {
      this(0, 0);
    }

    TestLoader(int loadInterval, int removeInterval) {
      this.loadInterval = loadInterval;
      this.removeInterval = removeInterval;
    }

    @Override
    public String load(Integer key) throws Exception {
      updateCountAndSleep(key, loadCountMap, loadInterval);
      return getValue(key);
    }

    @Override
    public Map<Integer, String> loadAll() {
      Map<Integer, String> map = Stream.of(new Object[][] {
          { 7, "bbbbbbb" },
          { 5, "ccccc" },
      }).collect(Collectors.toMap(data -> (Integer) data[0], data -> (String) data[1]));
      return map;
    }

    @Override public void remove(Integer key, String value) throws Exception {
      updateCountAndSleep(key, removeCountMap, removeInterval);
    }

    protected void updateCountAndSleep(Integer key, ConcurrentHashMap<Integer, AtomicInteger> map,
        int interval) throws InterruptedException {
      map.compute(key, (k, v) -> {
        if (v != null) {
          v.incrementAndGet();
          return v;
        } else
          return new AtomicInteger(1);
      });
      if (interval > 0) {
        Thread.sleep(interval);
      }
    }

    protected String getValue(int key) {
      char[] val = new char[key];
      Arrays.fill(val, 'a');
      return new String(val);
    }

    protected int getRemovalCount(int key) {
      AtomicInteger count = removeCountMap.get(key);
      return count == null ? 0 : count.get();
    }

    protected int getLoadCount(int key) {
      AtomicInteger count = loadCountMap.get(key);
      return count == null ? 0 : count.get();
    }
  }

  private static class TestSizer implements LockingCacheSizer<String> {
    @Override public long getSize(String value) { return value.length(); }
  }

  @Test
  public void testUnlock() throws Exception {
    final TestLoader loader = new TestLoader();
    final LockingCache<Integer, String, TestLoader, TestSizer> cache = new LockingCache(loader,
        new TestSizer());

    LockingCacheEntry<String> entry = cache.get(20);
    assertThat(entry.getValue().length()).isEqualTo(20);

    // can't remove a locked entry
    assertThat(cache.tryRemove(20)).isFalse();
    assertThat(cache.getCacheSize()).isEqualTo(20);
    entry.close();

    // can't get the value after releasing the lock
    assertThatThrownBy(() -> entry.getValue()).isInstanceOf(IllegalStateException.class);

    // get the entry again
    LockingCacheEntry<String> entry2 = cache.get(20);
    assertThat(entry2.getValue().length()).isEqualTo(20);
    assertThat(cache.getCacheSize()).isEqualTo(20);
    entry2.close();

    assertThat(cache.tryRemove(20)).isTrue();
    assertThat(cache.getCacheSize()).isEqualTo(0);

    // get the entry after removal
    LockingCacheEntry<String> entry3 = cache.get(20);
    assertThat(entry3.getValue().length()).isEqualTo(20);
    assertThat(cache.getCacheSize()).isEqualTo(20);
    entry3.close();

    assertThat(loader.getLoadCount(20)).isEqualTo(2);
    assertThat(loader.getRemovalCount(20)).isEqualTo(1);
  }

  @Test
  public void testTryWithResources() throws Exception {
    final TestLoader loader = new TestLoader();
    final LockingCache<Integer, String, TestLoader, TestSizer> cache = new LockingCache(loader,
        new TestSizer());

    try (LockingCacheEntry<String> entry = cache.get(20)) {
      assertThat(entry.getValue().length()).isEqualTo(20);
      assertThat(cache.tryRemove(20)).isFalse();
      assertThat(cache.getCacheSize()).isEqualTo(20);
    }

    try (LockingCacheEntry<String> entry = cache.get(20)) {
      assertThat(entry.getValue().length()).isEqualTo(20);
      assertThat(cache.getCacheSize()).isEqualTo(20);
    }

    assertThat(cache.tryRemove(20)).isTrue();
    assertThat(cache.getCacheSize()).isEqualTo(0);
    assertThat(loader.getLoadCount(20)).isEqualTo(1);
    assertThat(loader.getRemovalCount(20)).isEqualTo(1);
  }

  @Test
  public void testCacheCleanup() throws Exception{
    final TestLoader loader = new TestLoader();
    final LockingCache<Integer, String, TestLoader, TestSizer> cache = new LockingCache(loader,
        new TestSizer());

    // cache should start empty
    assertThat(cache.getCacheSize()).isEqualTo(0);

    // get some values
    LockingCacheEntry<String> entry3 = cache.get(3);
    assertThat(entry3.getValue().length()).isEqualTo(3);
    assertThat(cache.getCacheSize()).isEqualTo(3);
    Thread.sleep(1);
    LockingCacheEntry<String> entry19 = cache.get(19);
    assertThat(entry19.getValue().length()).isEqualTo(19);
    entry19.close();
    Thread.sleep(1);
    assertThat(cache.getCacheSize()).isEqualTo(22);
    LockingCacheEntry<String> entry11 = cache.get(11);
    assertThat(entry11.getValue().length()).isEqualTo(11);
    Thread.sleep(1);
    assertThat(cache.getCacheSize()).isEqualTo(33);
    LockingCacheEntry<String> entry13 = cache.get(13);
    assertThat(cache.getCacheSize()).isEqualTo(46);
    assertThat(entry13.getValue().length()).isEqualTo(13);
    Thread.sleep(1);
    entry13.close();
    entry11.close();

    // bump up the time for entry 19
    LockingCacheEntry<String> entry19b = cache.get(19);
    assertThat(entry19b.getValue().length()).isEqualTo(19);
    assertThat(cache.getCacheSize()).isEqualTo(46);
    entry19b.close();

    // cleanup
    cache.cleanup(30);
    assertThat(cache.getCacheSize()).isEqualTo(27);

    // cleanup all, except locked entries (3)
    cache.cleanup(0);
    assertThat(cache.getCacheSize()).isEqualTo(3);

    entry3.close();

    // cleanup all
    cache.cleanup(0);
    assertThat(cache.getCacheSize()).isEqualTo(0);
  }

  @Test
  public void testInitialize() throws Exception {
    final TestLoader loader = new TestLoader();
    final LockingCache<Integer, String, TestLoader, TestSizer> cache = new LockingCache(loader,
        new TestSizer());

    cache.initialize();
    assertThat(cache.getCacheSize()).isEqualTo(12);

    // get a value in the cache
    try(LockingCacheEntry<String> entry5 = cache.get(5)) {
      assertThat(entry5.getValue().length()).isEqualTo(5);
    }
    assertThat(cache.getCacheSize()).isEqualTo(12);

    // get a value not already in the cache
    try(LockingCacheEntry<String> entry11 = cache.get(11)) {
      assertThat(entry11.getValue().length()).isEqualTo(11);
    }
    assertThat(cache.getCacheSize()).isEqualTo(23);

    assertThat(loader.getLoadCount(5)).isEqualTo(0);
    assertThat(loader.getLoadCount(7)).isEqualTo(0);
    assertThat(loader.getLoadCount(11)).isEqualTo(1);
  }

  /**
   * Test only one thread is loading at a time. Create 5 threads that try to get the same value,
   * with a sleep in the load so that they will need to wait for each other. Load should only be
   * called once for any value, and the later threads should get the pre-loaded value.
   */
  @Test
  public void testConcurrentLoad() throws Exception {
    final TestLoader loader = new TestLoader(10, 0);
    final LockingCache<Integer, String, TestLoader, TestSizer> cache = new LockingCache(loader,
        new TestSizer());
    final ExecutorService executor= Executors.newFixedThreadPool(5);
    final CyclicBarrier barrier = new CyclicBarrier(5);
    for (int i = 0; i < 5; i++) {
      executor.execute(() -> {
        for (int key = 10; key < 15; key++)
        try (LockingCacheEntry<String> entry = cache.get(key)) {
          assertThat(entry.getValue().length()).isEqualTo(key);
          barrier.await();
        } catch (Exception e) {
          assertThat(e).doesNotThrowAnyException();
        }
      });
    }
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      assertThat(e).doesNotThrowAnyException();
    }
    assertThat(cache.getCacheSize()).isEqualTo(60);
    for (int i = 10; i < 15; i++) {
      assertThat(loader.getLoadCount(i)).isEqualTo(1);
    }
  }

  /** Used by testConcurrentGetRemove */
  private static class BlockingLoader extends TestLoader {
    private int loadCount = 0;
    private final CyclicBarrier barrier;

    BlockingLoader(CyclicBarrier barrier) {
      super(2, 20);
      this.barrier = barrier;
    }
    @Override
    public String load(Integer key) throws Exception {
      updateCountAndSleep(key, loadCountMap, loadInterval);
      // barriers 2 and 3 occur during the first load
      // barrier 7 occurs during the second load
      if (loadCount == 0) {
        loadCount++;
        barrier.await();
      }
      barrier.await();
      return getValue(key);
    }

    @Override
    public void remove(Integer key, String value) throws Exception {
      updateCountAndSleep(key, removeCountMap, removeInterval);
      // barrier 6 occurs inside remove
      barrier.await();
    }
  }

  /**
   * Test loading and cleanup do not step on each other, and that the value is reloaded after
   * cleanup as needed.
   * Thread 1 is loading, and Thread 2 is cleaning. Use {@link CyclicBarrier} to sync the threads.
   *
   *                Thread 1                    Thread2
   * barrier 0
   *                call get() to start load
   * barrier 1      inside load
   *                                            call tryRemove(), returns false
   * barrier 2      inside load
   *                exit load
   * barrier 3      holding read lock
   *                                            call tryRemove(), returns false
   * barrier 4      holding read lock
   *                call close()
   * barrier 5
   *                                            call tryRemove()
   * barrier 6                                  inside remove
   *                call get(), block
   *                                            exit remove
   * barrier 7      inside load                 back in main thread
   * barrier 8      done                        done
   */
  @Test
  public void testConcurrentGetRemove() throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final BlockingLoader loader = new BlockingLoader(barrier);
    final LockingCache<Integer, String, TestLoader, TestSizer> cache = new LockingCache(loader,
        new TestSizer());
    final ExecutorService executor= Executors.newFixedThreadPool(2);
    final int key = 10;

    // create loading thread
      executor.execute(() -> {
        try {
          barrier.await(); // initial sync up at barrier 0
          try (LockingCacheEntry<String> entry = cache.get(key)) {
            // value is loaded, and we are holding the read lock
            assertThat(entry.getValue().length()).isEqualTo(key);
            assertThat(cache.getCacheSize()).isEqualTo(key);
            barrier.await(); // barrier 3, notify that we have read lock
            barrier.await(); // barrier 4, other thread has tried to remove
          }
          // read lock released
          barrier.await(); // barrier 5, notify that we have released the lock
          barrier.await(); // barrier 6 other thread removing the value
          try (LockingCacheEntry<String> entry = cache.get(key)) {
            // value is loaded, and we are holding the read lock
            assertThat(entry.getValue().length()).isEqualTo(key);
            Thread.sleep(2);
          }
          assertThat(cache.getCacheSize()).isEqualTo(key);
          barrier.await(); //barrier 8, done
         } catch (Exception e) {
          assertThat(e).doesNotThrowAnyException();
        }
      });

    // create removing thread
    executor.execute(() -> {
      try {
        barrier.await(); // initial sync up at barrier 0
        barrier.await(); // barrier 1, other thread is loading
        assertThat(cache.tryRemove(key)).isFalse(); // remove should fail, since loading
        assertThat(cache.getCacheSize()).isEqualTo(0);
        barrier.await(); // barrier 2, notify loader that remove was called
        barrier.await(); // barier 3, other thread has read lock
        assertThat(cache.tryRemove(key)).isFalse(); // remove should fail, since loading
        assertThat(cache.getCacheSize()).isEqualTo(key);
        barrier.await(); // barrier 4, notify other thread that remove was called
        barrier.await(); // barrier 5, other thread has released the lock
        assertThat(cache.tryRemove(key)).isTrue(); // remove should suceed
        assertThat(cache.getCacheSize()).isEqualTo(0);
        barrier.await(); // barrier 7, other thread is loading
        barrier.await(); // barrier 8, done
      } catch (Exception e) {
        assertThat(e).doesNotThrowAnyException();
      }
    });

    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      assertThat(e).doesNotThrowAnyException();
    }
    assertThat(cache.getCacheSize()).isEqualTo(key);
    assertThat(loader.getLoadCount(key)).isEqualTo(2);
  }

  /** Used by testConcurrentGetRemove */
  private static class CleanupLoader extends TestLoader {
    static final String VALUE = "aaaaa";

    CleanupLoader() {
      super();
    }
    @Override
    protected String getValue(int key) {
      return VALUE;
    }
  }

  /** Test cleanup thread. Verify that it will wait if entries are locked, and will remove
   *  them when finally unlocked. Also check that it will remove up to the min size, and won't
   *  start until the max size is reached.
   */
  @Test
  public void testCleanupThread() throws Exception {
    final int numThreads = 4;
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    final CleanupLoader loader = new CleanupLoader();
    final int minSize = 50;
    final int maxSize = 100;
    final LockingCache<Integer, String, TestLoader, TestSizer> cache = new LockingCache(loader,
        new TestSizer(), minSize, maxSize, 1);
    final ExecutorService executor= Executors.newFixedThreadPool(numThreads);

    for (int i = 0; i < numThreads; i ++) {
      final int threadId = i;
      executor.execute(() -> {
        try {
          final int mod = threadId % 2;
          Map<Integer, LockingCacheEntry> entryMap = new HashMap<>();

          for (int j = 0; j < 8; j++) {
            int key = 2 * j + mod;
            try (LockingCacheEntry<String> entry = cache.get(key)) {
              assertThat(entry.getValue()).isEqualTo(CleanupLoader.VALUE);
            }
          }
          barrier.await();
          Thread.sleep(2000);
          // since we are staying under the max threshold, no entries should be removed
          assertThat(cache.getCacheSize()).isEqualTo(80);
          // insert some more values to go over the max, and keep the lock on these
          for (int j = 8; j < 15; j++) {
            int key = 2 * j + mod;
            LockingCacheEntry<String> entry = cache.get(key);
            entryMap.put(key, entry);
            assertThat(entry.getValue()).isEqualTo(CleanupLoader.VALUE);
          }
          barrier.await();
          Thread.sleep(2000);
          // the cleanup thread should remove all unlocked entries, but will still be above the
          // min threshold. Size can vary depending on when the cleanup thread kicks in, but should
          // be greater than or equal to the size of locked entries, and less than the max.
          assertThat(cache.getCacheSize()).isGreaterThanOrEqualTo(70);
          assertThat(cache.getCacheSize()).isLessThan(maxSize);

          // now go over the max threshold with locked entries
          for (int j = 15; j < 20; j++) {
            int key = 2 * j + mod;
            LockingCacheEntry<String> entry = cache.get(key);
            entryMap.put(key, entry);
            assertThat(entry.getValue()).isEqualTo(CleanupLoader.VALUE);
          }
          barrier.await();
          Thread.sleep(2000);
          // only the locked entries should be left
          assertThat(cache.getCacheSize()).isEqualTo(120);

          // now unlock entries
          entryMap.values().forEach(e -> e.close());
          barrier.await();
          Thread.sleep(2000);
          // cache size should be below the threshold
          assertThat(cache.getCacheSize()).isLessThanOrEqualTo(maxSize);

          // call cleanup ourselves, so that it runs in parallel
          barrier.await();
          cache.cleanup(20);
          barrier.await();
          assertThat(cache.getCacheSize()).isLessThan(20);
        } catch (Exception e) {
          assertThat(e).doesNotThrowAnyException();
        }
      });
    }

    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      assertThat(e).doesNotThrowAnyException();
    }
    assertThat(cache.getCacheSize()).isLessThan(20);
  }
 }
