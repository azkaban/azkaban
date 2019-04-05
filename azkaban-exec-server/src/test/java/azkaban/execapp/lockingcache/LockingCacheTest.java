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
import org.awaitility.Awaitility;
import org.junit.Test;

/** Test the {@link LockingCache} */
public class LockingCacheTest {
  private static final int TIMEOUT_SECONDS = 4;

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
    final LockingCache<Integer, String> cache = new LockingCache(loader,
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
    final LockingCache<Integer, String> cache = new LockingCache(loader,
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
    final LockingCache<Integer, String> cache = new LockingCache(loader,
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

    // cleanup entries for 11 and 13 (3 is locked and 19 was last accessed)
    cache.cleanup(30);
    assertThat(cache.getCacheSize()).isEqualTo(22);

    // cleanup all, except locked entries (3)
    cache.cleanup(0);
    assertThat(cache.getCacheSize()).isEqualTo(3);

    entry3.close();

    // cleanup all
    cache.cleanup(0);
    assertThat(cache.getCacheSize()).isEqualTo(0);
  }

  /**
   * Test only one thread is loading at a time. Create 5 threads that try to get the same value,
   * with a sleep in the load so that they will need to wait for each other. Load should only be
   * called once for any value, and the later threads should get the pre-loaded value.
   */
  @Test
  public void testConcurrentLoad() throws Exception {
    final TestLoader loader = new TestLoader(10, 0);
    final LockingCache<Integer, String> cache = new LockingCache(loader,
        new TestSizer());
    final ExecutorService executor= Executors.newFixedThreadPool(5);
    final CyclicBarrier barrier = new CyclicBarrier(5);
    for (int i = 0; i < 5; i++) {
      executor.execute(() -> {
        for (int key = 10; key < 15; key++)
        try (LockingCacheEntry<String> entry = cache.get(key)) {
          assertThat(entry.getValue().length()).isEqualTo(key);
          barrier.await();
        } catch (InterruptedException e) {
          executor.shutdownNow();
          // Preserve interrupt status
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
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
    final LockingCache<Integer, String> cache = new LockingCache(loader,
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
         } catch (InterruptedException e) {
          executor.shutdownNow();
          // Preserve interrupt status
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          throw new RuntimeException(e);
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
      } catch (InterruptedException e) {
        executor.shutdownNow();
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
    assertThat(cache.getCacheSize()).isEqualTo(key);
    assertThat(loader.getLoadCount(key)).isEqualTo(2);
  }

  /** Used by testCleanupThread */
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
  public void testCleanupThread() {
    final int numThreads = 4;
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    final CleanupLoader loader = new CleanupLoader();
    final int minSize = 50;
    final int maxSize = 100;
    final LockingCache<Integer, String> cache = new LockingCache(loader,
        new TestSizer(), minSize, maxSize, 1);
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

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
          Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).until(() -> cache
              .getCacheSize() == 80);
          // since we are staying under the max threshold, no entries should be removed
          assertThat(cache.getCacheSize()).isEqualTo(80);

          barrier.await();
          // insert some more values to go over the max, and keep the lock on these
          for (int j = 8; j < 15; j++) {
            int key = 2 * j + mod;
            LockingCacheEntry<String> entry = cache.get(key);
            entryMap.put(key, entry);
            assertThat(entry.getValue()).isEqualTo(CleanupLoader.VALUE);
          }
          barrier.await();
          Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
              .until(() -> cache
                  .getCacheSize() <=
                  maxSize);

          // the cleanup thread should remove all unlocked entries, but will still be above the
          // min threshold. Size can vary depending on when the cleanup thread kicks in, but should
          // be greater than or equal to the size of locked entries, and less than the max.
          assertThat(cache.getCacheSize()).isGreaterThanOrEqualTo(70);
          assertThat(cache.getCacheSize()).isLessThan(maxSize);

          barrier.await();
          // now go over the max threshold with locked entries
          for (int j = 15; j < 20; j++) {
            int key = 2 * j + mod;
            LockingCacheEntry<String> entry = cache.get(key);
            entryMap.put(key, entry);
            assertThat(entry.getValue()).isEqualTo(CleanupLoader.VALUE);
          }
          barrier.await();
          Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).until(()
              -> cache
              .getCacheSize() == 120);

          // only the locked entries should be left
          assertThat(cache.getCacheSize()).isEqualTo(120);

          // now unlock entries
          entryMap.values().forEach(e -> e.close());
          barrier.await();
          Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).until(() -> cache
              .getCacheSize() < maxSize);
          // cache size should be below the threshold
          assertThat(cache.getCacheSize()).isLessThanOrEqualTo(maxSize);

          // call cleanup ourselves, so that it runs in parallel
          barrier.await();
          cache.cleanup(20);
          barrier.await();
          assertThat(cache.getCacheSize()).isLessThanOrEqualTo(20);
        } catch (InterruptedException e) {
          executor.shutdownNow();
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    executor.shutdown();
    try {
      executor.awaitTermination(2, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    assertThat(cache.getCacheSize()).isLessThanOrEqualTo(20);
  }

  /** Test changing min and max values for the cleanup thread.
   */
  @Test
  public void testCleanupMinMax() throws Exception {
    final int numThreads = 4;
    final CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
    final CleanupLoader loader = new CleanupLoader();
    final int minSize = 50;
    final int maxSize = 100;
    final LockingCache<Integer, String> cache = new LockingCache(loader,
        new TestSizer(), minSize, maxSize, 1);
    final ExecutorService executor= Executors.newFixedThreadPool(numThreads);

    for (int i = 0; i < numThreads; i ++) {
      final int threadId = i;
     executor.execute(() -> {
        try {
          final int mod = threadId % 2;
          Map<Integer, LockingCacheEntry> entryMap = new HashMap<>();

          for (int j = 0; j < 15; j++) {
            int key = 2 * j + mod;
            try (LockingCacheEntry<String> entry = cache.get(key)) {
              assertThat(entry.getValue()).isEqualTo(CleanupLoader.VALUE);
            }
          }
          barrier.await(); // done with the first round of inserts
          barrier.await();  // wait to be notified to start start the 2nd round of inserts
          // insert some more values to go over the max, and keep the lock on these
          for (int j = 15; j < 25; j++) {
            int key = 2 * j + mod;
            try (LockingCacheEntry<String> entry = cache.get(key)) {
              assertThat(entry.getValue()).isEqualTo(CleanupLoader.VALUE);
            }
          }
          barrier.await(); //done with 2nd round of inserts
        } catch (InterruptedException e) {
          executor.shutdownNow();
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    // in the main thread, check the cache sizes
    barrier.await();  // wait for first round of inserts to complete

    Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).until(() -> cache.getCacheSize() <=
    maxSize);
    // check that cleanup has removed entries for the current max, min
    assertThat(cache.getCacheSize()).isLessThanOrEqualTo(maxSize);
    assertThat(cache.getCacheSize()).isGreaterThanOrEqualTo(minSize);

    // reset max and min to lower values
    cache.setMinSizeBytes(20);
    cache.setMaxSizeBytes(40);
    assertThat(cache.getMinSizeBytes()).isEqualTo(20);
    assertThat(cache.getMaxSizeBytes()).isEqualTo(40);
    Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).until(() -> cache.getCacheSize() <=
        40);
    // check that cleanup has removed entries for the current max, min
    assertThat(cache.getCacheSize()).isLessThanOrEqualTo(40);
    assertThat(cache.getCacheSize()).isGreaterThanOrEqualTo(20);

    // reset max and min to higher values
    cache.setMaxSizeBytes(80);
    cache.setMinSizeBytes(60);

    barrier.await();  // notify getter threads to start the 2nd round of inserts

    barrier.await(); // wait for 2nd round of inserts to complete
    Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).until(() -> cache.getCacheSize() >=
        60 && cache.getCacheSize() < 80);

    assertThat(cache.getCacheSize()).isGreaterThanOrEqualTo(60);
    assertThat(cache.getCacheSize()).isLessThanOrEqualTo(80);

    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
