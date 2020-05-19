/*
 * Copyright 2012 LinkedIn Corp.
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
package azkaban.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

/**
 * A simple subclass of {@link ThreadPoolExecutor} to keep track of in progress tasks as well as
 * other interesting statistics.
 *
 * The content of this class is copied from article "Java theory and practice: Instrumenting
 * applications with JMX"
 *
 * @author hluu
 */
public class TrackingThreadPool extends ThreadPoolExecutor {

  private static final Logger logger = Logger.getLogger(TrackingThreadPool.class);

  private final Map<Runnable, Boolean> inProgress =
      new ConcurrentHashMap<>();
  private final ThreadLocal<Long> startTime = new ThreadLocal<>();

  private ThreadPoolExecutingListener executingListener =
      new NoOpThreadPoolExecutingListener();

  private long totalTime;
  private int totalTasks;

  public TrackingThreadPool(final int corePoolSize, final int maximumPoolSize,
      final long keepAliveTime, final TimeUnit unit, final BlockingQueue<Runnable> workQueue,
      final ThreadPoolExecutingListener listener) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
        new ThreadFactoryBuilder().setNameFormat("azk-tracking-pool-%d").build());
    if (listener != null) {
      this.executingListener = listener;
    }
  }

  @Override
  protected void beforeExecute(final Thread t, final Runnable r) {
    try {
      this.executingListener.beforeExecute(r);
    } catch (final Throwable e) {
      // to ensure the listener doesn't cause any issues
      logger.warn("Listener threw exception", e);
    }
    super.beforeExecute(t, r);
    this.inProgress.put(r, Boolean.TRUE);
    this.startTime.set(Long.valueOf(System.currentTimeMillis()));
  }

  @Override
  protected void afterExecute(final Runnable r, final Throwable t) {
    final long time = System.currentTimeMillis() - this.startTime.get().longValue();
    synchronized (this) {
      this.totalTime += time;
      ++this.totalTasks;
    }
    this.inProgress.remove(r);
    super.afterExecute(r, t);
    try {
      this.executingListener.afterExecute(r);
    } catch (final Throwable e) {
      // to ensure the listener doesn't cause any issues
      logger.warn("Listener threw exception", e);
    }
  }

  public Set<Runnable> getInProgressTasks() {
    return Collections.unmodifiableSet(this.inProgress.keySet());
  }

  public synchronized int getTotalTasks() {
    return this.totalTasks;
  }

  public synchronized double getAverageTaskTime() {
    return (this.totalTasks == 0) ? 0 : this.totalTime / this.totalTasks;
  }

  private static class NoOpThreadPoolExecutingListener implements
      ThreadPoolExecutingListener {

    @Override
    public void beforeExecute(final Runnable r) {
    }

    @Override
    public void afterExecute(final Runnable r) {
    }
  }
}
