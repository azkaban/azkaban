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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * A simple subclass of {@link ThreadPoolExecutor} to keep track of in progress
 * tasks as well as other interesting statistics.
 * 
 * The content of this class is copied from article "Java theory and practice:
 * Instrumenting applications with JMX"
 * 
 * @author hluu
 * 
 */
public class TrackingThreadPool extends ThreadPoolExecutor {

  private static Logger logger = Logger.getLogger(TrackingThreadPool.class);

  private final Map<Runnable, Boolean> inProgress =
      new ConcurrentHashMap<Runnable, Boolean>();
  private final ThreadLocal<Long> startTime = new ThreadLocal<Long>();

  private ThreadPoolExecutingListener executingListener =
      new NoOpThreadPoolExecutingListener();

  private long totalTime;
  private int totalTasks;

  public TrackingThreadPool(int corePoolSize, int maximumPoolSize,
      long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue,
      ThreadPoolExecutingListener listener) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    if (listener != null) {
      executingListener = listener;
    }
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    try {
      executingListener.beforeExecute(r);
    } catch (Throwable e) {
      // to ensure the listener doesn't cause any issues
      logger.warn("Listener threw exception", e);
    }
    super.beforeExecute(t, r);
    inProgress.put(r, Boolean.TRUE);
    startTime.set(new Long(System.currentTimeMillis()));
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    long time = System.currentTimeMillis() - startTime.get().longValue();
    synchronized (this) {
      totalTime += time;
      ++totalTasks;
    }
    inProgress.remove(r);
    super.afterExecute(r, t);
    try {
      executingListener.afterExecute(r);
    } catch (Throwable e) {
      // to ensure the listener doesn't cause any issues
      logger.warn("Listener threw exception", e);
    }
  }

  public Set<Runnable> getInProgressTasks() {
    return Collections.unmodifiableSet(inProgress.keySet());
  }

  public synchronized int getTotalTasks() {
    return totalTasks;
  }

  public synchronized double getAverageTaskTime() {
    return (totalTasks == 0) ? 0 : totalTime / totalTasks;
  }

  private static class NoOpThreadPoolExecutingListener implements
      ThreadPoolExecutingListener {

    @Override
    public void beforeExecute(Runnable r) {
    }

    @Override
    public void afterExecute(Runnable r) {
    }
  }
}