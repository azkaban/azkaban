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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Queue that swaps its lists. Allows for non-blocking writes when reading. Swap
 * should be called before every read.
 */
public class SwapQueue<T> implements Iterable<T> {
  ArrayList<T> primaryQueue;
  ArrayList<T> secondaryQueue;

  public SwapQueue() {
    primaryQueue = new ArrayList<T>();
    secondaryQueue = new ArrayList<T>();
  }

  /**
   * Swaps primaryQueue with secondary queue. The previous primary queue will be
   * released.
   */
  public synchronized void swap() {
    primaryQueue = secondaryQueue;
    secondaryQueue = new ArrayList<T>();
  }

  /**
   * Returns a count of the secondary queue.
   *
   * @return
   */
  public synchronized int getSwapQueueSize() {
    return secondaryQueue.size();
  }

  public synchronized int getPrimarySize() {
    return primaryQueue.size();
  }

  public synchronized void addAll(Collection<T> col) {
    secondaryQueue.addAll(col);
  }

  /**
   * Returns both the secondary and primary size
   *
   * @return
   */
  public synchronized int getSize() {
    return secondaryQueue.size() + primaryQueue.size();
  }

  public synchronized void add(T element) {
    secondaryQueue.add(element);
  }

  /**
   * Returns iterator over the primary queue.
   */
  @Override
  public synchronized Iterator<T> iterator() {
    return primaryQueue.iterator();
  }
}
