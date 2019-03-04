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

package azkaban.execapp.metric;

import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.SlidingWindowReservoir;

/**
 * Project cache hit ratio of last 100 cache accesses.
 */
public class ProjectCacheHitRatio extends RatioGauge {

  private final SlidingWindowReservoir hits;
  private final SlidingWindowReservoir calls;
  public static final int WINDOW_SIZE = 100;

  public ProjectCacheHitRatio() {
    this.hits = new SlidingWindowReservoir(WINDOW_SIZE);
    this.calls = new SlidingWindowReservoir(WINDOW_SIZE);
  }

  public synchronized void markHit() {
    this.hits.update(1);
    this.calls.update(1);
  }

  public synchronized void markMiss() {
    this.hits.update(0);
    this.calls.update(1);
  }

  @Override
  public synchronized Ratio getRatio() {
    long hitCount = 0;
    for (final long num : this.hits.getSnapshot().getValues()) {
      hitCount += num;
    }

    long callCount = 0;
    for (final long num : this.calls.getSnapshot().getValues()) {
      callCount += num;
    }
    return Ratio.of(hitCount, callCount);
  }
}
