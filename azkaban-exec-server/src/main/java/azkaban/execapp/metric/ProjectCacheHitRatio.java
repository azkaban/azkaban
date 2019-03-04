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
import java.util.Arrays;

/**
 * Project cache hit ratio of last 100 cache accesses.
 *
 * <p>The advantage of sampling last 100 caches accesses over time-based sampling like last hour's
 * cache accesses is the former is more deterministic. Suppose there's only few execution in last
 * hour, then hit ratio might not be truly informative, which doesn't necessarily reflect
 * performance of the cache.</p>
 */
public class ProjectCacheHitRatio extends RatioGauge {

  private final SlidingWindowReservoir hits;
  public static final int WINDOW_SIZE = 100;

  public ProjectCacheHitRatio() {
    this.hits = new SlidingWindowReservoir(WINDOW_SIZE);
  }

  public synchronized void markHit() {
    this.hits.update(1);
  }

  public synchronized void markMiss() {
    this.hits.update(0);
  }

  @Override
  public synchronized Ratio getRatio() {
    final long hitCount = Arrays.stream(this.hits.getSnapshot().getValues()).sum();
    return Ratio.of(hitCount, this.hits.getSnapshot().size());
  }
}
