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
import com.codahale.metrics.SlidingTimeWindowReservoir;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Project cache hit ratio of last 30 mins.
 */
public class ProjectCacheHitRatio extends RatioGauge {

  private final SlidingTimeWindowReservoir hits;
  private final SlidingTimeWindowReservoir calls;
  private static final Duration WINDOW_DURATION = Duration.ofMinutes(30);

  public ProjectCacheHitRatio() {
    this.hits = new SlidingTimeWindowReservoir(WINDOW_DURATION.getSeconds(), TimeUnit.SECONDS);
    this.calls = new SlidingTimeWindowReservoir(WINDOW_DURATION.getSeconds(), TimeUnit.SECONDS);
  }

  public void markHit() {
    this.hits.update(1);
    this.calls.update(1);
  }

  public void markMiss() {
    this.calls.update(1);
  }

  @Override
  public Ratio getRatio() {
    return Ratio.of(this.hits.size(), this.calls.size());
  }
}
