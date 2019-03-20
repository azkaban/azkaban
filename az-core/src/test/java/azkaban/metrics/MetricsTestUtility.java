/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * This class is designed for a utility class to test drop wizard metrics
 */
public class MetricsTestUtility {

  private final MetricRegistry registry;

  public MetricsTestUtility(final MetricRegistry registry) {
    this.registry = registry;
  }

  public long getGaugeValue(final String name) {
    // Assume that the gauge value can be converted to type long.
    return (long) this.registry.getGauges().get(name).getValue();
  }

  /**
   * @return the value for the specified {@link Counter}
   */
  public long getCounterValue(final String name) {
    return this.registry.getCounters().get(name).getCount();
  }

  /** @return the value for the specified {@link Meter} */
  public long getMeterValue(final String name) {
    return this.registry.getMeters().get(name).getCount();
  }

  /** @return the {@link Snapshot} for the specified {@link Histogram}. */
  public Snapshot getHistogramSnapshot(final String name) {
    return this.registry.getHistograms().get(name).getSnapshot();
  }

  /** @return the count for the specified {@link Timer}. */
  public long getTimerCount(final String name) {
    return this.registry.getTimers().get(name).getCount();
  }

  /** @return the {@link Snapshot} for the specified {@link Timer}. */
  public Snapshot getTimerSnapshot(final String name) {
    return this.registry.getTimers().get(name).getSnapshot();
  }
}
