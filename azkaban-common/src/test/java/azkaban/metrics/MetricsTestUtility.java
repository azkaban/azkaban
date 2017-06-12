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

import com.codahale.metrics.MetricRegistry;


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
}
