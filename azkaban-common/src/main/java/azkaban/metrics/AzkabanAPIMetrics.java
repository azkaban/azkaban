/*
 * Copyright 2020 LinkedIn Corp.
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

import com.codahale.metrics.Histogram;

/**
 * Defines all the metrics related to an @see azkaban.server.AzkabanAPI.
 */
public class AzkabanAPIMetrics {

  private final CounterGauge appGetRequestCount;
  private final CounterGauge appPostRequestCount;

  private final CounterGauge nonAppGetRequestCount;
  private final CounterGauge nonAppPostRequestCount;

  private final Histogram responseTimeHistogram;

  public AzkabanAPIMetrics(
      final CounterGauge appGetRequestCount,
      final CounterGauge appPostRequestCount,
      final CounterGauge nonAppGetRequestCount,
      final CounterGauge nonAppPostRequestCount,
      final Histogram responseTimeHistogram) {
    this.appGetRequestCount = appGetRequestCount;
    this.appPostRequestCount = appPostRequestCount;
    this.nonAppGetRequestCount = nonAppGetRequestCount;
    this.nonAppPostRequestCount = nonAppPostRequestCount;
    this.responseTimeHistogram = responseTimeHistogram;
  }

  public void incrementAppGetRequests() {
    this.appGetRequestCount.add(1);
  }

  public void incrementAppPostRequests() {
    this.appPostRequestCount.add(1);
  }

  public void incrementNonAppGetRequests() {
    this.nonAppGetRequestCount.add(1);
  }

  public void incrementNonAppPostRequests() {
    this.nonAppPostRequestCount.add(1);
  }

  public void addResponseTime(final long time) {
    this.responseTimeHistogram.update(time);
  }
}
