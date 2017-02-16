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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * This singleton class CommonMetrics is in charge of collecting varieties of metrics
 * from azkaban-common modules.
 */
public enum CommonMetrics {
  INSTANCE;

  private Meter _dbConnectionMeter;
  private MetricRegistry _metrics;

  CommonMetrics() {
    _metrics = MetricsManager.INSTANCE.getRegistry();
    setupAllMetrics();
  }

  private void setupAllMetrics() {
    _dbConnectionMeter = addMeter("DB-Connection-meter");
  }

  public Meter addMeter(String name) {
    Meter curr = _metrics.meter(name);
    _metrics.register(name + "-gauge", (Gauge<Double>) curr::getOneMinuteRate);
    return curr;
  }

  public void markDBConnection() {

    /*
     * Two reasons that we don't make this function call synchronized:
     * 1). code hale metrics deals with concurrency internally;
     * 2). mark is basically a math addition operation, which should not cause race condition issue.
     */
    _dbConnectionMeter.mark();
  }
}