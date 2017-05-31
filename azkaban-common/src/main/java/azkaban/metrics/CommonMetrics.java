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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This singleton class CommonMetrics is in charge of collecting varieties of metrics
 * which are accessed in both web and exec modules. That said, these metrics will be
 * exposed in both Web server and executor.
 */
public enum CommonMetrics {
  INSTANCE;

  private final AtomicLong dbConnectionTime = new AtomicLong(0L);
  private final AtomicLong OOMWaitingJobCount = new AtomicLong(0L);
  private final MetricRegistry registry;
  private Meter dbConnectionMeter;
  private Meter flowFailMeter;

  CommonMetrics() {
    this.registry = MetricsManager.INSTANCE.getRegistry();
    setupAllMetrics();
  }

  private void setupAllMetrics() {
    this.dbConnectionMeter = MetricsUtility.addMeter("DB-Connection-meter", this.registry);
    this.flowFailMeter = MetricsUtility.addMeter("flow-fail-meter", this.registry);
    MetricsUtility.addGauge("OOM-waiting-job-count", this.registry, this.OOMWaitingJobCount::get);
    MetricsUtility.addGauge("dbConnectionTime", this.registry, this.dbConnectionTime::get);
  }

  /**
   * Mark the occurrence of an DB query event.
   */
  public void markDBConnection() {

    /*
     * This method should be Thread Safe.
     * Two reasons that we don't make this function call synchronized:
     * 1). drop wizard metrics deals with concurrency internally;
     * 2). mark is basically a math addition operation, which should not cause race condition issue.
     */
    this.dbConnectionMeter.mark();
  }

  /**
   * Mark flowFailMeter when a flow is considered as FAILED.
   * This method could be called by Web Server or Executor, as they both detect flow failure.
   */
  public void markFlowFail() {
    this.flowFailMeter.mark();
  }

  public void setDBConnectionTime(final long milliseconds) {
    this.dbConnectionTime.set(milliseconds);
  }

  /**
   * Mark the occurrence of an job waiting event due to OOM
   */
  public void incrementOOMJobWaitCount() {
    this.OOMWaitingJobCount.incrementAndGet();
  }

  /**
   * Unmark the occurrence of an job waiting event due to OOM
   */
  public void decrementOOMJobWaitCount() {
    this.OOMWaitingJobCount.decrementAndGet();
  }

}
