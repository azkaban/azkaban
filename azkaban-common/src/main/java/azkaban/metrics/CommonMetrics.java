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

  private Meter dbConnectionMeter;
  private Meter flowFailMeter;
  private AtomicLong dbConnectionTime = new AtomicLong(0L);
  private AtomicLong OOMWaitingJobCount = new AtomicLong(0L);
  private MetricRegistry registry;

  CommonMetrics() {
    registry = MetricsManager.INSTANCE.getRegistry();
    setupAllMetrics();
  }

  private void setupAllMetrics() {
    dbConnectionMeter = MetricsUtility.addMeter("DB-Connection-meter", registry);
    flowFailMeter = MetricsUtility.addMeter("flow-fail-meter", registry);
    MetricsUtility.addGauge("OOM-waiting-job-count", registry, OOMWaitingJobCount::get);
    MetricsUtility.addGauge("dbConnectionTime", registry, dbConnectionTime::get);
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
    dbConnectionMeter.mark();
  }

  /**
   * Mark flowFailMeter when a flow is considered as FAILED.
   * This method could be called by Web Server or Executor, as they both detect flow failure.
   */
  public void markFlowFail() {
    flowFailMeter.mark();
  }

  public void setDBConnectionTime(long milliseconds) {
    dbConnectionTime.set(milliseconds);
  }

  /**
   * Mark the occurrence of an job waiting event due to OOM
   */
  public void incrementOOMJobWaitCount() {
    OOMWaitingJobCount.incrementAndGet();
  }

  /**
   * Unmark the occurrence of an job waiting event due to OOM
   */
  public void decrementOOMJobWaitCount() {
    OOMWaitingJobCount.decrementAndGet();
  }


}
