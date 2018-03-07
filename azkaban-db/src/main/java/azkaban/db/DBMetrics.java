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

package azkaban.db;

import azkaban.metrics.MetricsManager;
import com.codahale.metrics.Meter;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * This singleton class CommonMetrics
 */
@Singleton
public class DBMetrics {

  private final AtomicLong dbConnectionTime = new AtomicLong(0L);
  private final MetricsManager metricsManager;
  private Meter dbConnectionMeter;
  private Meter dbConnectionFailMeter;
  private Meter queryFailMeter;
  private Meter updateFailMeter;
  private Meter transactionFailMeter;

  @Inject
  public DBMetrics(final MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
    setupAllMetrics();
  }

  private void setupAllMetrics() {
    this.dbConnectionMeter = this.metricsManager.addMeter("DB-Connection-meter");
    this.dbConnectionFailMeter = this.metricsManager.addMeter("DB-Fail-Connection-meter");
    this.queryFailMeter = this.metricsManager.addMeter("DB-Fail-Query-meter");
    this.updateFailMeter = this.metricsManager.addMeter("DB-Fail-Update-meter");
    this.transactionFailMeter = this.metricsManager.addMeter("DB-Fail-Transaction-meter");
    this.metricsManager.addGauge("dbConnectionTime", this.dbConnectionTime::get);
  }

  /**
   * Mark the occurrence of an DB query event.
   */
  void markDBConnection() {

    /*
     * This method should be Thread Safe.
     * Two reasons that we don't make this function call synchronized:
     * 1). drop wizard metrics deals with concurrency internally;
     * 2). mark is basically a math addition operation, which should not cause race condition issue.
     */
    this.dbConnectionMeter.mark();
  }

  /**
   * Mark the occurrence when DB query step fails
   */
  void markDBFailQuery() {
    this.queryFailMeter.mark();
  }

  /**
   * Mark the occurrence when DB update fails.
   */
  void markDBFailUpdate() {
    this.updateFailMeter.mark();
  }

  /**
   * Mark the occurrence when AZ DB transaction fails.
   */
  void markDBFailTransaction() {
    this.transactionFailMeter.mark();
  }

  /**
   * Mark the occurrence when DB get connection fails.
   */
  void markDBFailConnection() {
    this.dbConnectionFailMeter.mark();
  }


  void setDBConnectionTime(final long milliseconds) {
    this.dbConnectionTime.set(milliseconds);
  }
}
