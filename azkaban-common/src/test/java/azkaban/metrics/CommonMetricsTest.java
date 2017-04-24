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

import azkaban.metrics.MetricsTestUtility.DummyReporter;

import java.util.concurrent.TimeUnit;
import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
//todo: HappyRay Find a way to fix these flaky tests.
public class CommonMetricsTest {

  private DummyReporter dr;

  @Before
  public void setup() {
    dr = new DummyReporter(MetricsManager.INSTANCE.getRegistry());
    dr.start(Duration.ofMillis(2).toMillis(), TimeUnit.MILLISECONDS);
  }

  @After
  public void shutdown() {
    if (null != dr)
      dr.stop();

    dr = null;
  }

  @Test
  public void testMarkDBConnectionMetrics() {
    MetricsTestUtility.testMeter("DB-Connection-meter", dr, CommonMetrics.INSTANCE::markDBConnection);
  }

  @Test
  public void testDBConnectionTimeMetrics() {
    MetricsTestUtility.testGauge("dbConnectionTime", dr, CommonMetrics.INSTANCE::setDBConnectionTime);
  }

  @Test
  public void testOOMWaitingJobMetrics() {
    MetricsTestUtility.testGauge("OOM-waiting-job-count", dr, CommonMetrics.INSTANCE::incrementOOMJobWaitCount);
  }
}
