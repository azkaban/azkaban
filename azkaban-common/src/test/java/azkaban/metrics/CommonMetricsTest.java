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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;


public class CommonMetricsTest {

  private MetricsTestUtility testUtil;
  private CommonMetrics metrics;

  @Before
  public void setUp() {
    // Use of global state can cause problems e.g.
    // The state is shared among tests.
    // e.g. we can't run a variant of the testOOMWaitingJobMetrics twice since it relies on the initial state of
    // the registry.
    // This can also cause problem when we run tests in parallel in the future.
    // todo HappyRay: move MetricsManager, CommonMetrics to use Juice.
    this.testUtil = new MetricsTestUtility(MetricsManager.INSTANCE.getRegistry());
    this.metrics = CommonMetrics.INSTANCE;
  }

  @Test
  public void testDBConnectionTimeMetrics() {
    this.metrics.setDBConnectionTime(14);
    assertEquals(14, this.testUtil.getGaugeValue("dbConnectionTime"));
  }

  @Test
  public void testOOMWaitingJobMetrics() {
    final String metricName = "OOM-waiting-job-count";

    assertEquals(0, this.testUtil.getGaugeValue(metricName));
    this.metrics.incrementOOMJobWaitCount();
    assertEquals(1, this.testUtil.getGaugeValue(metricName));
  }
}
