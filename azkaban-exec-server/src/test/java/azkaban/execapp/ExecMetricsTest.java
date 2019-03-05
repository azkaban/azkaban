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

package azkaban.execapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import azkaban.metrics.MetricsManager;
import azkaban.metrics.MetricsTestUtility;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.junit.Test;

/** Tests for executor metrics */
public class ExecMetricsTest {

  private MetricsTestUtility testUtil;
  private ExecMetrics metrics;

  @Before
  public void setUp() {
    final MetricRegistry metricRegistry = new MetricRegistry();
    this.testUtil = new MetricsTestUtility(metricRegistry);
    this.metrics = new ExecMetrics(new MetricsManager(metricRegistry));
  }

  @Test
  public void testFlowSetupMetrics() throws InterruptedException {
    assertEquals(0, this.testUtil.getTimerCount(ExecMetrics.FLOW_SETUP_TIMER_NAME));
    Timer.Context context = this.metrics.getFlowSetupTimerContext();
    try {
      Thread.sleep(100);
    }
    finally {
      context.stop();
    }
    assertEquals(1, this.testUtil.getTimerCount(ExecMetrics.FLOW_SETUP_TIMER_NAME));
    Snapshot snapshot = this.testUtil.getTimerSnapshot(ExecMetrics.FLOW_SETUP_TIMER_NAME);
    double val = snapshot.getMax();
    assertTrue(snapshot.getMax() > 100);
  }

}
