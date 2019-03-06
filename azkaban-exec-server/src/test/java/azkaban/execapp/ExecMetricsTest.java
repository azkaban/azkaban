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

import static org.assertj.core.api.Assertions.*;

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
    assertThat(this.testUtil.getTimerCount(ExecMetrics.FLOW_SETUP_TIMER_NAME)).isEqualTo(0);
    Timer.Context context = this.metrics.getFlowSetupTimerContext();
    try {
      Thread.sleep(10);
    }
    finally {
      context.stop();
    }
    assertThat(this.testUtil.getTimerCount(ExecMetrics.FLOW_SETUP_TIMER_NAME)).isEqualTo(1);
    Snapshot snapshot = this.testUtil.getTimerSnapshot(ExecMetrics.FLOW_SETUP_TIMER_NAME);
    double val = snapshot.getMax();
    assertThat(snapshot.getMax()).isGreaterThanOrEqualTo(10);
  }

}
