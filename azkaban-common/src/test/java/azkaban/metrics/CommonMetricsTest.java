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

import static org.assertj.core.api.Assertions.*;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import org.junit.Before;
import org.junit.Test;


public class CommonMetricsTest {

  private MetricsTestUtility testUtil;
  private CommonMetrics metrics;

  @Before
  public void setUp() {
    final MetricRegistry metricRegistry = new MetricRegistry();
    this.testUtil = new MetricsTestUtility(metricRegistry);
    this.metrics = new CommonMetrics(new MetricsManager(metricRegistry));
  }

  @Test
  public void testOOMWaitingJobMetrics() {
    final String metricName = CommonMetrics.OOM_WAITING_JOB_COUNT_NAME;

    assertThat(this.testUtil.getGaugeValue(metricName)).isEqualTo(0);
    this.metrics.incrementOOMJobWaitCount();
    assertThat(this.testUtil.getGaugeValue(metricName)).isEqualTo(1);
  }

  @Test
  public void testSubmitMetrics() {
    assertThat(this.testUtil.getMeterValue(CommonMetrics.SUBMIT_FLOW_FAIL_METER_NAME)).isEqualTo(0);
    this.metrics.markSubmitFlowFail();
    assertThat(this.testUtil.getMeterValue(CommonMetrics.SUBMIT_FLOW_FAIL_METER_NAME)).isEqualTo(1);

    assertThat(this.testUtil.getMeterValue(CommonMetrics.SUBMIT_FLOW_SKIP_METER_NAME)).isEqualTo(0);
    this.metrics.markSubmitFlowSkip();
    assertThat(this.testUtil.getMeterValue(CommonMetrics.SUBMIT_FLOW_SKIP_METER_NAME)).isEqualTo(1);

    assertThat(this.testUtil.getMeterValue(CommonMetrics.SUBMIT_FLOW_SUCCESS_METER_NAME)).isEqualTo(0);
    this.metrics.markSubmitFlowSuccess();
    assertThat(this.testUtil.getMeterValue(CommonMetrics.SUBMIT_FLOW_SUCCESS_METER_NAME)).isEqualTo(1);
  }

  @Test
  public void testQueueWaitMetrics() {
    final double delta = 0.001;

    this.metrics.addQueueWait(500L);
    this.metrics.addQueueWait(600L);
    this.metrics.addQueueWait(1000L);
    Snapshot snapshot = this.testUtil.getHistogramSnapshot(CommonMetrics.QUEUE_WAIT_HISTOGRAM_NAME);
    assertThat(snapshot.getMedian()).isCloseTo(600.0, within(delta));
    assertThat(snapshot.getMean()).isCloseTo(700.0, within(delta));
    assertThat(snapshot.getMin()).isEqualTo(500);
    assertThat( snapshot.getMax()).isEqualTo(1000);
  }
}
