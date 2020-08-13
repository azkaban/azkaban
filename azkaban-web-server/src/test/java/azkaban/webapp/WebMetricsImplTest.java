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

package azkaban.webapp;

import static org.junit.Assert.assertEquals;

import azkaban.metrics.MetricsManager;
import azkaban.metrics.MetricsTestUtility;
import azkaban.webapp.metrics.WebMetricsImpl;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;


public class WebMetricsImplTest {

  private MetricsTestUtility testUtil;
  private WebMetricsImpl metrics;

  @Before
  public void setUp() {
    final MetricRegistry metricRegistry = new MetricRegistry();
    this.testUtil = new MetricsTestUtility(metricRegistry);
    this.metrics = new WebMetricsImpl(new MetricsManager(metricRegistry));
  }

  @Test
  public void testLogFetchLatencyMetrics() {
    this.metrics.setFetchLogLatency(14);
    assertEquals(14, this.testUtil.getGaugeValue("fetchLogLatency"));
  }
}
