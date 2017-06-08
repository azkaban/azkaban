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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static org.junit.Assert.assertEquals;

import azkaban.metrics.MetricsManager;
import azkaban.metrics.MetricsTestUtility;
import org.junit.BeforeClass;
import org.junit.Test;


public class WebMetricsTest {

  private static MetricsTestUtility testUtil;
  private static WebMetrics metrics;

  @BeforeClass
  public static void setUp() {
    // initialize new guice MetricsManager
    MetricsTestUtility.initServiceProvider();
    testUtil = new MetricsTestUtility(
        SERVICE_PROVIDER.getInstance(MetricsManager.class).getRegistry());
    metrics = SERVICE_PROVIDER.getInstance(WebMetrics.class);
  }

  @Test
  public void testLogFetchLatencyMetrics() {
    metrics.setFetchLogLatency(14);
    assertEquals(14, testUtil.getGaugeValue("fetchLogLatency"));
  }
}
