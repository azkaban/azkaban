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

import azkaban.metrics.MetricsManager;
import azkaban.metrics.CommonMetricsTest.DummyReporter;

import java.util.concurrent.TimeUnit;
import java.time.Duration;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class WebMetricsTest{

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
  public void testLogFetchLatencyMetrics() {
    WebMetrics.INSTANCE.setFetchLogLatency(1L);
    sleep20Millis();
    Assert.assertEquals(dr.getGuage("fetchLogLatency"), "1");

    WebMetrics.INSTANCE.setFetchLogLatency(99L);
    sleep20Millis();
    Assert.assertEquals(dr.getGuage("fetchLogLatency"), "99");
  }

  @Test
  public void testWebPostCallMeter() {

    sleep20Millis();
    long currMeterNum = dr.getMeter("Web-Post-Call-Meter");
    WebMetrics.INSTANCE.markWebPostCall();
    sleep20Millis();
    Assert.assertEquals(dr.getMeter("Web-Post-Call-Meter"), currMeterNum + 1);

    WebMetrics.INSTANCE.markWebPostCall();
    WebMetrics.INSTANCE.markWebPostCall();
    sleep20Millis();
    Assert.assertEquals(dr.getMeter("Web-Post-Call-Meter"), currMeterNum + 3);
  }

  @Test
  public void testWebGetCallMeter() {

    sleep20Millis();
    long currMeterNum = dr.getMeter("Web-Get-Call-Meter");
    WebMetrics.INSTANCE.markWebGetCall();
    sleep20Millis();
    Assert.assertEquals(dr.getMeter("Web-Get-Call-Meter"), currMeterNum + 1);

    WebMetrics.INSTANCE.markWebGetCall();
    WebMetrics.INSTANCE.markWebGetCall();
    sleep20Millis();
    Assert.assertEquals(dr.getMeter("Web-Get-Call-Meter"), currMeterNum + 3);
  }

  /**
   * Helper method to sleep 20 milli seconds.
   */
  private void sleep20Millis() {
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
