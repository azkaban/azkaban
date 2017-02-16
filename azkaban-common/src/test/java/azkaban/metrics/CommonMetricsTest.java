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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

  /**
   * One Dummy Reporter extending {@link ScheduledReporter} collects metrics in various maps,
   * which test methods are able to access easily.
   */
  public static class DummyReporter extends ScheduledReporter{

    private Map<String, String> map;

    private Map<String, Long> meterMap;

    public DummyReporter(MetricRegistry registry) {
      super(registry, "dummy-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
      this.map = new HashMap<>();
      this.meterMap = new HashMap<>();
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
        SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms,
        SortedMap<String, Meter> meters,
        SortedMap<String, Timer> timers) {

      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        map.put(entry.getKey(), entry.getValue().getValue().toString());
      }

      for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        meterMap.put(entry.getKey(), entry.getValue().getCount());
      }
    }

    public String getGuage(String key) {
      return map.get(key);
    }

    public long getMeter(String key) {
      return meterMap.getOrDefault(key, 0L);
    }
  }


  @Test
  public void testuploadDbMetrics() {

    sleep20Millis();
    long currMeter = dr.getMeter("DB-Connection-meter");
    CommonMetrics.INSTANCE.markDBConnection();
    sleep20Millis();
    Assert.assertEquals(dr.getMeter("DB-Connection-meter"), currMeter + 1);

    CommonMetrics.INSTANCE.markDBConnection();
    CommonMetrics.INSTANCE.markDBConnection();
    sleep20Millis();
    Assert.assertEquals(dr.getMeter("DB-Connection-meter"), currMeter + 3);
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
