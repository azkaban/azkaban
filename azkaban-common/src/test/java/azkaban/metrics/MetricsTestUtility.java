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

import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * This class is designed for a utility class to test drop wizard metrics
 */
public class MetricsTestUtility {

  /**
   * One Dummy Reporter extending {@link ScheduledReporter} collects metrics in various maps,
   * which test methods are able to access easily.
   */
  public static class DummyReporter extends ScheduledReporter{

    private Map<String, String> gaugeMap;

    private Map<String, Long> meterMap;

    public DummyReporter(MetricRegistry registry) {
      super(registry, "dummy-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
      this.gaugeMap = new HashMap<>();
      this.meterMap = new HashMap<>();
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
        SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms,
        SortedMap<String, Meter> meters,
        SortedMap<String, Timer> timers) {

      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        gaugeMap.put(entry.getKey(), entry.getValue().getValue().toString());
      }

      for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        meterMap.put(entry.getKey(), entry.getValue().getCount());
      }
    }

    private String getGauge(String key) {
      return gaugeMap.get(key);
    }

    private long getMeter(String key) {
      return meterMap.getOrDefault(key, 0L);
    }
  }

  public static void testMeter(String meterName, DummyReporter dr, Runnable runnable) {

    sleepMillis(20);
    long currMeter = dr.getMeter(meterName);
    runnable.run();
    sleepMillis(20);
    Assert.assertEquals(dr.getMeter(meterName), currMeter + 1);

    runnable.run();
    runnable.run();
    sleepMillis(20);
    Assert.assertEquals(dr.getMeter(meterName), currMeter + 3);
  }

  public static void testGauge(String GaugeName, DummyReporter dr, Consumer<Long> func) {
    func.accept(1L);
    sleepMillis(20);
    Assert.assertEquals(dr.getGauge(GaugeName), "1");
    func.accept(99L);
    sleepMillis(20);
    Assert.assertEquals(dr.getGauge(GaugeName), "99");
  }


  /**
   * Helper method to sleep milli seconds.
   */
  private static void sleepMillis(int numMilli) {
    try {
      Thread.sleep(numMilli);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
