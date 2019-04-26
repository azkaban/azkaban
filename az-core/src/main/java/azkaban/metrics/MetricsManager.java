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

import static azkaban.Constants.ConfigurationKeys.CUSTOM_METRICS_REPORTER_CLASS_NAME;
import static azkaban.Constants.ConfigurationKeys.METRICS_SERVER_URL;

import azkaban.utils.Props;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import java.lang.reflect.Constructor;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The singleton class, MetricsManager, is the place to have MetricRegistry and ConsoleReporter in
 * this class. Also, web servers and executors can call {@link #startReporting(String, Props)} to
 * start reporting AZ metrics to remote metrics server.
 */
@Singleton
public class MetricsManager {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);
  private final MetricRegistry registry;

  @Inject
  public MetricsManager(final MetricRegistry registry) {
    this.registry = registry;
    registerJvmMetrics();
  }

  private void registerJvmMetrics() {
    this.registry.register("MEMORY_Gauge", new MemoryUsageGaugeSet());
    this.registry.register("GC_Gauge", new GarbageCollectorMetricSet());
    this.registry.register("Thread_State_Gauge", new ThreadStatesGaugeSet());
  }

  /**
   * A {@link Meter} measures the rate of events over time (e.g., “requests per second”). Here we
   * track 1-minute moving averages.
   */
  public Meter addMeter(final String name) {
    final Meter curr = this.registry.meter(name);
    this.registry.register(name + "-gauge", (Gauge<Double>) curr::getOneMinuteRate);
    return curr;
  }

  /**
   * A {@link Gauge} is an instantaneous reading of a particular value. This method leverages
   * Supplier, a Functional Interface, to get Generics metrics values. With this support, no matter
   * what our interesting metrics is a Double or a Long, we could pass it to Metrics Parser.
   *
   * E.g., in {@link CommonMetrics#setupAllMetrics()}, we construct a supplier lambda by having a
   * AtomicLong object and its get method, in order to collect dbConnection metric.
   */
  public <T> void addGauge(final String name, final Supplier<T> gaugeFunc) {
    this.registry.register(name, (Gauge<T>) gaugeFunc::get);
  }

  /**
   * A {@link Counter} is just a gauge for an AtomicLong instance.
   */
  public Counter addCounter(final String name) {
    return this.registry.counter(name);
  }

  /**
   * A {@link Histogram} measures the statistical distribution of values in a stream of data. In
   * addition to minimum, maximum, mean, etc., it also measures median, 75th,
   * 90th, 95th, 98th, 99th, and 99.9th percentiles.
   */
  public Histogram addHistogram(final String name) { return this.registry.histogram(name); }

  /**
   * A {@link Timer} measures both the rate that a particular piece of code is called and the
   * distribution of its duration.
   */
  public Timer addTimer(final String name) { return this.registry.timer(name); }

  /**
   * reporting metrics to remote metrics collector. Note: this method must be synchronized, since
   * both web server and executor will call it during initialization.
   */
  public synchronized void startReporting(final String reporterName, final Props props) {
    final String metricsReporterClassName = props.get(CUSTOM_METRICS_REPORTER_CLASS_NAME);
    final String metricsServerURL = props.get(METRICS_SERVER_URL);
    if (metricsReporterClassName != null && metricsServerURL != null) {
      try {
        LOG.info("metricsReporterClassName: " + metricsReporterClassName);
        final Class metricsClass = Class.forName(metricsReporterClassName);

        final Constructor[] constructors = metricsClass.getConstructors();
        constructors[0].newInstance(reporterName, this.registry, metricsServerURL);

      } catch (final Exception e) {
        LOG.error("Encountered error while loading and instantiating "
            + metricsReporterClassName, e);
        throw new IllegalStateException("Encountered error while loading and instantiating "
            + metricsReporterClassName, e);
      }
    } else {
      LOG.error(String.format("No value for property: %s or %s was found",
          CUSTOM_METRICS_REPORTER_CLASS_NAME, METRICS_SERVER_URL));
    }
  }
}
