/*
 * Copyright 2016 LinkedIn Corp.
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

import azkaban.AzkabanConstants;
import azkaban.utils.Props;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.concurrent.TimeUnit;


public enum MetricsManager {
  INSTANCE;

  private final MetricRegistry registry        = new MetricRegistry();
  private ConsoleReporter consoleReporter      = null;
  private static final Logger logger = Logger.getLogger(MetricsManager.class);

  /**
   * Constructor is eaagerly called when this class is loaded.
   */
  private MetricsManager() {
    registry.register("MEMORY_Gauge", new MemoryUsageGaugeSet());
    registry.register("GC_Gauge", new GarbageCollectorMetricSet());
    registry.register("Thread_State_Gauge", new ThreadStatesGaugeSet());
  }
  /**
   * Return the Metrics registry.
   *
   * @return the single {@code MetricRegistry} used for all of Az Metrics
   *         monitoring
   */
  public MetricRegistry getRegistry() {
    return registry;
  }

  public void startReporting(Props props) {
    String metricsReporterClassName = props.get(AzkabanConstants.CUSTOM_METRICS_REPORTER_CLASS_NAME);
    String metricsServerURL = props.get(AzkabanConstants.CUSTOM_METRICS_REPORTER_CLASS_NAME);
    if (metricsReporterClassName != null && metricsServerURL != null) {
      try {
        logger.info("metricsReporterClassName: " + metricsReporterClassName);
        Class metricsClass = Class.forName(metricsReporterClassName);

        Constructor[] constructors =
            metricsClass.getConstructors();
        constructors[0].newInstance(registry, metricsServerURL);

      } catch (Exception e) {
        logger.error("Encountered error while loading and instantiating "
            + metricsReporterClassName, e);
        throw new IllegalStateException(
            "Encountered error while loading and instantiating "
                + metricsReporterClassName, e);
      }
    } else {
      logger.info("No value for property: "
          + AzkabanConstants.CUSTOM_METRICS_REPORTER_CLASS_NAME
          + "or" + AzkabanConstants.METRICS_SERVER_URL + " was found");
    }

  }

  /**
   * Create a ConsoleReporter to the AZ Metrics registry.
   * @param reportInterval
   *            time to wait between dumping metrics to the console
   */
  public synchronized void addConsoleReporter(Duration reportInterval) {
    if (null != consoleReporter) {
      return;
    }

    consoleReporter = ConsoleReporter.forRegistry(getRegistry()).build();
    consoleReporter.start(reportInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Stop ConsoldeReporter previously created by a call to
   * {@link #addConsoleReporter(Duration)} and release it for GC.
   */
  public synchronized void removeConsoleReporter() {
    if (null != consoleReporter)
      consoleReporter.stop();

    consoleReporter = null;
  }
}
