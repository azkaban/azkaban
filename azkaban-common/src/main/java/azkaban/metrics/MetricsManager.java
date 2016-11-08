package azkaban.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ConsoleReporter;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public enum MetricsManager {
  INSTANCE;

  private final MetricRegistry registry        = new MetricRegistry();
  private ConsoleReporter consoleReporter      = null;

  /**
   * Return the Metrics registry.
   *
   * @return the single {@code MetricRegistry} used for all of Az Metrics
   *         monitoring
   */
  public MetricRegistry getRegistry() {
    return registry;
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
