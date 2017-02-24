package azkaban.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Creating an utility class to facilitate metrics class like {@link azkaban.metrics.CommonMetrics}
 * to share common operations easily.
 */
public final class MetricsUtility {

  private MetricsUtility() {
    //Utility class's constructor should not be called
  }

  /**
   * A {@link Meter} measures the rate of events over time (e.g., “requests per second”).
   * Here we track 1-minute moving averages.
   */
  public static Meter addMeter(String name, MetricRegistry registry) {
    Meter curr = registry.meter(name);
    registry.register(name + "-gauge", (Gauge<Double>) curr::getOneMinuteRate);
    return curr;
  }

  /**
   * A {@link Gauge} is an instantaneous reading of a particular value.
   * This method adds an AtomicLong number/metric to registry.
   */
  public static void addLongGauge(String name, AtomicLong value, MetricRegistry registry) {
    registry.register(name, (Gauge<Long>) value::get);
  }
}
