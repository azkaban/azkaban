package azkaban.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.function.Supplier;

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
   * A {@link Timer} aggregates timing durations and provides duration statistics, plus throughput statistics
   * TODO: experimented timer but finally removed. but leave the API here to be used in future.
   */
  public static Timer addTimer(String name, MetricRegistry registry) {
    return registry.timer(name);
  }

  /**
   * A {@link Gauge} is an instantaneous reading of a particular value.
   * This method adds a general supplier function to the metrics registry.
   */
  public static <T> void addGauge(String name, MetricRegistry registry, Supplier<T> gaugeFunc) {
    registry.register(name, (Gauge<T>) gaugeFunc::get);
  }
}
