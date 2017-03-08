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
    registry.register(name + "-gauge", (Gauge<Double>) curr::getFifteenMinuteRate);
    return curr;
  }

  /**
   * A {@link Gauge} is an instantaneous reading of a particular value.
   * This method leverages Supplier, a Functional Interface, to get Generics metrics values.
   * With this support, no matter what our interesting metrics is a Double or a Long, we could pass it
   * to Metrics Parser.
   *
   * E.g., in {@link CommonMetrics#setupAllMetrics()}, we construct a supplier lambda by having
   * a AtomicLong object and its get method, in order to collect dbConnection metric.
   */
  public static <T> void addGauge(String name, MetricRegistry registry, Supplier<T> gaugeFunc) {
    registry.register(name, (Gauge<T>) gaugeFunc::get);
  }
}
