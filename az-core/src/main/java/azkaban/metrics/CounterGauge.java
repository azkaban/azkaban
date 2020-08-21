package azkaban.metrics;

import com.codahale.metrics.Gauge;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom Gauge which reports the number of events in the last reporting interval. The event count
 * resets from one interval to the next.
 */
public class CounterGauge implements Gauge<Long> {

  private final AtomicLong aggregate = new AtomicLong();

  @Override
  public Long getValue() {
    return this.aggregate.getAndSet(0);
  }

  public void add(final long value) {
    this.aggregate.addAndGet(value);
  }
}
