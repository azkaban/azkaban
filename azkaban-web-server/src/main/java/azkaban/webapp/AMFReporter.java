package azkaban.webapp;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//todo: use import org.apache.commons.logging.Log; to be consistent with other files.

public class AMFReporter extends ScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AMFReporter.class);

  private final AmfSender _sender;

  protected AMFReporter(MetricRegistry registry, String serverUrl)
      throws Exception {
    super(registry, "AMF", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    LOGGER.info("AmfReporter Initializing. serverUrl: {}", serverUrl);
    // todo: get from config
    _sender = new AmfSender("RayTestHiveMetastore", serverUrl);
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    ArrayList<MetricEntry> metrics = new ArrayList<>();

    LOGGER.debug("AmfReporter report called.");
    // AMF expects timestamps to be in seconds.
    long timeStamp = System.currentTimeMillis() / 1000;

    if (!gauges.isEmpty()) {
      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        String key = entry.getKey();
        Gauge gauge = entry.getValue();

        try {
          String s = gauge.getValue().toString();
          double value = Double.parseDouble(s);

          MetricEntry metricEntry = new MetricEntry(key, timeStamp, value, false);
          metrics.add(metricEntry);
        } catch (NumberFormatException e) {
          LOGGER.debug("Received a gauge whose value can't be converted to a double value. {}", gauge.getValue());
        }
      }
    }
    if (!counters.isEmpty()) {
      for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        String key = entry.getKey();
//        LOGGER.debug("counter key: " + key);
        Counter counter = entry.getValue();
        double value = (double) counter.getCount();

        MetricEntry metricEntry = new MetricEntry(key, timeStamp, value, true);
        metrics.add(metricEntry);
      }
    }
    if (!timers.isEmpty()) {
      for (Map.Entry<String, Timer> entry : timers.entrySet()) {
        String key = entry.getKey();
        Timer timer = entry.getValue();
        addTimer(metrics, key, timer, timeStamp);
      }
    }

    try {
      _sender.putMetrics(metrics);
    } catch (Exception e) {
      //todo: only print error once per successive errors
      LOGGER.error("Failed to send metrics. Exception: %s", e.getMessage());
    }
  }

  private void addTimer(ArrayList<MetricEntry> metrics, String name, Timer timer, long timeStamp) {
    String nameWithDot = name + ".";
    MetricEntry metricEntry;
    metricEntry = new MetricEntry(nameWithDot + "count", timeStamp, timer.getCount(), true);
    metrics.add(metricEntry);
    metricEntry = new MetricEntry(nameWithDot + "meanRate", timeStamp, convertRate(timer.getMeanRate()), false);
    metrics.add(metricEntry);

    metricEntry =
        new MetricEntry(nameWithDot + "oneMinuteRate", timeStamp, convertRate(timer.getOneMinuteRate()), false);
    metrics.add(metricEntry);

    metricEntry =
        new MetricEntry(nameWithDot + "fiveMinuteRate", timeStamp, convertRate(timer.getFiveMinuteRate()), false);
    metrics.add(metricEntry);

    metricEntry =
        new MetricEntry(nameWithDot + "fifteenMinuteRate", timeStamp, convertRate(timer.getFifteenMinuteRate()), false);
    metrics.add(metricEntry);

    addTimerSnapshot(metrics, timer, nameWithDot, timeStamp);

  }

  private void addTimerSnapshot(ArrayList<MetricEntry> metrics, Timer timer, String namePrefix, long timeStamp) {
    final Snapshot snapshot = timer.getSnapshot();

    addOneTimerAttribute(metrics, timeStamp, namePrefix, "min", snapshot.getMin());
    addOneTimerAttribute(metrics, timeStamp, namePrefix, "max", snapshot.getMax());
    addOneTimerAttribute(metrics, timeStamp, namePrefix, "mean", snapshot.getMean());
    addOneTimerAttribute(metrics, timeStamp, namePrefix, "stddev", snapshot.getStdDev());
    addOneTimerAttribute(metrics, timeStamp, namePrefix, "median", snapshot.getMedian());
    addOneTimerAttribute(metrics, timeStamp, namePrefix, "75thPercentile", snapshot.get75thPercentile());
    addOneTimerAttribute(metrics, timeStamp, namePrefix, "95thPercentile", snapshot.get95thPercentile());
    addOneTimerAttribute(metrics, timeStamp, namePrefix, "99thPercentile", snapshot.get99thPercentile());
    addOneTimerAttribute(metrics, timeStamp, namePrefix, "99.9thPercentile", snapshot.get999thPercentile());
  }

  private void addOneTimerAttribute(ArrayList<MetricEntry> metrics, long timeStamp, String namePrefix, String name, double value) {
    MetricEntry metricEntry = new MetricEntry(namePrefix + name, timeStamp, convertDuration(value), false);
    metrics.add(metricEntry);
  }
}

class MetricEntry {
  private String name;
  private long time;
  private double value;
  private boolean isCounter;

  public MetricEntry(String name, long time, double value, boolean isCounter) {
    this.name = name;
    this.time = time;
    this.value = value;
    this.isCounter = isCounter;
  }

  public String getName() {
    return name;
  }

  public long getTime() {
    return time;
  }

  public double getValue() {
    return value;
  }

  public boolean isCounter() {
    return isCounter;
  }
}
