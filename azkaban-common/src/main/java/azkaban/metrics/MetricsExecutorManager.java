package azkaban.metrics;

import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Gauge;

import azkaban.executor.ExecutorManager;

public class MetricsExecutorManager {
  private ExecutorManager manager;
  private String endpointName;

  public MetricsExecutorManager(ExecutorManager manager, String endpointName) {
    this.manager = manager;
    this.endpointName = endpointName;
  }

  public void addMetrics(MetricRegistry metrics) {

    metrics.register(MetricRegistry.name(ExecutorManager.class, "cache-evictions"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return manager.getLastExecutorManagerThreadCheckTime();
      }
    });

    Gauge<Integer> gauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return 5;
      }
    };

    metrics.register("Another one", gauge);
  }
}
