package azkaban.webapp;

import azkaban.metrics.MetricsWorker;
import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Gauge;

import azkaban.executor.ExecutorManager;

public class MetricsWebWorker extends MetricsWorker {
  private ExecutorManager _executorManager;

  public MetricsWebWorker(MetricsWebWorkerBuilder builder) {
    super(builder.endpointName);
    this._executorManager = builder._executorManager;
  }

  public void addExecutorManagerMetrics(MetricRegistry metrics) throws Exception {
    if (_executorManager == null)
      throw new Exception("TODO: ");

    metrics.register("WEB-NumRunningFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _executorManager.getRunningFlows().size();
      }
    });

    metrics.register("WEB-NumQueuedFlows", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return _executorManager.getQueuedFlowSize();
      }
    });
  }

  public static class MetricsWebWorkerBuilder {
    private ExecutorManager _executorManager;
    private String endpointName;

    public MetricsWebWorkerBuilder(String endpointName) {
      this.endpointName = endpointName;
    }

    public MetricsWebWorkerBuilder addExecutorManager(ExecutorManager executorManager) {
      this._executorManager = executorManager;
      return this;
    }

    public MetricsWebWorker build() {
      return new MetricsWebWorker(this);
    }
  }

}
