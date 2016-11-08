package azkaban.execapp;


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import azkaban.execapp.FlowRunnerManager;
import azkaban.metrics.MetricsWorker;


public class MetricsExecWorker extends MetricsWorker {
  private FlowRunnerManager _flowRunnerManager;

  public MetricsExecWorker(MetricsExecWorkerBuilder builder) {
    super(builder.endpointName);
    this._flowRunnerManager = builder._flowRunnerManager;
  }

  public void addExecutorManagerMetrics(MetricRegistry metrics) throws Exception {
    if (_flowRunnerManager == null)
      throw new Exception("TODO: ");

    metrics.register("EXEC-NumRunningFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _flowRunnerManager.getNumRunningFlows();
      }
    });

    metrics.register("EXEC-NumQueuedFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _flowRunnerManager.getNumQueuedFlows();
      }
    });
  }

  public static class MetricsExecWorkerBuilder {
    private FlowRunnerManager _flowRunnerManager;
    private String endpointName;

    public MetricsExecWorkerBuilder(String endpointName) {
      this.endpointName = endpointName;
    }

    public MetricsExecWorkerBuilder addFlowRunnerManager(FlowRunnerManager flowRunnerManager) {
      this._flowRunnerManager = flowRunnerManager;
      return this;
    }

    public MetricsExecWorker build() {
      return new MetricsExecWorker(this);
    }
  }

}
