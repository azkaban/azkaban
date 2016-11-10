package azkaban.execapp;


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import azkaban.execapp.FlowRunnerManager;

public class MetricsExecRegister {

  private String endpointName;
  private FlowRunnerManager _flowRunnerManager;

  public MetricsExecRegister(MetricsExecRegisterBuilder builder) {
    this.endpointName = builder.endpointName;
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

  public static class MetricsExecRegisterBuilder {
    private FlowRunnerManager _flowRunnerManager;
    private String endpointName;

    public MetricsExecRegisterBuilder(String endpointName) {
      this.endpointName = endpointName;
    }

    public MetricsExecRegisterBuilder addFlowRunnerManager(FlowRunnerManager flowRunnerManager) {
      this._flowRunnerManager = flowRunnerManager;
      return this;
    }

    public MetricsExecRegister build() {
      return new MetricsExecRegister(this);
    }
  }

}
