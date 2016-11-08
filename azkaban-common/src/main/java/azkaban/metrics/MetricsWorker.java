package azkaban.metrics;

public abstract class MetricsWorker {
  private String endpointName;

  public MetricsWorker(String endpointName) {
    this.endpointName = endpointName;
  }
}
