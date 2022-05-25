package azkaban.jmx;

import org.apache.http.impl.client.FutureRequestExecutionMetrics;

public class JmxJobCallback implements JmxJobCallbackMBean {

  private final FutureRequestExecutionMetrics jobCallbackMetrics;

  public JmxJobCallback(final FutureRequestExecutionMetrics jobCallbackMetrics) {
    this.jobCallbackMetrics = jobCallbackMetrics;
  }

  @Override
  public long getNumJobCallbacks() {
    return this.jobCallbackMetrics.getRequestCount();
  }

  @Override
  public long getNumSuccessfulJobCallbacks() {
    return this.jobCallbackMetrics.getSuccessfulConnectionCount();
  }

  @Override
  public long getNumFailedJobCallbacks() {
    return this.jobCallbackMetrics.getFailedConnectionCount();
  }

  @Override
  public long getNumActiveJobCallbacks() {
    return this.jobCallbackMetrics.getActiveConnectionCount();
  }

}
