package azkaban.execapp.jmx;

import org.apache.http.impl.client.FutureRequestExecutionMetrics;

public class JmxJobCallback implements JmxJobCallbackMBean {

  private FutureRequestExecutionMetrics jobCallbackMetrics;

  public JmxJobCallback(FutureRequestExecutionMetrics jobCallbackMetrics) {
    this.jobCallbackMetrics = jobCallbackMetrics;
  }

  @Override
  public long getNumJobCallbacks() {
    return jobCallbackMetrics.getRequestCount();
  }

  @Override
  public long getNumSuccessfulJobCallbacks() {
    return jobCallbackMetrics.getSuccessfulConnectionCount();
  }

  @Override
  public long getNumFailedJobCallbacks() {
    return jobCallbackMetrics.getFailedConnectionCount();
  }

  @Override
  public long getNumActiveJobCallbacks() {
    return jobCallbackMetrics.getActiveConnectionCount();
  }

}
