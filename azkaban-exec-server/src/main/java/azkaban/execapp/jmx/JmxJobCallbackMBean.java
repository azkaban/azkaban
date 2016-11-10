package azkaban.execapp.jmx;

import azkaban.jmx.DisplayName;

public interface JmxJobCallbackMBean {
  @DisplayName("OPERATION: getNumJobCallbacks")
  public long getNumJobCallbacks();

  @DisplayName("OPERATION: getNumSuccessfulJobCallbacks")
  public long getNumSuccessfulJobCallbacks();

  @DisplayName("OPERATION: getNumFailedJobCallbacks")
  public long getNumFailedJobCallbacks();

  @DisplayName("OPERATION: getNumActiveJobCallbacks")
  public long getNumActiveJobCallbacks();

}
