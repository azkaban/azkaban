package azkaban.execapp.jmx;

import java.util.Map;

import azkaban.jmx.DisplayName;

/**
 * Define all the MBean attributes at the job level
 * 
 * @author hluu
 *
 */
public interface JmxJobMXBean {

  @DisplayName("OPERATION: getNumRunningJobs")
  public int getNumRunningJobs();

  @DisplayName("OPERATION: getTotalNumExecutedJobs")
  public int getTotalNumExecutedJobs();

  @DisplayName("OPERATION: getTotalFailedJobs")
  public int getTotalFailedJobs();

  @DisplayName("OPERATION: getTotalSucceededJobs")
  public int getTotalSucceededJobs();

  @DisplayName("OPERATION: getTotalSucceededJobsByJobType")
  public Map<String, Integer> getTotalSucceededJobsByJobType();

  @DisplayName("OPERATION: getTotalFailedJobsByJobType")
  public Map<String, Integer> getTotalFailedJobsByJobType();

}
