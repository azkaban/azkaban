package azkaban.jmx;

import java.util.List;

public interface JmxExecutorManagerMBean {
	@DisplayName("OPERATION: getNumRunningFlows")
	public int getNumRunningFlows();
	
	@DisplayName("OPERATION: getExecutorThreadState")
	public String getExecutorThreadState();

	@DisplayName("OPERATION: isThreadActive")
	public boolean isThreadActive();

	@DisplayName("OPERATION: getLastThreadCheckTime")
	public Long getLastThreadCheckTime();

	@DisplayName("OPERATION: getPrimaryExecutorHostPorts")
	public List<String> getPrimaryExecutorHostPorts();
}
