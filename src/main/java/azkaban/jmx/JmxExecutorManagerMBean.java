package azkaban.jmx;

import java.util.List;

public interface JmxExecutorManagerMBean {
	@DisplayName("OPERATION: getNumRunningFlows")
	public int getNumRunningFlows();
	
	@DisplayName("OPERATION: getRunningFlows")
	public String getRunningFlows();
	
	@DisplayName("OPERATION: getExecutorThreadState")
	public String getExecutorThreadState();
	
	@DisplayName("OPERATION: getExecutorThreadStage")
	public String getExecutorThreadStage();

	@DisplayName("OPERATION: isThreadActive")
	public boolean isThreadActive();

	@DisplayName("OPERATION: getLastThreadCheckTime")
	public Long getLastThreadCheckTime();

	@DisplayName("OPERATION: getPrimaryExecutorHostPorts")
	public List<String> getPrimaryExecutorHostPorts();
}
