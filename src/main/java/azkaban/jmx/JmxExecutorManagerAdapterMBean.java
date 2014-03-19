package azkaban.jmx;

import java.util.List;

public interface JmxExecutorManagerAdapterMBean {
	@DisplayName("OPERATION: getNumRunningFlows")
	public int getNumRunningFlows();
	
	@DisplayName("OPERATION: getExecutorThreadState")
	public String getExecutorManagerThreadState();

	@DisplayName("OPERATION: isThreadActive")
	public boolean isExecutorManagerThreadActive();

	@DisplayName("OPERATION: getLastThreadCheckTime")
	public Long getLastExecutorManagerThreadCheckTime();

	@DisplayName("OPERATION: getPrimaryExecutorHostPorts")
	public List<String> getPrimaryExecutorHostPorts();
	
//	@DisplayName("OPERATION: getExecutorThreadStage")
//	public String getExecutorThreadStage();
//	
//	@DisplayName("OPERATION: getRunningFlows")
//	public String getRunningFlows();
}
