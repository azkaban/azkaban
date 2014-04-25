package azkaban.jmx;

public interface JmxTriggerManagerMBean {	
	
	@DisplayName("OPERATION: getLastThreadCheckTime")
	public long getLastRunnerThreadCheckTime();

	@DisplayName("OPERATION: isThreadActive")
	public boolean isRunnerThreadActive();

	@DisplayName("OPERATION: getPrimaryTriggerHostPort")
	public String getPrimaryTriggerHostPort();
	
//	@DisplayName("OPERATION: getAllTriggerHostPorts")
//	public List<String> getAllTriggerHostPorts();
	
	@DisplayName("OPERATION: getNumTriggers")
	public int getNumTriggers();
	
	@DisplayName("OPERATION: getTriggerSources")
	public String getTriggerSources();
	
	@DisplayName("OPERATION: getTriggerIds")
	public String getTriggerIds();
	
	@DisplayName("OPERATION: getScannerIdleTime")
	public long getScannerIdleTime();
	
	@DisplayName("OPERATION: getScannerThreadStage")
	public String getScannerThreadStage();
}
