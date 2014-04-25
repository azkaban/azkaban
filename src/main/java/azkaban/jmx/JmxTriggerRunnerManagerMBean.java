package azkaban.jmx;

public interface JmxTriggerRunnerManagerMBean {

	@DisplayName("OPERATION: getLastRunnerThreadCheckTime")
	public long getLastRunnerThreadCheckTime();

	@DisplayName("OPERATION: getNumTriggers")
	public int getNumTriggers();
	
	@DisplayName("OPERATION: isRunnerThreadActive")
	public boolean isRunnerThreadActive();
	
	@DisplayName("OPERATION: getTriggerSources")
	public String getTriggerSources();
	
	@DisplayName("OPERATION: getTriggerIds")
	public String getTriggerIds();
	
	@DisplayName("OPERATION: getScannerIdleTime")
	public long getScannerIdleTime();
	
}
