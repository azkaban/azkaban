package azkaban.jmx;

import java.util.List;

public interface JmxTriggerManagerMBean {	
	
	@DisplayName("OPERATION: getLastThreadCheckTime")
	public String getLastThreadCheckTime();

	@DisplayName("OPERATION: isThreadActive")
	public boolean isThreadActive();

	@DisplayName("OPERATION: getPrimaryTriggerHostPorts")
	public List<String> getPrimaryTriggerHostPorts();
	
	@DisplayName("OPERATION: getAllTriggerHostPorts")
	public List<String> getAllTriggerHostPorts();
	
	@DisplayName("OPERATION: getNumTriggers")
	public int getNumTriggers();
	
	@DisplayName("OPERATION: getTriggerSources")
	public String getTriggerSources();
	
	@DisplayName("OPERATION: getTriggerIds")
	public String getTriggerIds();
}
