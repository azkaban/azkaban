package azkaban.jmx;

import java.util.List;

public interface JmxSLAManagerMBean {
	@DisplayName("OPERATION: getSLAThreadState")
	String getSLAThreadState();
	
	@DisplayName("OPERATION: isThreadActive")
	boolean isThreadActive();
	
	@DisplayName("OPERATION: getLastThreadCheckTime")
	Long getLastThreadCheckTime();
	
	@DisplayName("OPERATION: getNumActiveSLA")
	int getNumActiveSLA();
	
	@DisplayName("OPERATION: getSLASummary")
	List<String> getSLASummary();
}
