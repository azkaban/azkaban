package azkaban.jmx;

public interface JmxSchedulerMBean {
	@DisplayName("OPERATION: getScheduleThreadState")
	String getScheduleThreadState();
	
	@DisplayName("OPERATION: getScheduleThreadStage")
	String getScheduleThreadStage();
	
	@DisplayName("OPERATION: getNextScheduleTime")
	Long getNextScheduleTime();
	
	@DisplayName("OPERATION: getLastCheckTime")
	Long getLastThreadCheckTime();
	
	@DisplayName("OPERATION: isThreadActive")
	Boolean isThreadActive();
}