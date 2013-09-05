package azkaban.jmx;

public interface JmxFlowRunnerManagerMBean {
	@DisplayName("OPERATION: getLastCleanerThreadCheckTime")
	public long getLastCleanerThreadCheckTime();

	@DisplayName("OPERATION: getLastSubmitterThreadCheckTime")
	public long getLastSubmitterThreadCheckTime();

	@DisplayName("OPERATION: isSubmitterThreadActive")
	public boolean isSubmitterThreadActive();

	@DisplayName("OPERATION: isCleanerThreadActive")
	public boolean isCleanerThreadActive();

	@DisplayName("OPERATION: getSubmitterThreadState")
	public String getSubmitterThreadState();

	@DisplayName("OPERATION: getCleanerThreadState")
	public String getCleanerThreadState();

	@DisplayName("OPERATION: isExecutorThreadPoolShutdown")
	public boolean isExecutorThreadPoolShutdown();

	@DisplayName("OPERATION: getNumExecutingFlows")
	public int getNumExecutingFlows();
	
	@DisplayName("OPERATION: getRunningFlows")
	public String getRunningFlows();
	
	@DisplayName("OPERATION: getTotalNumRunningJobs")
	public int countTotalNumRunningJobs();
}
