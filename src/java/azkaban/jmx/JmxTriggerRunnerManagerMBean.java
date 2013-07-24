package azkaban.jmx;

public interface JmxTriggerRunnerManagerMBean {

	@DisplayName("OPERATION: getLastRunnerThreadCheckTime")
	public long getLastRunnerThreadCheckTime();

	@DisplayName("OPERATION: isRunnerThreadActive")
	public boolean isRunnerThreadActive();

}
