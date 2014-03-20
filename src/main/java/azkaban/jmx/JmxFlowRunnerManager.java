package azkaban.jmx;

import azkaban.execapp.FlowRunnerManager;

public class JmxFlowRunnerManager implements JmxFlowRunnerManagerMBean {
	private FlowRunnerManager manager;
	
	public JmxFlowRunnerManager(FlowRunnerManager manager) {
		this.manager = manager;
	}

	@Override
	public long getLastCleanerThreadCheckTime() {
		return manager.getLastCleanerThreadCheckTime();
	}

	@Override
	public long getLastSubmitterThreadCheckTime() {
		return manager.getLastSubmitterThreadCheckTime();
	}

	@Override
	public boolean isSubmitterThreadActive() {
		return manager.isSubmitterThreadActive();
	}

	@Override
	public boolean isCleanerThreadActive() {
		return manager.isCleanerThreadActive();
	}

	@Override
	public String getSubmitterThreadState() {
		return manager.getSubmitterThreadState().toString();
	}

	@Override
	public String getCleanerThreadState() {
		return manager.getCleanerThreadState().toString();
	}

	@Override
	public boolean isExecutorThreadPoolShutdown() {
		return manager.isExecutorThreadPoolShutdown();
	}

	@Override
	public int getNumExecutingFlows() {
		return manager.getNumExecutingFlows();
	}

	@Override
	public int countTotalNumRunningJobs() {
		return manager.getNumExecutingJobs();
	}

	@Override
	public String getRunningFlows() {
		return manager.getRunningFlowIds();
	}

}
