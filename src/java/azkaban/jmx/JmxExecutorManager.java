package azkaban.jmx;

import azkaban.executor.ExecutorManager;

public class JmxExecutorManager implements JmxExecutorManagerMBean {
	private ExecutorManager manager;

	public JmxExecutorManager(ExecutorManager manager) {
		this.manager = manager;
	}

	@Override
	public int getNumRunningFlows() {
		return this.manager.getRunningFlows().size();
	}

	@Override
	public String getExecutorThreadState() {
		return manager.getExecutorThreadState().toString();
	}

	@Override
	public boolean isThreadActive() {
		return manager.isThreadActive();
	}

	@Override
	public Long getLastThreadCheckTime() {
		return manager.getLastThreadCheckTime();
	}
}
