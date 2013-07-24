package azkaban.jmx;

import azkaban.triggerapp.TriggerRunnerManager;

public class JmxTriggerRunnerManager implements JmxTriggerRunnerManagerMBean {
	private TriggerRunnerManager manager;
	
	public JmxTriggerRunnerManager(TriggerRunnerManager manager) {
		this.manager = manager;
	}

	@Override
	public long getLastRunnerThreadCheckTime() {
		return manager.getLastRunnerThreadCheckTime();
	}

	@Override
	public boolean isRunnerThreadActive() {
		return manager.isRunnerThreadActive();
	}

}
