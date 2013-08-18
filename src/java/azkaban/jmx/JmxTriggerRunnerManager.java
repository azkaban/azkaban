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

	@Override
	public int getNumTriggers() {
		return manager.getNumTriggers();
	}

	@Override
	public String getTriggerSources() {
		return manager.getTriggerSources();
	}

	@Override
	public String getTriggerIds() {
		return manager.getTriggerIds();
	}

	@Override
	public long getScannerIdleTime() {
		return manager.getScannerIdleTime();
	}

}
