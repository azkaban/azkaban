package azkaban.jmx;

import azkaban.scheduler.ScheduleManager;

public class JmxScheduler implements JmxSchedulerMBean {
	private final ScheduleManager manager;
	
	public JmxScheduler(ScheduleManager manager) {
		this.manager = manager;
	}
	
	@Override
	public String getScheduleThreadState() {
		return manager.getThreadState().toString();
	}

	@Override
	public Long getNextScheduleTime() {
		return manager.getNextUpdateTime();
	}

	@Override
	public Long getLastThreadCheckTime() {
		return manager.getLastCheckTime();
	}

	@Override
	public Boolean isThreadActive() {
		return manager.isThreadActive();
	}

	@Override
	public String getScheduleThreadStage() {
		return manager.getThreadStage();
	}
}