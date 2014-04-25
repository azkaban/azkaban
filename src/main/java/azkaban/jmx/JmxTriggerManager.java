package azkaban.jmx;

import azkaban.trigger.TriggerManagerAdapter;
import azkaban.trigger.TriggerManagerAdapter.TriggerJMX;

public class JmxTriggerManager implements JmxTriggerManagerMBean {
	private TriggerJMX jmxStats;

	public JmxTriggerManager(TriggerManagerAdapter manager) {
		this.jmxStats = manager.getJMX();
	}

	@Override
	public long getLastRunnerThreadCheckTime() {
		return jmxStats.getLastRunnerThreadCheckTime();
	}

	@Override
	public boolean isRunnerThreadActive() {
		return jmxStats.isRunnerThreadActive();
	}

	@Override
	public String getPrimaryTriggerHostPort() {
		return jmxStats.getPrimaryServerHost();
	}

//	@Override
//	public List<String> getAllTriggerHostPorts() {
//		return new ArrayList<String>(manager.getAllActiveTriggerServerHosts());
//	}

	@Override
	public int getNumTriggers() {
		return jmxStats.getNumTriggers();
	}

	@Override
	public String getTriggerSources() {
		return jmxStats.getTriggerSources();
	}

	@Override
	public String getTriggerIds() {
		return jmxStats.getTriggerIds();
	}

	@Override
	public long getScannerIdleTime() {
		return jmxStats.getScannerIdleTime();
	}

	@Override
	public String getScannerThreadStage() {
		// TODO Auto-generated method stub
		return jmxStats.getScannerThreadStage();
	}
}
