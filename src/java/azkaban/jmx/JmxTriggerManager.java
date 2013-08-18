package azkaban.jmx;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import azkaban.trigger.TriggerManager;

public class JmxTriggerManager implements JmxTriggerManagerMBean {
	private TriggerManager manager;

	public JmxTriggerManager(TriggerManager manager) {
		this.manager = manager;
	}

	@Override
	public String getLastThreadCheckTime() {
		return new DateTime(manager.getLastThreadCheckTime()).toString();
	}

	@Override
	public boolean isThreadActive() {
		return manager.isThreadActive();
	}

	@Override
	public List<String> getPrimaryTriggerHostPorts() {
		return new ArrayList<String>(manager.getPrimaryServerHosts());
	}

	@Override
	public List<String> getAllTriggerHostPorts() {
		return new ArrayList<String>(manager.getAllActiveTriggerServerHosts());
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
	
}
