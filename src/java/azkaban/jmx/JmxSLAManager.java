package azkaban.jmx;

import java.util.ArrayList;
import java.util.List;

import azkaban.sla.SLA;
import azkaban.sla.SLAManager;

public class JmxSLAManager implements JmxSLAManagerMBean {
	private final SLAManager manager;
	
	public JmxSLAManager(SLAManager manager) {
		this.manager = manager;
	}

	@Override
	public String getSLAThreadState() {
		return manager.getSLAThreadState().toString();
	}

	@Override
	public boolean isThreadActive() {
		return manager.isThreadActive();
	}

	@Override
	public Long getLastThreadCheckTime() {
		return manager.getLastCheckTime();
	}

	@Override
	public int getNumActiveSLA() {
		return manager.getNumActiveSLA();
	}

	@Override
	public List<String> getSLASummary() {
		ArrayList<String> summary = new ArrayList<String>();
		for(SLA sla: manager.getSLAList()) {
			summary.add(sla.toString());
		}
		return summary;
	}

}
