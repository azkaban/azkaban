package azkaban.jmx;

import java.util.ArrayList;
import java.util.List;

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
	public String getExecutorThreadStage() {
		return manager.getExecutorThreadStage();
	}

	@Override
	public boolean isThreadActive() {
		return manager.isThreadActive();
	}

	@Override
	public Long getLastThreadCheckTime() {
		return manager.getLastThreadCheckTime();
	}
	
	@Override 
	public List<String> getPrimaryExecutorHostPorts() {
		return new ArrayList<String>(manager.getPrimaryServerHosts());
	}

	@Override
	public String getRunningFlows() {
		return manager.getRunningFlowIds();
	}

	
}
