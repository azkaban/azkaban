package azkaban.jmx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import azkaban.executor.ExecutorManagerAdapter;

public class JmxExecutorManagerAdapter implements JmxExecutorManagerAdapterMBean {
	private ExecutorManagerAdapter manager;

	public JmxExecutorManagerAdapter(ExecutorManagerAdapter manager) {
		this.manager = manager;
	}

	@Override
	public int getNumRunningFlows() {
		try {
			return this.manager.getRunningFlows().size();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public String getExecutorManagerThreadState() {
		return manager.getExecutorManagerThreadState().toString();
	}

	@Override
	public boolean isExecutorManagerThreadActive() {
		return manager.isExecutorManagerThreadActive();
	}

	@Override
	public Long getLastExecutorManagerThreadCheckTime() {
		return manager.getLastExecutorManagerThreadCheckTime();
	}
	
	@Override 
	public List<String> getPrimaryExecutorHostPorts() {
		return new ArrayList<String>(manager.getPrimaryServerHosts());
	}

}
