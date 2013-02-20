package azkaban.jmx;

import azkaban.execapp.FlowRunnerManager;

public class JmxFlowRunnerManager {
	private FlowRunnerManager manager;
	
	public JmxFlowRunnerManager(FlowRunnerManager manager) {
		this.manager = manager;
	}
}
