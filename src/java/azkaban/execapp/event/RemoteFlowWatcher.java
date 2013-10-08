package azkaban.execapp.event;

import java.util.ArrayList;
import java.util.Map;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;

public class RemoteFlowWatcher extends FlowWatcher {
	private final static long CHECK_INTERVAL_MS = 60*1000;
	
	private int execId;
	private ExecutorLoader loader;
	private ExecutableFlow flow;
	private RemoteUpdaterThread thread;
	private boolean isShutdown = false;
	
	// Every minute
	private long checkIntervalMs = CHECK_INTERVAL_MS;
	
	public RemoteFlowWatcher(int execId, ExecutorLoader loader) {
		this(execId, loader, CHECK_INTERVAL_MS);
	}
	
	public RemoteFlowWatcher(int execId, ExecutorLoader loader, long interval) {
		super(execId);
		checkIntervalMs = interval;
		
		try {
			flow = loader.fetchExecutableFlow(execId);
		} catch (ExecutorManagerException e) {
			return;
		}
		
		super.setFlow(flow);
		this.loader = loader;
		this.execId = execId;
		if (flow != null) {
			this.thread = new RemoteUpdaterThread();
			this.thread.setName("Remote-watcher-flow-" + execId);
			this.thread.start();
		}
	}
	
	private class RemoteUpdaterThread extends Thread {
		@Override
		public void run() {
			do {
				ExecutableFlow updateFlow = null;
				try {
					updateFlow = loader.fetchExecutableFlow(execId);
				} catch (ExecutorManagerException e) {
					e.printStackTrace();
					isShutdown = true;
				}
				
				long updateTime = 0;
				if (flow == null) {
					flow = updateFlow;
				}
				else {
					Map<String, Object> updateData = updateFlow.toUpdateObject(updateTime);
					ArrayList<ExecutableNode> updatedNodes = new ArrayList<ExecutableNode>();
					flow.applyUpdateObject(updateData, updatedNodes);

					flow.setStatus(updateFlow.getStatus());
					flow.setEndTime(updateFlow.getEndTime());
					flow.setUpdateTime(updateFlow.getUpdateTime());
					
					for (ExecutableNode node : updatedNodes) {
						handleJobStatusChange(node.getNestedId(), node.getStatus());
					}
					
					updateTime = flow.getUpdateTime();
				}
				
				if (Status.isStatusFinished(flow.getStatus())) {
					isShutdown = true;
				}
				else {
					synchronized(this) {
						try {
							wait(checkIntervalMs);
						} catch (InterruptedException e) {
						}
					}
				}
			} while (!isShutdown);
		}
		
	}
	
	@Override
	public synchronized void stopWatcher() {
		if(isShutdown) {
			return;
		}
		isShutdown = true;
		if (thread != null) {
			thread.interrupt();
		}
		super.failAllWatches();
		loader = null;
		flow = null;
	}
}
