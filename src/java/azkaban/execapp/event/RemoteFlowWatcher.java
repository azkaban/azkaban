package azkaban.execapp.event;

import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;

public class RemoteFlowWatcher extends FlowWatcher {
	private final static long CHECK_INTERVAL_MS = 60*1000;
	
	private int execId;
	private ExecutorLoader loader;
	private ExecutableFlowBase flow;
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
				ExecutableFlowBase updateFlow = null;
				try {
					updateFlow = loader.fetchExecutableFlow(execId);
				} catch (ExecutorManagerException e) {
					e.printStackTrace();
					isShutdown = true;
				}
				
				if (flow == null) {
					flow = updateFlow;
				}
				else {
					flow.setStatus(updateFlow.getStatus());
					flow.setEndTime(updateFlow.getEndTime());
					flow.setUpdateTime(updateFlow.getUpdateTime());
					
					for (ExecutableNode node : flow.getExecutableNodes()) {
						String jobId = node.getId();
						ExecutableNode newNode = updateFlow.getExecutableNode(jobId);
						long updateTime = node.getUpdateTime();
						node.setUpdateTime(newNode.getUpdateTime());
						node.setStatus(newNode.getStatus());
						node.setStartTime(newNode.getStartTime());
						node.setEndTime(newNode.getEndTime());
						
						if (updateTime < newNode.getUpdateTime()) {
							handleJobFinished(jobId, newNode.getStatus());
						}
					}
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
