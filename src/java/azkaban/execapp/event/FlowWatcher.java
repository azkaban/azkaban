package azkaban.execapp.event;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;

public abstract class FlowWatcher {
	private int execId;
	private ExecutableFlow flow;
	private Map<String, BlockingStatus> map = new ConcurrentHashMap<String, BlockingStatus>();
	private boolean cancelWatch = false;
	
	public FlowWatcher(int execId) {
		this.execId = execId;
	}
	
	public void setFlow(ExecutableFlow flow) {
		this.flow = flow;
	}
	
	/**
	 * Called to fire events to the JobRunner listeners
	 * @param jobId
	 */
	protected synchronized void handleJobFinished(String jobId, Status status) {
		BlockingStatus block = map.get(jobId);
		if (block != null) {
			block.changeStatus(status);
		}
	}

	public int getExecId() {
		return execId;
	}
	
	public synchronized BlockingStatus getBlockingStatus(String jobId) {
		if (cancelWatch) {
			return null;
		}
		
		ExecutableNode node = flow.getExecutableNode(jobId);
		if (node == null) {
			return null;
		}
		
		BlockingStatus blockingStatus = map.get(jobId);
		if (blockingStatus == null) {
			blockingStatus = new BlockingStatus(execId, jobId, node.getStatus());
			map.put(jobId, blockingStatus);
		}

		return blockingStatus;
	}
	
	public Status peekStatus(String jobId) {
		ExecutableNode node = flow.getExecutableNode(jobId);
		if (node != null) {
			return node.getStatus();
		}
		
		return null;
	}
	
	public synchronized void failAllWatches() {
		cancelWatch = true;
		
		for(BlockingStatus status : map.values()) {
			status.unblock();
		}
	}
	
	public abstract void stopWatcher();
}
