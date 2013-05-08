package azkaban.execapp.event;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;

public abstract class FlowWatcher {
	private Logger logger;
	
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
	
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	protected Logger getLogger() {
		return this.logger;
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
		logger.info("Failing all watches on " + execId);
		cancelWatch = true;
		
		for(BlockingStatus status : map.values()) {
			logger.info("Unblocking " + status.getJobId());
			status.changeStatus(Status.KILLED);
			status.unblock();
		}
		
		logger.info("Successfully failed all watches on " + execId);
	}
	
	public boolean isWatchCancelled() {
		return cancelWatch;
	}
	
	public abstract void stopWatcher();
}
