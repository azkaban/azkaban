package azkaban.executor;

import azkaban.executor.ExecutableFlow.ExecutableNode;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.event.EventHandler;
import azkaban.utils.Props;

public class JobRunner  extends EventHandler implements Runnable {
	private Props props;
	private Props outputProps;
	private ExecutableNode node;
	
	public JobRunner(ExecutableNode node, Props props) {
		this.props = props;
		this.node = node;
		this.node.setStatus(Status.WAITING);
	}
	
	public ExecutableNode getNode() {
		return node;
	}
	
	@Override
	public void run() {
		if (node.getStatus() == Status.KILLED) {
			this.fireEventListeners(Event.create(this, Type.JOB_KILLED));
			return;
		}
		
		this.fireEventListeners(Event.create(this, Type.JOB_STARTED));
		node.setStatus(Status.RUNNING);
		
		// Run Job
		boolean succeeded = true;
		
		if (succeeded) {
			node.setStatus(Status.SUCCEEDED);
			this.fireEventListeners(Event.create(this, Type.JOB_SUCCEEDED));
		}
		else {
			node.setStatus(Status.FAILED);
			this.fireEventListeners(Event.create(this, Type.JOB_FAILED));
		}
	}

	public void cancel() {
		// Cancel code here
		
		node.setStatus(Status.KILLED);
	}
	
	public Status getStatus() {
		return node.getStatus();
	}
	
	public Props getOutputProps() {
		return outputProps;
	}
}
