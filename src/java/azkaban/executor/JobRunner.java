package azkaban.executor;

import java.io.File;

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
	private File workingDir;
	
	public JobRunner(ExecutableNode node, Props props, File workingDir) {
		this.props = props;
		this.node = node;
		this.node.setStatus(Status.WAITING);
		this.workingDir = workingDir;
	}
	
	public ExecutableNode getNode() {
		return node;
	}
	
	@Override
	public void run() {
		if (node.getStatus() == Status.DISABLED) {
			this.fireEventListeners(Event.create(this, Type.JOB_SUCCEEDED));
			return;
		}
		else if (node.getStatus() == Status.KILLED) {
			this.fireEventListeners(Event.create(this, Type.JOB_KILLED));
			return;
		}
		node.setStartTime(System.currentTimeMillis());
		node.setStatus(Status.RUNNING);
		this.fireEventListeners(Event.create(this, Type.JOB_STARTED));
		
		
		// Run Job
		boolean succeeded = true;
		
		node.setEndTime(System.currentTimeMillis());
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
