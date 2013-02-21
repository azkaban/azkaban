package azkaban.execapp;

import azkaban.execapp.event.Event;
import azkaban.execapp.event.EventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;

/**
 * Class that watches and updates execution flows that are being listened to by
 * other executing flows.
 */
public class FlowWatcher {
	private FlowRunnerManager manager;
	private ExecutorLoader loader;
	private WatcherThread watcherThread;
	
	public FlowWatcher(FlowRunnerManager manager, ExecutorLoader loader) {
		this.manager = manager;
		this.loader = loader;
		
		watcherThread = new WatcherThread();
		watcherThread.start();
	}
	
	public void watchExecution(int execId, EventListener listener) throws ExecutorManagerException {
		watchExecution(execId, null, listener);
	}
	
	public void watchExecution(int execId, String jobId, EventListener listener) throws ExecutorManagerException {
		Watch watcher = new Watch(execId, jobId, listener);
		
		
	}
	
	public void unwatchAll(EventListener listener, int execId, String jobId) {
		
	}
	
	private class WatcherThread extends Thread {
		boolean isShutdown = false;
		
		public WatcherThread() {
			this.setName("Execution-watcher-Thread");
		}
		
		public void run() {
			while (!isShutdown) {
				
			}
		}
	}
	
	public class Watch {
		private final EventListener listener;

		private final int execId;
		private final String jobId;
		private long updateTime = -1;

		public Watch(int execId, EventListener listener) {
			this(execId, null, listener);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + execId;
			result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
			result = prime * result + ((listener == null) ? 0 : listener.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Watch other = (Watch) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (execId != other.execId)
				return false;
			if (jobId == null) {
				if (other.jobId != null)
					return false;
			} else if (!jobId.equals(other.jobId))
				return false;
			if (listener == null) {
				if (other.listener != null)
					return false;
			} else if (!listener.equals(other.listener))
				return false;
			return true;
		}

		public Watch(int execId, String jobId, EventListener listener) {
			this.execId = execId;
			this.listener = listener;
			this.jobId = jobId;
		}
		
		private void notifyIfUpdate(ExecutableFlow flow) {
			if (jobId == null) {
				if (flow.getUpdateTime() > updateTime) {
					listener.handleEvent(Event.create(flow, Event.Type.EXTERNAL_FLOW_UPDATED));

					updateTime = flow.getUpdateTime();
				}
			}
			else {
				ExecutableNode node = flow.getExecutableNode(jobId);

				if (node.getUpdateTime() > updateTime) {
					listener.handleEvent(Event.create(node, Event.Type.EXTERNAL_JOB_UPDATED));

					updateTime = node.getUpdateTime();
				}
			}
		}

		private FlowWatcher getOuterType() {
			return FlowWatcher.this;
		}
	}
}
