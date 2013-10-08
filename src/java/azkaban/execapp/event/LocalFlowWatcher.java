package azkaban.execapp.event;


import azkaban.execapp.FlowRunner;
import azkaban.execapp.JobRunner;
import azkaban.execapp.event.Event.Type;
import azkaban.executor.ExecutableNode;

public class LocalFlowWatcher extends FlowWatcher {
	private LocalFlowWatcherListener watcherListener;
	private FlowRunner runner;
	private boolean isShutdown = false;
	
	public LocalFlowWatcher(FlowRunner runner) {
		super(runner.getExecutableFlow().getExecutionId());
		super.setFlow(runner.getExecutableFlow());
		
		watcherListener = new LocalFlowWatcherListener();
		this.runner = runner;
		runner.addListener(watcherListener);
	}

	@Override
	public void stopWatcher() {
		// Just freeing stuff
		if(isShutdown) {
			return;
		}
		
		isShutdown = true;
		runner.removeListener(watcherListener);
		runner = null;
		
		getLogger().info("Stopping watcher, and unblocking pipeline");
		super.failAllWatches();
	}

	public class LocalFlowWatcherListener implements EventListener {
		@Override
		public void handleEvent(Event event) {
			if (event.getType() == Type.JOB_FINISHED) {
				if (event.getRunner() instanceof FlowRunner) {
					// The flow runner will finish a job without it running
					Object data = event.getData();
					if (data instanceof ExecutableNode) {
						ExecutableNode node = (ExecutableNode)data;
						handleJobStatusChange(node.getNestedId(), node.getStatus());
					}
				}
				else if (event.getRunner() instanceof JobRunner) {
					// A job runner is finished
					JobRunner runner = (JobRunner)event.getRunner();
					ExecutableNode node = runner.getNode();
					
					handleJobStatusChange(node.getNestedId(), node.getStatus());
				}
			}
			else if (event.getType() == Type.FLOW_FINISHED) {
				stopWatcher();
			}
		}
	}
}
