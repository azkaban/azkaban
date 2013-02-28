package azkaban.execapp.event;

import azkaban.execapp.FlowRunner;
import azkaban.execapp.JobRunner;
import azkaban.executor.ExecutableNode;

public class LocalFlowWatcher extends FlowWatcher {
	private LocalFlowWatcherListener watcherListener;
	private FlowRunner runner;
	
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
		runner.removeListener(watcherListener);
		runner = null;
		
		super.failAllWatches();
	}

	public class LocalFlowWatcherListener implements EventListener {
		@Override
		public void handleEvent(Event event) {
			if (event.getRunner() instanceof JobRunner) {
				JobRunner runner = (JobRunner)event.getRunner();
				ExecutableNode node = runner.getNode();
				
				handleJobFinished(node.getJobId(), node.getStatus());
			}
		}
	}
}
