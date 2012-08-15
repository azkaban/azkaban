package azkaban.executor;

public class FlowRunner implements Runnable {
	private ExecutableFlow flow;
	private FlowRunnerManager manager;
	
	public FlowRunner(ExecutableFlow flow) {
		this.flow = flow;
		this.manager = manager;
	}
	
	@Override
	public void run() {
		
	}
}