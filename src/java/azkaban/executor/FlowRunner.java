package azkaban.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.event.EventHandler;

public class FlowRunner extends EventHandler implements Runnable {
	public static final int NUM_CONCURRENT_THREADS = 10;

	private ExecutableFlow flow;
	private FlowRunnerManager manager;
	private ExecutorService executorService;
	private int numThreads = NUM_CONCURRENT_THREADS;
	
	public FlowRunner(ExecutableFlow flow) {
		this.flow = flow;
		this.manager = manager;
		this.executorService = Executors.newFixedThreadPool(numThreads);
	}
	
	@Override
	public void run() {
		this.fireEventListeners(new Event(this, Type.FLOW_STARTED, null));
		
		
		
		this.fireEventListeners(new Event(this, Type.FLOW_FINISHED, null));
	}
}