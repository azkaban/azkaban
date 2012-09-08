package azkaban.executor;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.event.EventListener;
import azkaban.utils.ExecutableFlowLoader;
import azkaban.utils.Props;

/**
 * Execution manager for the server side execution.
 *
 */
public class FlowRunnerManager {
	private static Logger logger = Logger.getLogger(FlowRunnerManager.class);
	private File basePath;
	
	private static final int DEFAULT_NUM_EXECUTING_FLOWS = 30;
	private ConcurrentHashMap<String, FlowRunner> runningFlows = new ConcurrentHashMap<String, FlowRunner>();
	private LinkedBlockingQueue<FlowRunner> queue = new LinkedBlockingQueue<FlowRunner>();
	private int numThreads = DEFAULT_NUM_EXECUTING_FLOWS;

	private ExecutorService executorService;
	private SubmitterThread submitterThread;
	private FlowRunnerEventListener eventListener;
	
	public FlowRunnerManager(Props props) {
		basePath = new File(props.getString("execution.directory"));
		numThreads = props.getInt("executor.flow.threads", DEFAULT_NUM_EXECUTING_FLOWS);
		executorService = Executors.newFixedThreadPool(numThreads);
		eventListener = new FlowRunnerEventListener(this);
		
		submitterThread = new SubmitterThread(queue);
		submitterThread.start();
	}
	
	public void submitFlow(String id, String path) throws ExecutorManagerException {
		// Load file and submit
		logger.info("Flow " + id + " submitted with path " + path);
		
		File dir = new File(path);
		ExecutableFlow flow = ExecutableFlowLoader.loadExecutableFlowFromDir(dir);
		flow.setExecutionPath(path);

		FlowRunner runner = new FlowRunner(flow);
		runningFlows.put(id, runner);
		runner.addListener(eventListener);
		executorService.submit(runner);
	}
	
	public void cancelFlow(String id, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(id);
		if (runner != null) {
			runner.cancel(user);
		}
	}
	
	public void pauseFlow(String id, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(id);
		if (runner != null) {
			runner.pause(user);
		}
	}
	
	public void resumeFlow(String id, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(id);
		if (runner != null) {
			runner.resume(user);
		}
	}
	
	public FlowRunner getFlowRunner(String id) {
		return runningFlows.get(id);
	}
	
	public ExecutableFlow getExecutableFlow(String id) {
		FlowRunner runner = runningFlows.get(id);
		if (runner == null) {
			return null;
		}
		
		return runner.getFlow();
	}
	
	private class SubmitterThread extends Thread {
		private BlockingQueue<FlowRunner> queue;
		private boolean shutdown = false;
		
		public SubmitterThread(BlockingQueue<FlowRunner> queue) {
			this.queue = queue;
		}
		
		public void shutdown() {
			shutdown = true;
			this.interrupt();
		}
		
		public void run() {
			while(!shutdown) {
				try {
					FlowRunner flowRunner = queue.take();
					executorService.submit(flowRunner);
				} 
				catch (InterruptedException e) {
					logger.info("Interrupted. Probably to shut down.");
				}
			}
		}
	}
	
	private class FlowRunnerEventListener implements EventListener {
		private FlowRunnerManager manager;
		
		public FlowRunnerEventListener(FlowRunnerManager manager) {
			this.manager = manager;
		}

		@Override
		public synchronized void handleEvent(Event event) {
			FlowRunner runner = (FlowRunner)event.getRunner();
			ExecutableFlow flow = runner.getFlow();
			
			System.out.println("Event " + flow.getExecutionId() + " " + flow.getFlowId() + " " + event.getType());
			if (event.getType() == Type.FLOW_FINISHED) {
				logger.info("Flow " + flow.getExecutionId() + " has finished.");
				runningFlows.remove(flow.getExecutionId());
			}
		}
	}
}
