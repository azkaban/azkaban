package azkaban.executor;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import azkaban.executor.event.Event;
import azkaban.executor.event.EventListener;
import azkaban.utils.ExecutableFlowLoader;
import azkaban.utils.Props;

/**
 * Execution manager for the server side execution client.
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
		File dir = new File(path);
		ExecutableFlow flow = ExecutableFlowLoader.loadExecutableFlowFromDir(dir);
		
		FlowRunner runner = new FlowRunner(flow);
		runner.addListener(eventListener);
		executorService.submit(runner);
	}
	
	//
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
			
		}
	}
}
