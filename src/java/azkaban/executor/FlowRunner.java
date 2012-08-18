package azkaban.executor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import azkaban.executor.ExecutableFlow.ExecutableNode;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.event.EventHandler;
import azkaban.executor.event.EventListener;
import azkaban.flow.FlowProps;
import azkaban.utils.ExecutableFlowLoader;
import azkaban.utils.Props;

public class FlowRunner extends EventHandler implements Runnable {
	private static final Layout DEFAULT_LAYOUT = new PatternLayout( "%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");

	public static final int NUM_CONCURRENT_THREADS = 10;

	private ExecutableFlow flow;
	private ExecutorService executorService;
	private BlockingQueue<JobRunner> jobsToRun = new LinkedBlockingQueue<JobRunner>();
	private int numThreads = NUM_CONCURRENT_THREADS;
	private boolean cancelled = true;
	
	private Map<String, JobRunner> jobRunnersMap;
	private JobRunnerEventListener listener;
	private Map<String, Props> sharedProps = new HashMap<String, Props>();
	private Map<String, Props> outputProps = new HashMap<String, Props>();
	private File basePath;
	private AtomicInteger commitCount = new AtomicInteger(0);
	private HashSet<String> finalNodes = new HashSet<String>();

	private Logger logger;
	private Layout loggerLayout = DEFAULT_LAYOUT;
	private Appender flowAppender;
	
	public enum FailedFlowOptions {
		FINISH_RUNNING_JOBS,
		KILL_ALL
	}
	
	private FailedFlowOptions failedOptions = FailedFlowOptions.FINISH_RUNNING_JOBS;
	
	public FlowRunner(ExecutableFlow flow) {
		this.flow = flow;
		this.basePath = new File(flow.getExecutionPath());
		this.executorService = Executors.newFixedThreadPool(numThreads);
		this.jobRunnersMap = new HashMap<String, JobRunner>();
		this.listener = new JobRunnerEventListener(this);
		
		createLogger();
	}
	
	public ExecutableFlow getFlow() {
		return flow;
	}
	
	private void createLogger() {
		// Create logger
		String loggerName = System.currentTimeMillis() + "." + flow.getExecutionId();
		logger = Logger.getLogger(loggerName);

		// Create file appender
		String logName = "_flow." + flow.getExecutionId() + ".log";
		File logFile = new File(this.basePath, logName);
		String absolutePath = logFile.getAbsolutePath();

		flowAppender = null;
		try {
			flowAppender = new FileAppender(loggerLayout, absolutePath, false);
			logger.addAppender(flowAppender);
		} catch (IOException e) {
			logger.error("Could not open log file in " + basePath, e);
		}
	}
	
	private void closeLogger() {
		logger.removeAppender(flowAppender);
		flowAppender.close();
	}
	
	public void cancel() {
		logger.info("Cancel Invoked");
		finalNodes.clear();
		cancelled = true;
		
		executorService.shutdownNow();
		
		// Loop through job runners
		for (JobRunner runner: jobRunnersMap.values()) {
			if (runner.getStatus() == Status.WAITING || runner.getStatus() == Status.RUNNING) {
				runner.cancel();
			}
		}
		
		this.notify();
	}
	
	public boolean isCancelled() {
		return cancelled;
	}
	
	private synchronized void commitFlow() {
		int count = commitCount.getAndIncrement();

		try {
			ExecutableFlowLoader.writeExecutableFlowFile(this.basePath, flow, count);
		} catch (ExecutorManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		flow.setStatus(Status.RUNNING);
		flow.setStartTime(System.currentTimeMillis());
		logger.info("Starting Flow");
		this.fireEventListeners(Event.create(this, Type.FLOW_STARTED));
		
		// Load all shared props
		try {
			logger.info("Loading all shared properties");
			loadAllProperties(flow);
		}
		catch (IOException e) {
			flow.setStatus(Status.FAILED);
			logger.error("Property loading failed due to " + e.getMessage());
			logger.error("Exiting Prematurely.");
			this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
			return;
		}

		// Set up starting nodes
		try {
			logger.info("Queuing starting jobs.");
			for (String startNode: flow.getStartNodes()) {
				ExecutableNode node = flow.getExecutableNode(startNode);
				JobRunner jobRunner = createJobRunner(node, null);
				jobsToRun.add(jobRunner);
			}
		} catch (IOException e) {
			logger.error("Starting job queueing failed due to " + e.getMessage());
			flow.setStatus(Status.FAILED);
			jobsToRun.clear();
			logger.error("Exiting Prematurely.");
			this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
			return;
		}
		
		// When this is empty, we will stop.
		finalNodes.addAll(flow.getEndNodes());
		
		// Main loop
		while(!finalNodes.isEmpty()) {
			JobRunner runner = null;
			try {
				runner = jobsToRun.take();
			} catch (InterruptedException e) {
			}
			
			if (!finalNodes.isEmpty() && runner != null) {
				try {
					ExecutableNode node = runner.getNode();
					node.setStatus(Status.WAITING);
					executorService.submit(runner);
					logger.info("Job Started " + node.getId());
					finalNodes.remove(node.getId());
				} catch (RejectedExecutionException e) {
					// Should reject if I shutdown executor.
					break;
				}
			}
			
			// Just to make sure we back off on the flooding.
			synchronized (this) {
				try {
					wait(10);
				} catch (InterruptedException e) {
					
				}
			}
		}
		
		logger.info("Finishing up flow. Awaiting Termination");
		executorService.shutdown();
		while (executorService.isTerminated()) {
			try {
				executorService.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		flow.setEndTime(System.currentTimeMillis());
		if (flow.getStatus() == Status.RUNNING) {
			logger.info("Flow finished successfully in " + (flow.getEndTime() - flow.getStartTime()) + " ms.");
			flow.setStatus(Status.SUCCEEDED);
		}
		else {
			logger.info("Flow finished with failures in " + (flow.getEndTime() - flow.getStartTime()) + " ms.");
			flow.setStatus(Status.FAILED);
		}

		commitFlow();
		this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
		closeLogger();
	}
	
	private JobRunner createJobRunner(ExecutableNode node, Props previousOutput) throws IOException {
		String source = node.getJobPropsSource();
		String propsSource = node.getPropsSource();

		Props parentProps = propsSource == null ? null : sharedProps.get(propsSource);
		
		// We add the previous job output and put into this props.
		if (previousOutput != null) {
			Props earliestParent = previousOutput.getEarliestAncestor();
			earliestParent.setParent(parentProps);
			
			parentProps = earliestParent;
		}
		
		File propsFile = new File(basePath, source);
		Props jobProps = new Props(parentProps, propsFile);
		
		JobRunner jobRunner = new JobRunner(node, jobProps, basePath);
		jobRunner.addListener(listener);
		jobRunnersMap.put(node.getId(), jobRunner);
		
		return jobRunner;
	}
	
	private void loadAllProperties(ExecutableFlow flow) throws IOException {
		// First load all the properties
		for (FlowProps fprops: flow.getFlowProps()) {
			String source = fprops.getSource();
			File propsFile = new File(basePath, source);
			
			Props props = new Props(null, propsFile);
			sharedProps.put(source, props);
		}

		// Resolve parents
		for (FlowProps fprops: flow.getFlowProps()) {
			if (fprops.getInheritedSource() != null) {
				String source = fprops.getSource();
				String inherit = fprops.getInheritedSource();
				
				Props props = sharedProps.get(source);
				Props inherits = sharedProps.get(inherit);
				
				props.setParent(inherits);
			}
		}
	}
	
	private void handleSucceededJob(ExecutableNode node) {
		for(String dependent: node.getOutNodes()) {
			ExecutableNode dependentNode = flow.getExecutableNode(dependent);
			
			// Check all dependencies
			boolean ready = true;
			for (String dependency: dependentNode.getInNodes()) {
				ExecutableNode dependencyNode = flow.getExecutableNode(dependency); 
				if (dependencyNode.getStatus() != Status.SUCCEEDED &&
					dependencyNode.getStatus() != Status.DISABLED) {
					ready = false;
					break;
				}
			}
			
			if (ready) {
				Props previousOutput = null;
				// Iterate the in nodes again and create the dependencies
				for (String dependency: node.getInNodes()) {
					Props output = outputProps.get(dependency);
					if (output != null) {
						output = Props.clone(output);
						
						output.setParent(previousOutput);
						previousOutput = output;
					}
				}
				
				JobRunner runner = null;
				try {
					runner = this.createJobRunner(dependentNode, previousOutput);
				} catch (IOException e) {
					System.err.println("Failed due to " + e.getMessage());
					dependentNode.setStatus(Status.FAILED);
					handleFailedJob(dependentNode);
					return;
				}
				
				jobsToRun.add(runner);
			}
		}
	}
	
	private void handleFailedJob(ExecutableNode node) {
		System.err.println("Job " + node.getId() + " failed.");
		this.fireEventListeners(Event.create(this, Type.FLOW_FAILED_FINISHING));
		
		switch (failedOptions) {
			// We finish running current jobs and then fail. Do not accept new jobs.
			case FINISH_RUNNING_JOBS:
				finalNodes.clear();
				executorService.shutdown();
				this.notify();
			break;
			// We kill all running jobs and fail immediately
			case KILL_ALL:
				this.cancel();
			break;
		}

	}
	
	private class JobRunnerEventListener implements EventListener {
		private FlowRunner flowRunner;
		
		public JobRunnerEventListener(FlowRunner flowRunner) {
			this.flowRunner = flowRunner;
		}

		@Override
		public synchronized void handleEvent(Event event) {
			JobRunner runner = (JobRunner)event.getRunner();
			ExecutableNode node = runner.getNode();
			String jobID = node.getId();
			System.out.println("Event " + jobID + " " + event.getType().toString());

			// On Job success, we add the output props and then set up the next run.
			if (event.getType() == Type.JOB_SUCCEEDED) {
				logger.info("Job Succeeded " + jobID + " in " + (node.getEndTime() - node.getStartTime()) + " ms");
				Props props = runner.getOutputProps();
				outputProps.put(jobID, props);
				flowRunner.handleSucceededJob(runner.getNode());
			}
			else if (event.getType() == Type.JOB_FAILED) {
				logger.info("Job Failed " + jobID + " in " + (node.getEndTime() - node.getStartTime()) + " ms");
				logger.info(jobID + " FAILED");
				flowRunner.handleFailedJob(runner.getNode());
			}
			
			flowRunner.commitFlow();
		}
	}
}