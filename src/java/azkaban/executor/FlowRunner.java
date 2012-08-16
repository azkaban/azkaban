package azkaban.executor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import azkaban.executor.ExecutableFlow.ExecutableNode;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.event.EventHandler;
import azkaban.executor.event.EventListener;
import azkaban.flow.FlowProps;
import azkaban.utils.Props;

public class FlowRunner extends EventHandler implements Runnable {
	public static final int NUM_CONCURRENT_THREADS = 10;

	private ExecutableFlow flow;
	private ExecutorService executorService;
	private BlockingQueue<JobRunner> jobsToRun = new LinkedBlockingQueue<JobRunner>();
	private int numThreads = NUM_CONCURRENT_THREADS;
	private boolean cancelled = true;
	private boolean done = false;
	
	private Map<String, JobRunner> jobRunnersMap;
	private JobRunnerEventListener listener;
	private Map<String, Props> sharedProps = new HashMap<String, Props>();
	private Map<String, Props> outputProps = new HashMap<String, Props>();
	private File basePath;
	
	public enum FailedFlowOptions {
		FINISH_RUNNING_JOBS,
		COMPLETE_ALL_DEPENDENCIES,
		CANCEL_ALL
	}
	
	private FailedFlowOptions failedOptions = FailedFlowOptions.FINISH_RUNNING_JOBS;
	
	public FlowRunner(ExecutableFlow flow) {
		this.flow = flow;
		this.basePath = new File(flow.getExecutionPath());
		this.executorService = Executors.newFixedThreadPool(numThreads);
		this.jobRunnersMap = new HashMap<String, JobRunner>();
		this.listener = new JobRunnerEventListener(this);
	}
	
	public ExecutableFlow getFlow() {
		return flow;
	}
	
	public void cancel() {
		done = true;
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
	
	@Override
	public void run() {
		this.fireEventListeners(Event.create(this, Type.FLOW_STARTED));
		
		// Load all shared props
		try {
			loadAllProperties(flow);
		}
		catch (IOException e) {
			flow.setStatus(Status.FAILED);
			System.err.println("Failed due to " + e.getMessage());
			this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
			return;
		}

		// Set up starting nodes
		try {
			for (String startNode: flow.getStartNodes()) {
				ExecutableNode node = flow.getExecutableNode(startNode);
				JobRunner jobRunner = createJobRunner(node, null);
				jobsToRun.add(jobRunner);
			}
		} catch (IOException e) {
			System.err.println("Failed due to " + e.getMessage());
			flow.setStatus(Status.FAILED);
			jobsToRun.clear();
			this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
			return;
		}
		
		while(!done) {
			JobRunner runner = null;
			try {
				runner = jobsToRun.take();
			} catch (InterruptedException e) {
			}
			
			if (!done && runner != null) {
				executorService.submit(runner);
			}
		}
		
		this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
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
		
		JobRunner jobRunner = new JobRunner(node, jobProps);
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
					dependencyNode.getStatus() != Status.IGNORED) {
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
		if (failedOptions == FailedFlowOptions.FINISH_RUNNING_JOBS) {
			done = true;
		}
		else if (failedOptions == FailedFlowOptions.CANCEL_ALL) {
			this.cancel();
		}
		else if (failedOptions == FailedFlowOptions.COMPLETE_ALL_DEPENDENCIES) {
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
			String jobID = runner.getNode().getId();
			System.out.println("Event " + jobID + " " + event.getType().toString());

			if (event.getType() == Type.JOB_SUCCEEDED) {
				// Continue adding items.
				Props props = runner.getOutputProps();
				outputProps.put(jobID, props);
				
				flowRunner.handleSucceededJob(runner.getNode());
			}
			else if (event.getType() == Type.JOB_FAILED) {
				flowRunner.handleFailedJob(runner.getNode());
			}
		}
	}
}