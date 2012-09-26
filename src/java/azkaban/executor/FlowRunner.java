package azkaban.executor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
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
	private static final Layout DEFAULT_LAYOUT = new PatternLayout(
			"%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");

	public static final int NUM_CONCURRENT_THREADS = 10;

	private ExecutableFlow flow;
	private ExecutorService executorService;
	private BlockingQueue<JobRunner> jobsToRun = new LinkedBlockingQueue<JobRunner>();
	private List<JobRunner> pausedJobsToRun = Collections
			.synchronizedList(new ArrayList<JobRunner>());
	private int numThreads = NUM_CONCURRENT_THREADS;
	private boolean cancelled = false;
	private boolean paused = false;

	private Map<String, JobRunner> runningJobs;
	private JobRunnerEventListener listener;
	private Map<String, Props> sharedProps = new HashMap<String, Props>();
	private Map<String, Props> outputProps = new HashMap<String, Props>();
	private File basePath;
	private AtomicInteger commitCount = new AtomicInteger(0);

	private Logger logger;
	private Layout loggerLayout = DEFAULT_LAYOUT;
	private Appender flowAppender;

	private Thread currentThread;

	public enum FailedFlowOptions {
		FINISH_RUNNING_JOBS, KILL_ALL
	}

	private FailedFlowOptions failedOptions = FailedFlowOptions.FINISH_RUNNING_JOBS;

	public FlowRunner(ExecutableFlow flow) {
		this.flow = flow;
		this.basePath = new File(flow.getExecutionPath());
		this.executorService = Executors.newFixedThreadPool(numThreads);
		this.runningJobs = new ConcurrentHashMap<String, JobRunner>();
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

	public synchronized void cancel(String user) {
		logger.info("Cancel called by " + user);
		cancelled = true;

		executorService.shutdownNow();

		if (pausedJobsToRun.size() > 0) {
			logger.info("Cancelling... Clearing paused jobs queue of size "
					+ pausedJobsToRun.size());
			pausedJobsToRun.clear();
		}

		// Loop through job runners
		for (JobRunner runner : runningJobs.values()) {
			if (runner.getStatus() == Status.WAITING
					|| runner.getStatus() == Status.RUNNING
					|| runner.getStatus() == Status.PAUSED) {
				logger.info("Cancelling... Killing job "
						+ runner.getNode().getId() + " with status "
						+ runner.getStatus());
				runner.cancel();
			}
		}

		logger.info("Flow cancelled.");
		if (flow.getStatus() != Status.FAILED) {
			flow.setStatus(Status.KILLED);
		}
		flow.setEndTime(System.currentTimeMillis());
		this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
	}

	public synchronized void pause(String user) {
		if (flow.getStatus() == Status.RUNNING
				|| flow.getStatus() == Status.WAITING) {
			logger.info("Flow paused by " + user);
			paused = true;
			flow.setStatus(Status.PAUSED);
		}
	}

	public synchronized void resume(String user) {
		if (isCancelled()) {
			logger.info("Cannot resume cancelled flow.");
			return;
		}

		if (flow.getStatus() == Status.PAUSED) {
			paused = false;
			logger.info("Flow resumed by " + user);
			jobsToRun.addAll(pausedJobsToRun);
			flow.setStatus(Status.RUNNING);
		}
	}

	public boolean isCancelled() {
		return cancelled;
	}

	private synchronized void commitFlow() {
		int count = commitCount.getAndIncrement();

		try {
			ExecutableFlowLoader.writeExecutableFlowFile(this.basePath, flow,
					count);
		} catch (ExecutorManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		currentThread = Thread.currentThread();

		flow.setStatus(Status.RUNNING);
		flow.setStartTime(System.currentTimeMillis());
		logger.info("Starting Flow");
		this.fireEventListeners(Event.create(this, Type.FLOW_STARTED));

		// Load all shared props
		try {
			logger.info("Loading all shared properties");
			loadAllProperties(flow);
		} catch (IOException e) {
			flow.setStatus(Status.FAILED);
			logger.error("Property loading failed due to " + e.getMessage());
			logger.error("Exiting Prematurely.");
			this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
			return;
		}

		// Set up starting nodes
		try {
			logger.info("Queuing starting jobs.");
			for (String startNode : flow.getStartNodes()) {
				ExecutableNode node = flow.getExecutableNode(startNode);
				JobRunner jobRunner = createJobRunner(node, null);
				jobsToRun.add(jobRunner);
				runningJobs.put(startNode, jobRunner);
			}
		} catch (IOException e) {
			logger.error("Starting job queueing failed due to "
					+ e.getMessage());
			flow.setStatus(Status.FAILED);
			jobsToRun.clear();
			runningJobs.clear();
			logger.error("Exiting Prematurely.");
			this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
			return;
		}

		// Main loop
		while (!runningJobs.isEmpty()) {
			JobRunner runner = null;
			try {
				runner = jobsToRun.poll(5, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				logger.info("FlowRunner thread has been interrupted.");
				if (runningJobs.isEmpty()) {
					break;
				} else {
					continue;
				}
			}

			if (runner != null) {
				try {
					ExecutableNode node = runner.getNode();
					node.setStatus(Status.WAITING);
					executorService.submit(runner);
					logger.info("Job Started " + node.getId());
				} catch (RejectedExecutionException e) {
					// Should reject if I shutdown executor.
					break;
				}

				// Just to make sure we back off so we don't flood.
				synchronized (this) {
					try {
						wait(5);
					} catch (InterruptedException e) {

					}
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
			logger.info("Flow finished successfully in "
					+ (flow.getEndTime() - flow.getStartTime()) + " ms.");
			flow.setStatus(Status.SUCCEEDED);
		} else if (flow.getStatus() == Status.KILLED) {
			logger.info("Flow was killed in "
					+ (flow.getEndTime() - flow.getStartTime()) + " ms.");
			flow.setStatus(Status.KILLED);
		} else {
			logger.info("Flow finished with failures in "
					+ (flow.getEndTime() - flow.getStartTime()) + " ms.");
			flow.setStatus(Status.FAILED);
		}

		commitFlow();
		this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
		closeLogger();
	}

	private JobRunner createJobRunner(ExecutableNode node, Props previousOutput)
			throws IOException {
		String source = node.getJobPropsSource();
		String propsSource = node.getPropsSource();

		Props parentProps = propsSource == null ? null : sharedProps
				.get(propsSource);

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

		return jobRunner;
	}

	private void loadAllProperties(ExecutableFlow flow) throws IOException {
		// First load all the properties
		for (FlowProps fprops : flow.getFlowProps()) {
			String source = fprops.getSource();
			File propsFile = new File(basePath, source);

			Props props = new Props(null, propsFile);
			sharedProps.put(source, props);
		}

		// Resolve parents
		for (FlowProps fprops : flow.getFlowProps()) {
			if (fprops.getInheritedSource() != null) {
				String source = fprops.getSource();
				String inherit = fprops.getInheritedSource();

				Props props = sharedProps.get(source);
				Props inherits = sharedProps.get(inherit);

				props.setParent(inherits);
			}
		}
	}

	private void interrupt() {
		currentThread.interrupt();
	}

	private void handleSucceededJob(ExecutableNode node) {
		if (this.isCancelled()) {
			return;
		}

		for (String dependent : node.getOutNodes()) {
			ExecutableNode dependentNode = flow.getExecutableNode(dependent);

			// Check all dependencies
			boolean ready = true;
			for (String dependency : dependentNode.getInNodes()) {
				ExecutableNode dependencyNode = flow
						.getExecutableNode(dependency);
				if (dependencyNode.getStatus() != Status.SUCCEEDED
						&& dependencyNode.getStatus() != Status.DISABLED) {
					ready = false;
					break;
				}
			}

			if (ready) {
				Props previousOutput = null;
				// Iterate the in nodes again and create the dependencies
				for (String dependency : node.getInNodes()) {
					Props output = outputProps.get(dependency);
					if (output != null) {
						output = Props.clone(output);

						output.setParent(previousOutput);
						previousOutput = output;
					}
				}

				JobRunner runner = null;
				try {
					runner = this
							.createJobRunner(dependentNode, previousOutput);
				} catch (IOException e) {
					logger.error("JobRunner creation failed due to "
							+ e.getMessage());
					dependentNode.setStatus(Status.FAILED);
					handleFailedJob(dependentNode);
					return;
				}

				runningJobs.put(dependentNode.getId(), runner);
				if (paused) {
					dependentNode.setStatus(Status.PAUSED);
					pausedJobsToRun.add(runner);
					logger.info("Flow is paused so adding "
							+ dependentNode.getId() + " to paused list.");
				} else {
					logger.info("Adding " + dependentNode.getId() + " to run queue.");
					jobsToRun.add(runner);
				}
			}
		}

		runningJobs.remove(node.getId());
	}

	private void handleFailedJob(ExecutableNode node) {
		System.err.println("Job " + node.getId() + " failed.");
		this.fireEventListeners(Event.create(this, Type.FLOW_FAILED_FINISHING));

		switch (failedOptions) {
		// We finish running current jobs and then fail. Do not accept new jobs.
		case FINISH_RUNNING_JOBS:
			runningJobs.clear();
			executorService.shutdown();
			break;
		// We kill all running jobs and fail immediately
		case KILL_ALL:
			this.cancel("azkaban");
			break;
		}

		runningJobs.remove(node.getId());
	}

	private class JobRunnerEventListener implements EventListener {
		private FlowRunner flowRunner;

		public JobRunnerEventListener(FlowRunner flowRunner) {
			this.flowRunner = flowRunner;
		}

		@Override
		public synchronized void handleEvent(Event event) {
			JobRunner runner = (JobRunner) event.getRunner();
			ExecutableNode node = runner.getNode();
			String jobID = node.getId();
			System.out.println("Event " + jobID + " "
					+ event.getType().toString());

			// On Job success, we add the output props and then set up the next
			// run.
			if (event.getType() == Type.JOB_SUCCEEDED) {
				logger.info("Job Succeeded " + jobID + " in "
						+ (node.getEndTime() - node.getStartTime()) + " ms");
				Props props = runner.getOutputProps();
				outputProps.put(jobID, props);
				flowRunner.handleSucceededJob(runner.getNode());
			} else if (event.getType() == Type.JOB_FAILED) {
				logger.info("Job Failed " + jobID + " in "
						+ (node.getEndTime() - node.getStartTime()) + " ms");
				logger.info(jobID + " FAILED");
				flowRunner.handleFailedJob(runner.getNode());
			}

			flowRunner.commitFlow();
			if (runningJobs.isEmpty()) {
				System.out.println("There are no more running jobs.");
				flowRunner.interrupt();
			}
		}
	}
}