package azkaban.execapp;

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

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import azkaban.execapp.event.Event;
import azkaban.execapp.event.Event.Type;
import azkaban.execapp.event.EventHandler;
import azkaban.execapp.event.EventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlow.FailureAction;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.FlowProps;
import azkaban.jobtype.JobtypeManager;
import azkaban.security.HadoopSecurityManager;
import azkaban.utils.Props;

public class FlowRunner extends EventHandler implements Runnable {
	private static final Layout DEFAULT_LAYOUT = new PatternLayout("%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");
	private int execId;

	private File execDir;

	private ExecutorService executorService;
	private ExecutorLoader executorLoader;

	private ExecutableFlow flow;
	private Thread currentThread;
	private int numThreads = 10;
	
	private Logger logger;
	private Layout loggerLayout = DEFAULT_LAYOUT;
	private Appender flowAppender;
	private File logFile;
	
	// Properties map
	private Map<String, Props> sharedProps = new HashMap<String, Props>();
	private Map<String, Props> jobOutputProps = new HashMap<String, Props>();
	
	private Props globalProps;
	private final JobtypeManager jobtypeManager;
	private final HadoopSecurityManager hadoopSecurityManager;
	
	private JobRunnerEventListener listener = new JobRunnerEventListener();
	private BlockingQueue<JobRunner> jobsToRun = new LinkedBlockingQueue<JobRunner>();
	private Map<String, JobRunner> runningJob = new ConcurrentHashMap<String, JobRunner>();
	private Map<String, JobRunner> allJobs = new ConcurrentHashMap<String, JobRunner>();
	private List<JobRunner> pausedJobsToRun = Collections.synchronizedList(new ArrayList<JobRunner>());
	
	private Object actionSyncObj = new Object();
	private boolean flowPaused = false;
	private boolean flowFailed = false;
	private boolean flowFinished = false;
	private boolean flowCancelled = false;
	
	public FlowRunner(ExecutableFlow flow, ExecutorLoader executorLoader, JobtypeManager jobtypeManager, HadoopSecurityManager hadoopSecurityManager) throws ExecutorManagerException {
		this.execId = flow.getExecutionId();
		this.flow = flow;
		this.executorLoader = executorLoader;
		this.executorService = Executors.newFixedThreadPool(numThreads);
		this.execDir = new File(flow.getExecutionPath());
		this.jobtypeManager = jobtypeManager;
		this.hadoopSecurityManager = hadoopSecurityManager;
	}

	public FlowRunner setGlobalProps(Props globalProps) {
		this.globalProps = globalProps;
		return this;
	}

	public File getExecutionDir() {
		return execDir;
	}
	
	@Override
	public void run() {
		try {
			int projectId = flow.getProjectId();
			int version = flow.getVersion();
			String flowId = flow.getFlowId();
			
			// Create execution dir
			createLogger(flowId);
			logger.info("Running execid:" + execId + " flow:" + flowId + " project:" + projectId + " version:" + version);
			
			// The current thread is used for interrupting blocks
			currentThread = Thread.currentThread();
			currentThread.setName("FlowRunner-exec-" + flow.getExecutionId());

			flow.setStartTime(System.currentTimeMillis());
			
			logger.info("Creating active reference");
			if (!executorLoader.updateExecutableReference(execId, System.currentTimeMillis())) {
				throw new ExecutorManagerException("The executor reference doesn't exist. May have been killed prematurely.");
			}
			logger.info("Updating initial flow directory.");
			updateFlow();

			logger.info("Fetching job and shared properties.");
			loadAllProperties();
			logger.info("Queuing initial jobs.");
			queueStartingJobs();

			this.fireEventListeners(Event.create(this, Type.FLOW_STARTED));
			runFlow();
		} catch (Throwable t) {
			if (logger != null) {
				logger.error("An error has occurred during the running of the flow. Quiting.", t);
			}
			flow.setStatus(Status.FAILED);
		}
		finally {
			closeLogger();
			flow.setEndTime(System.currentTimeMillis());
			updateFlow();
			this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
		}
	}
	
	private void updateFlow() {
		updateFlow(System.currentTimeMillis());
	}
	
	private synchronized void updateFlow(long time) {
		try {
			flow.setUpdateTime(time);
			executorLoader.updateExecutableFlow(flow);
		} catch (ExecutorManagerException e) {
			logger.error("Error updating flow.", e);
		}
	}
	
	private void createLogger(String flowId) {
		// Create logger
		String loggerName = execId + "." + flowId;
		logger = Logger.getLogger(loggerName);

		// Create file appender
		String logName = "_flow." + loggerName + ".log";
		logFile = new File(execDir, logName);
		String absolutePath = logFile.getAbsolutePath();

		flowAppender = null;
		try {
			flowAppender = new FileAppender(loggerLayout, absolutePath, false);
			logger.addAppender(flowAppender);
		} catch (IOException e) {
			logger.error("Could not open log file in " + execDir, e);
		}
	}
	
	private void closeLogger() {
		if (logger != null) {
			logger.removeAppender(flowAppender);
			flowAppender.close();
			
			try {
				executorLoader.uploadLogFile(execId, "", logFile);
			} catch (ExecutorManagerException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void loadAllProperties() throws IOException {
		// First load all the properties
		for (FlowProps fprops : flow.getFlowProps()) {
			String source = fprops.getSource();
			File propsPath = new File(execDir, source);
			Props props = new Props(null, propsPath);
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
			else {
				String source = fprops.getSource();
				Props props = sharedProps.get(source);
				props.setParent(globalProps);
			}
		}
	}
	
	/**
	 * Main method that executes the jobs.
	 * 
	 * @throws Exception
	 */
	private void runFlow() throws Exception {
		logger.info("Starting flows");
		flow.setStatus(Status.RUNNING);
		updateFlow();
		while (!flowFinished) {
			JobRunner runner = null;
			try {
				runner = jobsToRun.poll(5, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				logger.info("FlowRunner thread has been interrupted.");
				continue;
			}
			
			if(runner == null) continue;
			
			try {
				synchronized(actionSyncObj) {
					ExecutableNode node = runner.getNode();
					if (flowPaused) {
						logger.info("Job Paused " + node.getJobId());
						node.setStatus(Status.PAUSED);
						pausedJobsToRun.add(runner);
					}
					else {
						runningJob.put(node.getJobId(), runner);
						allJobs.put(node.getJobId(), runner);
						executorService.submit(runner);
						logger.info("Job Started " + node.getJobId());
					}
				}
			} catch (RejectedExecutionException e) {
				logger.error(e);
			}
		}
		
		logger.info("Finishing up flow. Awaiting Termination");
		executorService.shutdown();
		
		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
		};
		
		switch(flow.getStatus()) {
		case FAILED_FINISHING:
			logger.info("Setting flow status to Failed.");
			flow.setStatus(Status.FAILED);
		case FAILED:
		case KILLED:
			break;
		default:
			flow.setStatus(Status.SUCCEEDED);
		}
	}
	
	private void queueStartingJobs() {
		for (String startNode : flow.getStartNodes()) {
			ExecutableNode node = flow.getExecutableNode(startNode);
			JobRunner jobRunner = createJobRunner(node, null);
			jobsToRun.add(jobRunner);
		}
	}
	
	private JobRunner createJobRunner(ExecutableNode node, Props previousOutput) {
		String source = node.getJobPropsSource();
		String propsSource = node.getPropsSource();

		// If no properties are set, we just set the global properties.
		Props parentProps = propsSource == null ? globalProps : sharedProps.get(propsSource);

		// Set up overrides
		@SuppressWarnings("unchecked")
		Props flowProps = new Props(null, flow.getFlowParameters()); 
		
		if (flowProps.size() > 0) {
			flowProps.setParent(parentProps);
			parentProps = flowProps;
		}
		
		// We add the previous job output and put into this props.
		if (previousOutput != null) {
			Props earliestParent = previousOutput.getEarliestAncestor();
			earliestParent.setParent(parentProps);

			parentProps = previousOutput;
		}
		
		// Load job file.
		File path = new File(execDir, source);
		Props prop = null;
		try {
			prop = new Props(null, path);
			prop.setParent(parentProps);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Error loading job file " + source + " for job " + node.getJobId());
		}
		
		JobRunner jobRunner = new JobRunner(node, prop, path.getParentFile(), executorLoader, jobtypeManager, hadoopSecurityManager);
		jobRunner.addListener(listener);

		return jobRunner;
	}
	
	public void pause(String user) {
		synchronized(actionSyncObj) {
			if (flow.getStatus() == Status.RUNNING || flow.getStatus() == Status.PREPARING) {
				logger.info("Flow paused by " + user);
				flowPaused = true;
				flow.setStatus(Status.PAUSED);
				
				updateFlow();
			}
		}
	}
	
	public void resume(String user) {
		synchronized(actionSyncObj) {
			if (!flowPaused) {
				logger.info("Cannot resume flow that isn't paused");
			}
			else {
				logger.info("Flow resumed by " + user);
				flowPaused = false;
				if (!flowCancelled) {
					flow.setStatus(Status.RUNNING);
				}

				for (JobRunner runner: pausedJobsToRun) {
					ExecutableNode node = runner.getNode();
					if (flowCancelled) {
						logger.info("Resumed flow is cancelled. Job killed " + node.getJobId());
						node.setStatus(Status.KILLED);
					}
					else {
						node.setStatus(Status.QUEUED);
					}
					
					jobsToRun.add(runner);
				}
				updateFlow();
			}
		}
	}
	
	public void cancel(String user) {
		synchronized(actionSyncObj) {
			flowPaused = false;
			flowCancelled = true;
			
			for (JobRunner runner: pausedJobsToRun) {
				ExecutableNode node = runner.getNode();
				logger.info("Resumed flow is cancelled. Job killed " + node.getJobId());
				node.setStatus(Status.KILLED);

				jobsToRun.add(runner);
			}
			
			for (JobRunner runner : runningJob.values()) {
				runner.cancel();
			}
			
			if (flow.getStatus() != Status.FAILED && flow.getStatus() != Status.FAILED_FINISHING) {
				flow.setStatus(Status.KILLED);
			}

			updateFlow();
			interrupt();
		}
	}
	
	private void interrupt() {
		currentThread.interrupt();
	}
	
	private Status getImpliedStatus(ExecutableNode node) {
		switch(node.getStatus()) {
		case FAILED:
		case KILLED:
		case SKIPPED:
		case SUCCEEDED:
		case QUEUED:
		case RUNNING:
			return null;
		default:
			break;
		}
		
		boolean shouldKill = false;
		for (String dependency : node.getInNodes()) {
			ExecutableNode dependencyNode = flow.getExecutableNode(dependency);
			
			Status depStatus = dependencyNode.getStatus();
			switch (depStatus) {
			case FAILED:
			case KILLED:
				shouldKill = true;
			case SKIPPED:
			case SUCCEEDED:
				continue;
			default:
				// Return null means it's not ready to run.
				return null;
			}
		}
		
		if (shouldKill || flowCancelled || (flowFailed && flow.getFailureAction() != FailureAction.FINISH_ALL_POSSIBLE)) {
			return Status.KILLED;
		}
		
		// If it's disabled but ready to run, we want to make sure it continues being disabled.
		if (node.getStatus() == Status.DISABLED) {
			return Status.DISABLED;
		}
		
		// All good to go, ready to run.
		return Status.READY;
	}
	
	private synchronized void queueNextJobs(ExecutableNode node) {
		for (String dependent : node.getOutNodes()) {
			ExecutableNode dependentNode = flow.getExecutableNode(dependent);
			
			Status nextStatus = getImpliedStatus(dependentNode);
			if (nextStatus == null) {
				// Not yet ready or not applicable
				continue;
			}

			dependentNode.setStatus(nextStatus);

			Props previousOutput = null;
			// Iterate the in nodes again and create the dependencies
			for (String dependency : dependentNode.getInNodes()) {
				Props output = jobOutputProps.get(dependency);
				if (output != null) {
					output = Props.clone(output);
					output.setParent(previousOutput);
					previousOutput = output;
				}
			}

			JobRunner runner = this.createJobRunner(dependentNode, previousOutput);
			synchronized(actionSyncObj) {
				if (flowPaused) {
					if (dependentNode.getStatus() != Status.DISABLED && dependentNode.getStatus() != Status.KILLED) {
						dependentNode.setStatus(Status.PAUSED);
					}
					pausedJobsToRun.add(runner);
					logger.info("Job Paused " + dependentNode.getJobId());
				}
				else {
					logger.info("Adding " + dependentNode.getJobId() + " to run queue.");
					if (dependentNode.getStatus() != Status.DISABLED && dependentNode.getStatus() != Status.KILLED) {
						dependentNode.setStatus(Status.QUEUED);
					}

					jobsToRun.add(runner);
				}
			}
		}
	}

	private class JobRunnerEventListener implements EventListener {
		public JobRunnerEventListener() {
		}

		@Override
		public synchronized void handleEvent(Event event) {
			JobRunner runner = (JobRunner)event.getRunner();
			if (event.getType() == Type.JOB_FINISHED) {
				ExecutableNode node = runner.getNode();

				logger.info("Job Finished " + node.getJobId());
				synchronized (actionSyncObj) {
					if (node.getStatus() == Status.FAILED) {
						// Setting failure
						flowFailed = true;
						if (!isFailedStatus(flow.getStatus())) {
							flow.setStatus(Status.FAILED_FINISHING);
							if (flow.getFailureAction() == FailureAction.CANCEL_ALL) {
								cancel("azkaban");
							}
						}
					}
					
					jobOutputProps.put(node.getJobId(), runner.getOutputProps());
					
					runningJob.remove(node.getJobId());
					queueNextJobs(node);
				}
				
				runner.cleanup();

				if (isFlowFinished()) {
					logger.info("Flow appears finished. Cleaning up.");
					flowFinished = true;
					interrupt();
				}
			}
			
			if (event.isShouldUpdate()) {
				ExecutableNode node = runner.getNode();
				updateFlow(node.getUpdateTime());
			}
		}
	}
	
	private boolean isFailedStatus(Status status) {
		switch (status) {
		case FAILED_FINISHING:
		case FAILED:
		case KILLED:
			return true;
		default:
			return false;
		}
	}
	
	private boolean isFlowFinished() {
		for (String end: flow.getEndNodes()) {
			ExecutableNode node = flow.getExecutableNode(end);
			switch(node.getStatus()) {
			case KILLED:
			case SKIPPED:
			case FAILED:
			case SUCCEEDED:
				continue;
			default:
				return false;
			}
		}
		
		return true;
	}
	
	public boolean isCancelled() {
		return flowCancelled;
	}
	
	public ExecutableFlow getExecutableFlow() {
		return flow;
	}
	
	public File getFlowLogFile() {
		return logFile;
	}
	
	public File getJobLogFile(String jobId) {
		JobRunner runner = allJobs.get(jobId);
		if (runner == null) {
			return null;
		}
		
		return runner.getLogFile();
	}
}