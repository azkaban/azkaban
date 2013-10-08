/*
 * Copyright 2012 LinkedIn Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.execapp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import azkaban.execapp.event.Event;
import azkaban.execapp.event.Event.Type;
import azkaban.execapp.event.EventHandler;
import azkaban.execapp.event.EventListener;
import azkaban.execapp.event.FlowWatcher;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.FlowProps;
import azkaban.jobtype.JobTypeManager;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;

public class FlowRunner extends EventHandler implements Runnable {
	private static final Layout DEFAULT_LAYOUT = new PatternLayout("%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");
	// We check update every 5 minutes, just in case things get stuck. But for the most part, we'll be idling.
	private static final long CHECK_WAIT_MS = 5*60*1000;
	
	private Logger logger;
	private Layout loggerLayout = DEFAULT_LAYOUT;
	private Appender flowAppender;
	private File logFile;
	
	private ExecutorService executorService;
	private ExecutorLoader executorLoader;
	private ProjectLoader projectLoader;
	
	private int execId;
	private File execDir;
	private ExecutableFlow flow;
	private Thread flowRunnerThread;
	private int numJobThreads = 10;
	
	// Sync object for queuing
	private Object mainSyncObj = new Object();
	
	// Properties map
	private Map<String, Props> sharedProps = new HashMap<String, Props>();
	private Map<String, Props> jobOutputProps = new HashMap<String, Props>();
	
	private Props globalProps;
	private Props commonProps;
	private final JobTypeManager jobtypeManager;
	
	private JobRunnerEventListener listener = new JobRunnerEventListener();
	private Map<String, JobRunner> jobRunners = new ConcurrentHashMap<String, JobRunner>();
	private Map<String, JobRunner> activeJobRunners = new ConcurrentHashMap<String, JobRunner>();
	
	// Used for pipelining
	private Integer pipelineLevel = null;
	private Integer pipelineExecId = null;
	
	// Watches external flows for execution.
	private FlowWatcher watcher = null;

	private Set<String> proxyUsers = null;
	private boolean validateUserProxy;
	
	private String jobLogFileSize = "5MB";
	private int jobLogNumFiles = 4;
	
	private boolean flowPaused = false;
	private boolean flowFailed = false;
	private boolean flowFinished = false;
	private boolean flowCancelled = false;
	
	public FlowRunner(ExecutableFlow flow, ExecutorLoader executorLoader, ProjectLoader projectLoader, JobTypeManager jobtypeManager) throws ExecutorManagerException {
		this.execId = flow.getExecutionId();
		this.flow = flow;
		this.executorLoader = executorLoader;
		this.projectLoader = projectLoader;
		this.execDir = new File(flow.getExecutionPath());
		this.jobtypeManager = jobtypeManager;

		ExecutionOptions options = flow.getExecutionOptions();
		this.pipelineLevel = options.getPipelineLevel();
		this.pipelineExecId = options.getPipelineExecutionId();

		this.proxyUsers = flow.getProxyUsers();
	}

	public FlowRunner setFlowWatcher(FlowWatcher watcher) {
		this.watcher = watcher;
		return this;
	}
	
	public FlowRunner setGlobalProps(Props globalProps) {
		this.globalProps = globalProps;
		return this;
	}
	
	public FlowRunner setNumJobThreads(int jobs) {
		numJobThreads = jobs;
		return this;
	}
	
	public FlowRunner setJobLogSettings(String jobLogFileSize, int jobLogNumFiles) {
		this.jobLogFileSize = jobLogFileSize;
		this.jobLogNumFiles = jobLogNumFiles;
		
		return this;
	}
	
	public FlowRunner setValidateProxyUser(boolean validateUserProxy) {
		this.validateUserProxy = validateUserProxy;
		return this;
	}
	
	public File getExecutionDir() {
		return execDir;
	}
	
	public void run() {
		try {
			if (this.executorService == null) {
				this.executorService = Executors.newFixedThreadPool(numJobThreads);
			}
			setupFlowExecution();
			flow.setStartTime(System.currentTimeMillis());
			
			updateFlowReference();
			
			logger.info("Updating initial flow directory.");
			updateFlow();
			logger.info("Fetching job and shared properties.");
			loadAllProperties();

			this.fireEventListeners(Event.create(this, Type.FLOW_STARTED));
			runFlow();
		} catch (Throwable t) {
			if (logger != null) {
				logger.error("An error has occurred during the running of the flow. Quiting.", t);
			}
			flow.setStatus(Status.FAILED);
		}
		finally {
			if (watcher != null) {
				logger.info("Watcher is attached. Stopping watcher.");
				watcher.stopWatcher();
				logger.info("Watcher cancelled status is " + watcher.isWatchCancelled());
			}

			flow.setEndTime(System.currentTimeMillis());
			logger.info("Setting end time for flow " + execId + " to " + System.currentTimeMillis());
			closeLogger();
			
			updateFlow();
			this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
		}
	}
	
	private void setupFlowExecution() {
		int projectId = flow.getProjectId();
		int version = flow.getVersion();
		String flowId = flow.getFlowId();
		
		// Add a bunch of common azkaban properties
		commonProps = PropsUtils.addCommonFlowProperties(flow);
		
		// Create execution dir
		createLogger(flowId);
		
		if (this.watcher != null) {
			this.watcher.setLogger(logger);
		}
		
		logger.info("Running execid:" + execId + " flow:" + flowId + " project:" + projectId + " version:" + version);
		if (pipelineExecId != null) {
			logger.info("Running simulateously with " + pipelineExecId + ". Pipelining level " + pipelineLevel);
		}
		
		// The current thread is used for interrupting blocks
		flowRunnerThread = Thread.currentThread();
		flowRunnerThread.setName("FlowRunner-exec-" + flow.getExecutionId());

	}
	
	private void updateFlowReference() throws ExecutorManagerException {
		logger.info("Update active reference");
		if (!executorLoader.updateExecutableReference(execId, System.currentTimeMillis())) {
			throw new ExecutorManagerException("The executor reference doesn't exist. May have been killed prematurely.");
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
				executorLoader.uploadLogFile(execId, "", 0, logFile);
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
			synchronized(mainSyncObj) {
				if (flowPaused) {
					try {
						mainSyncObj.wait(CHECK_WAIT_MS);
					} catch (InterruptedException e) {
					}

					continue;
				}
				else {
					List<ExecutableNode> jobsReadyToRun = findReadyJobsToRun();
					
					if (!jobsReadyToRun.isEmpty() && !flowCancelled) {
						for (ExecutableNode node : jobsReadyToRun) {
							long currentTime = System.currentTimeMillis();
							
							// Queue a job only if it's ready to run.
							if (node.getStatus() == Status.READY) {
								// Collect output props from the job's dependencies.
								Props outputProps = collectOutputProps(node);
								node.setStatus(Status.QUEUED);
								JobRunner runner = createJobRunner(node, outputProps);
								logger.info("Submitting job " + node.getJobId() + " to run.");
								try {
									executorService.submit(runner);
									jobRunners.put(node.getJobId(), runner);
									activeJobRunners.put(node.getJobId(), runner);
								} catch (RejectedExecutionException e) {
									logger.error(e);
								};
								
							} // If killed, then auto complete and KILL
							else if (node.getStatus() == Status.KILLED) {
								logger.info("Killing " + node.getJobId() + " due to prior errors.");
								node.setStartTime(currentTime);
								node.setEndTime(currentTime);
								fireEventListeners(Event.create(this, Type.JOB_FINISHED, node));
							} // If disabled, then we auto skip
							else if (node.getStatus() == Status.DISABLED) {
								logger.info("Skipping disabled job " + node.getJobId() + ".");
								node.setStartTime(currentTime);
								node.setEndTime(currentTime);
								node.setStatus(Status.SKIPPED);
								fireEventListeners(Event.create(this, Type.JOB_FINISHED, node));
							}
						}
						
						updateFlow();
					}
					else {
						if (isFlowFinished() || flowCancelled ) {
							flowFinished = true;
							break;
						}
					
						try {
							mainSyncObj.wait(CHECK_WAIT_MS);
						} catch (InterruptedException e) {
						}
					}
				}
			}
		}
		
		if (flowCancelled) {
			try {
				logger.info("Flow was force cancelled cleaning up.");
				for(JobRunner activeRunner : activeJobRunners.values()) {
					activeRunner.cancel();
				}
				
				for (ExecutableNode node: flow.getExecutableNodes()) {
					if (Status.isStatusFinished(node.getStatus())) {
						continue;
					}
					else if (node.getStatus() == Status.DISABLED) {
						node.setStatus(Status.SKIPPED);
					}
					else {
						node.setStatus(Status.KILLED);
					}
					fireEventListeners(Event.create(this, Type.JOB_FINISHED, node));
				}
			} catch (Exception e) {
				logger.error(e);
			}
	
			updateFlow();
		}
		
		logger.info("Finishing up flow. Awaiting Termination");
		executorService.shutdown();
		
		synchronized(mainSyncObj) {
			switch(flow.getStatus()) {
			case FAILED_FINISHING:
				logger.info("Setting flow status to Failed.");
				flow.setStatus(Status.FAILED);
			case FAILED:
			case KILLED:
				logger.info("Flow is set to " + flow.getStatus().toString());
				break;
			default:
				flow.setStatus(Status.SUCCEEDED);
				logger.info("Flow is set to " + flow.getStatus().toString());
			}
		}
	}
	
	private List<ExecutableNode> findReadyJobsToRun() {
		ArrayList<ExecutableNode> jobsToRun = new ArrayList<ExecutableNode>();
		for (ExecutableNode node : flow.getExecutableNodes()) {
			if (Status.isStatusFinished(node.getStatus())) {
				continue;
			}
			else {
				// Check the dependencies to see if execution conditions are met,
				// and what the status should be set to.
				Status impliedStatus = getImpliedStatus(node);
				if (getImpliedStatus(node) != null) {
					node.setStatus(impliedStatus);
					jobsToRun.add(node);
				}
			}
		}
		
		return jobsToRun;
	}

	private boolean isFlowFinished() {
		if (!activeJobRunners.isEmpty()) {
			return false;
		}
		
		for (String end: flow.getEndNodes()) {
			ExecutableNode node = flow.getExecutableNode(end);
			if (!Status.isStatusFinished(node.getStatus()) ) {
				return false;
			}
		}
		
		return true;
	}
	
	private Props collectOutputProps(ExecutableNode node) {
		Props previousOutput = null;
		// Iterate the in nodes again and create the dependencies
		for (String dependency : node.getInNodes()) {
			Props output = jobOutputProps.get(dependency);
			if (output != null) {
				output = Props.clone(output);
				output.setParent(previousOutput);
				previousOutput = output;
			}
		}
		
		return previousOutput;
	}
	
	private JobRunner createJobRunner(ExecutableNode node, Props previousOutput) {
		String source = node.getJobPropsSource();
		String propsSource = node.getPropsSource();

		// If no properties are set, we just set the global properties.
		Props parentProps = propsSource == null ? globalProps : sharedProps.get(propsSource);

		// Set up overrides
		ExecutionOptions options = flow.getExecutionOptions();
		@SuppressWarnings("unchecked")
		Props flowProps = new Props(null, options.getFlowParameters()); 
		flowProps.putAll(commonProps);
		flowProps.setParent(parentProps);
		parentProps = flowProps;

		// We add the previous job output and put into this props.
		if (previousOutput != null) {
			Props earliestParent = previousOutput.getEarliestAncestor();
			earliestParent.setParent(parentProps);

			parentProps = previousOutput;
		}
		
		// Load job file.
		File path = new File(execDir, source);
		Props prop = null;
		
		// load the override props if any
		try {
			prop = projectLoader.fetchProjectProperty(flow.getProjectId(), flow.getVersion(), node.getJobId()+".jor");
		}
		catch(ProjectManagerException e) {
			e.printStackTrace();
			logger.error("Error loading job override property for job " + node.getJobId());
		}
		if(prop == null) {
			// if no override prop, load the original one on disk
			try {
				prop = new Props(null, path);				
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("Error loading job file " + source + " for job " + node.getJobId());
			}
		}
		// setting this fake source as this will be used to determine the location of log files.
		prop.setSource(path.getPath());
		prop.setParent(parentProps);
		
		JobRunner jobRunner = new JobRunner(node, prop, path.getParentFile(), executorLoader, jobtypeManager);
		if (watcher != null) {
			jobRunner.setPipeline(watcher, pipelineLevel);
		}
		if (validateUserProxy) {
			jobRunner.setValidatedProxyUsers(proxyUsers);
		}
		
		jobRunner.setDelayStart(node.getDelayedExecution());
		jobRunner.setLogSettings(logger, jobLogFileSize, jobLogNumFiles);
		jobRunner.addListener(listener);

		return jobRunner;
	}
	
	public void pause(String user) {
		synchronized(mainSyncObj) {
			if (!flowFinished) {
				logger.info("Flow paused by " + user);
				flowPaused = true;
				flow.setStatus(Status.PAUSED);
				
				updateFlow();
			}
			else {
				logger.info("Cannot pause finished flow. Called by user " + user);
			}
		}
		
		interrupt();
	}
	
	public void resume(String user) {
		synchronized(mainSyncObj) {
			if (!flowPaused) {
				logger.info("Cannot resume flow that isn't paused");
			}
			else {
				logger.info("Flow resumed by " + user);
				flowPaused = false;
				if (flowFailed) {
					flow.setStatus(Status.FAILED_FINISHING);
				}
				else if (flowCancelled) {
					flow.setStatus(Status.KILLED);
				}
				else {
					flow.setStatus(Status.RUNNING);
				}
				
				updateFlow();
			}
		}
	}
	
	public void cancel(String user) {
		synchronized(mainSyncObj) {
			logger.info("Flow cancelled by " + user);
			cancel();
			updateFlow();
		}
		interrupt();
	}
	
	private void cancel() {
		synchronized(mainSyncObj) {
			logger.info("Cancel has been called on flow " + execId);
			flowPaused = false;
			flowCancelled = true;
			
			if (watcher != null) {
				logger.info("Watcher is attached. Stopping watcher.");
				watcher.stopWatcher();
				logger.info("Watcher cancelled status is " + watcher.isWatchCancelled());
			}
			
			logger.info("Cancelling " + activeJobRunners.size() + " jobs.");
			for (JobRunner runner : activeJobRunners.values()) {
				runner.cancel();
			}
			
			if (flow.getStatus() != Status.FAILED && flow.getStatus() != Status.FAILED_FINISHING) {
				logger.info("Setting flow status to " + Status.KILLED.toString());
				flow.setStatus(Status.KILLED);
			}
		}
	}
	
	public void retryFailures(String user) {
		synchronized(mainSyncObj) {
			logger.info("Retrying failures invoked by " + user);
			ArrayList<String> failures = new ArrayList<String>();
			for (ExecutableNode node: flow.getExecutableNodes()) {
				if (node.getStatus() == Status.FAILED) {
					failures.add(node.getJobId());
				}
				else if (node.getStatus() == Status.KILLED) {
					node.setStartTime(-1);
					node.setEndTime(-1);
					node.setStatus(Status.READY);
				}
			}
			
			retryJobs(failures, user);
		}
	}
	
	public void retryJobs(List<String> jobIds, String user) {
		synchronized(mainSyncObj) {
			for (String jobId: jobIds) {
				ExecutableNode node = flow.getExecutableNode(jobId);
				if (node == null) {
					logger.error("Job " + jobId + " doesn't exist in execution " + flow.getExecutionId() + ". Cannot retry.");
					continue;
				}
				
				if (Status.isStatusFinished(node.getStatus())) {
					// Resets the status and increments the attempt number
					node.resetForRetry();
					reEnableDependents(node);
					logger.info("Re-enabling job " + node.getJobId() + " attempt " + node.getAttempt());
				}
				else {
					logger.error("Cannot retry job " + jobId + " since it hasn't run yet. User " + user);
					continue;
				}
			}
			
			boolean isFailureFound = false;
			for (ExecutableNode node: flow.getExecutableNodes()) {
				Status nodeStatus = node.getStatus();
				if (nodeStatus == Status.FAILED || nodeStatus == Status.KILLED) {
					isFailureFound = true;
					break;
				}
			}
			
			if (!isFailureFound) {
				flow.setStatus(Status.RUNNING);
				flow.setUpdateTime(System.currentTimeMillis());
				flowFailed = false;
			}
			
			updateFlow();
			interrupt();
		}
	}
	
	private void reEnableDependents(ExecutableNode node) {
		for(String dependent: node.getOutNodes()) {
			ExecutableNode dependentNode = flow.getExecutableNode(dependent);
			
			if (dependentNode.getStatus() == Status.KILLED) {
				dependentNode.setStatus(Status.READY);
				dependentNode.setUpdateTime(System.currentTimeMillis());
				reEnableDependents(dependentNode);
			}
			else if (dependentNode.getStatus() == Status.SKIPPED) {
				dependentNode.setStatus(Status.DISABLED);
				dependentNode.setUpdateTime(System.currentTimeMillis());
				reEnableDependents(dependentNode);
			}
		}
	}
	
	private void interrupt() {
		flowRunnerThread.interrupt();
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
			case RUNNING:
			case QUEUED:
			case DISABLED:
				return null;
			default:
				// Return null means it's not ready to run.
				return null;
			}
		}
		
		ExecutionOptions options = flow.getExecutionOptions();
		if (shouldKill || flowCancelled || (flowFailed && options.getFailureAction() != FailureAction.FINISH_ALL_POSSIBLE)) {
			return Status.KILLED;
		}
		
		// If it's disabled but ready to run, we want to make sure it continues being disabled.
		if (node.getStatus() == Status.DISABLED) {
			return Status.DISABLED;
		}
		
		// All good to go, ready to run.
		return Status.READY;
	}
	
	private class JobRunnerEventListener implements EventListener {
		public JobRunnerEventListener() {
		}

		@Override
		public synchronized void handleEvent(Event event) {
			JobRunner runner = (JobRunner)event.getRunner();
			
			if (event.getType() == Type.JOB_STATUS_CHANGED) {
				updateFlow();
			}
			else if (event.getType() == Type.JOB_FINISHED) {
				synchronized(mainSyncObj) {
					ExecutableNode node = runner.getNode();
					activeJobRunners.remove(node.getJobId());
					
					logger.info("Job Finished " + node.getJobId() + " with status " + node.getStatus());
					if (runner.getOutputProps() != null) {
						logger.info("Job " + node.getJobId() + " had output props.");
						jobOutputProps.put(node.getJobId(), runner.getOutputProps());
					}
					
					updateFlow();
					
					if (node.getStatus() == Status.FAILED) {
						// Retry failure if conditions are met.
						if (!runner.isCancelled() && runner.getRetries() > node.getAttempt()) {
							logger.info("Job " + node.getJobId() + " will be retried. Attempt " + node.getAttempt() + " of " + runner.getRetries());
							node.setDelayedExecution(runner.getRetryBackoff());
							node.resetForRetry();
						}
						else {
							if (!runner.isCancelled() && runner.getRetries() > 0) {
					
								logger.info("Job " + node.getJobId() + " has run out of retry attempts");
								// Setting delayed execution to 0 in case this is manually re-tried.
								node.setDelayedExecution(0);
							}

							flowFailed = true;
							
							ExecutionOptions options = flow.getExecutionOptions();
							// The KILLED status occurs when cancel is invoked. We want to keep this
							// status even in failure conditions.
							if (flow.getStatus() != Status.KILLED && flow.getStatus() != Status.FAILED) {
								flow.setStatus(Status.FAILED_FINISHING);
								if (options.getFailureAction() == FailureAction.CANCEL_ALL && !flowCancelled) {
									logger.info("Flow failed. Failure option is Cancel All. Stopping execution.");
									cancel();
								}
							}
						}
					}
					
					interrupt();
	
					fireEventListeners(event);
				}
			}
		}
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
	
	public File getJobLogFile(String jobId, int attempt) {
		ExecutableNode node = flow.getExecutableNode(jobId);
		File path = new File(execDir, node.getJobPropsSource());
		
		String logFileName = JobRunner.createLogFileName(execId, jobId, attempt);
		File logFile = new File(path.getParentFile(), logFileName);
		
		if (!logFile.exists()) {
			return null;
		}
		
		return logFile;
	}
	
	public File getJobMetaDataFile(String jobId, int attempt) {
		ExecutableNode node = flow.getExecutableNode(jobId);
		File path = new File(execDir, node.getJobPropsSource());
		
		String metaDataFileName = JobRunner.createMetaDataFileName(execId, jobId, attempt);
		File metaDataFile = new File(path.getParentFile(), metaDataFileName);
		
		if (!metaDataFile.exists()) {
			return null;
		}
		
		return metaDataFile;
	}
	
	public boolean isRunnerThreadAlive() {
		if (flowRunnerThread != null) {
			return flowRunnerThread.isAlive();
		}
		return false;
	}
	
	public boolean isThreadPoolShutdown() {
		return executorService.isShutdown();
	}
	
	public int getNumRunningJobs() {
		return activeJobRunners.size();
	}
}
