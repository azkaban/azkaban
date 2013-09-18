/*
 * Copyright 2013 LinkedIn Corp
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
import java.util.Collections;
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
import azkaban.executor.ExecutableFlowBase;
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
	private final ExecutableFlow flow;
	private Thread flowRunnerThread;
	private int numJobThreads = 10;
	private ExecutionOptions.FailureAction failureAction;
	
	// Sync object for queuing
	private Object mainSyncObj = new Object();
	
	// Properties map
	private Map<String, Props> sharedProps = new HashMap<String, Props>();
//	private Map<String, Props> jobOutputProps = new HashMap<String, Props>();
	
	private Props globalProps;
//	private Props commonProps;
	private final JobTypeManager jobtypeManager;
	
	private JobRunnerEventListener listener = new JobRunnerEventListener();
	private Set<JobRunner> activeJobRunners = Collections.newSetFromMap(new ConcurrentHashMap<JobRunner, Boolean>());
	
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
		this(flow, executorLoader, projectLoader, jobtypeManager, null);
	}

	public FlowRunner(ExecutableFlow flow, ExecutorLoader executorLoader, ProjectLoader projectLoader, JobTypeManager jobtypeManager, ExecutorService executorService) throws ExecutorManagerException {
		this.execId = flow.getExecutionId();
		this.flow = flow;
		this.executorLoader = executorLoader;
		this.projectLoader = projectLoader;
		this.execDir = new File(flow.getExecutionPath());
		this.jobtypeManager = jobtypeManager;

		ExecutionOptions options = flow.getExecutionOptions();
		this.pipelineLevel = options.getPipelineLevel();
		this.pipelineExecId = options.getPipelineExecutionId();
		this.failureAction = options.getFailureAction();
		this.proxyUsers = flow.getProxyUsers();
		this.executorService = executorService;
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
	
	@SuppressWarnings("unchecked")
	private void setupFlowExecution() {
		int projectId = flow.getProjectId();
		int version = flow.getVersion();
		String flowId = flow.getFlowId();
		
		// Add a bunch of common azkaban properties
		Props commonFlowProps = PropsUtils.addCommonFlowProperties(this.globalProps, flow);
		
		if (flow.getJobSource() != null) {
			String source = flow.getJobSource();
			Props flowProps = sharedProps.get(source);
			flowProps.setParent(commonFlowProps);
			commonFlowProps = flowProps;
		}
		
		// If there are flow overrides, we apply them now.
		Map<String,String> flowParam = flow.getExecutionOptions().getFlowParameters();
		if (flowParam != null && !flowParam.isEmpty()) {
			commonFlowProps = new Props(commonFlowProps, flowParam);
		}
		flow.setInputProps(commonFlowProps);
		
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
					if (!progressGraph(flow)) {
						if (flow.isFlowFinished() || flowCancelled ) {
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
				for(JobRunner activeRunner : activeJobRunners) {
					activeRunner.cancel();
				}

				flow.killNode(System.currentTimeMillis());
			} catch (Exception e) {
				logger.error(e);
			}
	
			updateFlow();
		}
		
		logger.info("Finishing up flow. Awaiting Termination");
		executorService.shutdown();
		
		synchronized(mainSyncObj) {
			finalizeFlow(flow);
		}
	}
	
	private boolean progressGraph(ExecutableFlowBase flow) throws IOException {
		List<ExecutableNode> jobsReadyToRun = flow.findNextJobsToRun();

		if (!jobsReadyToRun.isEmpty()) {
			long currentTime = System.currentTimeMillis();
			for (ExecutableNode node: jobsReadyToRun) {
				Status nextStatus = getImpliedStatus(node);
				
				// If the flow has seen previous failures and the flow has been cancelled, than 
				if (nextStatus == Status.KILLED) {
					logger.info("Killing " + node.getId() + " due to prior errors.");
					node.killNode(currentTime);
					fireEventListeners(Event.create(this, Type.JOB_FINISHED, node));
				}
				else if (nextStatus == Status.DISABLED) {
					logger.info("Skipping disabled job " + node.getId() + ".");
					node.skipNode(currentTime);
					fireEventListeners(Event.create(this, Type.JOB_FINISHED, node));
				}
				else {
					runExecutableNode(node);
				}
			}
			
			updateFlow();
			return true;
		}
		
		return false;
	}
	
	private void finalizeFlow(ExecutableFlowBase flow) {
		String id = flow == this.flow ? "" : flow.getPrintableId() + " ";
		
		switch(flow.getStatus()) {
		case FAILED_FINISHING:
			logger.info("Setting flow " + id + "status to Failed.");
			flow.setStatus(Status.FAILED);
		case FAILED:
		case KILLED:
		case FAILED_SUCCEEDED:
			logger.info("Flow " + id + "is set to " + flow.getStatus().toString());
			break;
		default:
			flow.setStatus(Status.SUCCEEDED);
			logger.info("Flow " + id + "is set to " + flow.getStatus().toString());
		}
	}
	
	@SuppressWarnings("unchecked")
	private void prepareJobProperties(ExecutableNode node) throws IOException {
		Props props = null;
		// The following is the hiearchical ordering of dependency resolution
		// 1. Parent Flow Properties
		ExecutableFlowBase parentFlow = node.getParentFlow();
		if (parentFlow != null) {
			props = parentFlow.getInputProps();
		}
		
		// 2. Shared Properties
		String sharedProps = node.getPropsSource();
		if (sharedProps != null) {
			Props shared = this.sharedProps.get(sharedProps);
			if (shared != null) {
				// Clone because we may clobber
				shared = Props.clone(shared);
				shared.setEarliestAncestor(props);
				props = shared;
			}
		}

		// 3. Flow Override properties
		Map<String,String> flowParam = flow.getExecutionOptions().getFlowParameters();
		if (flowParam != null && !flowParam.isEmpty()) {
			props = new Props(props, flowParam);
		}
		
		// 4. Output Properties
		Props outputProps = collectOutputProps(node);
		if (outputProps != null) {
			outputProps.setEarliestAncestor(props);
			props = outputProps;
		}
		
		// 5. The job source
		Props jobSource = loadJobProps(node);
		if (jobSource != null) {
			jobSource.setParent(props);
			props = jobSource;
		}
		
		node.setInputProps(props);
	}
	
	private Props loadJobProps(ExecutableNode node) throws IOException {
		Props props = null;
		String source = node.getJobSource();
		if (source == null) {
			return null;
		}
		
		// load the override props if any
		try {
			props = projectLoader.fetchProjectProperty(flow.getProjectId(), flow.getVersion(), node.getId()+".jor");
		}
		catch(ProjectManagerException e) {
			e.printStackTrace();
			logger.error("Error loading job override property for job " + node.getId());
		}
		
		File path = new File(execDir, source);
		if(props == null) {
			// if no override prop, load the original one on disk
			try {
				props = new Props(null, path);				
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("Error loading job file " + source + " for job " + node.getId());
			}
		}
		// setting this fake source as this will be used to determine the location of log files.
		props.setSource(path.getPath());
		return props;
	}
	
	private void runExecutableNode(ExecutableNode node) throws IOException {
		// Collect output props from the job's dependencies.
		prepareJobProperties(node);
		
		if (node instanceof ExecutableFlowBase) {
			node.setStatus(Status.RUNNING);
			node.setStartTime(System.currentTimeMillis());
			
			logger.info("Starting subflow " + node.getPrintableId() + ".");
		}
		else {
			node.setStatus(Status.QUEUED);
			JobRunner runner = createJobRunner(node);
			logger.info("Submitting job " + node.getPrintableId() + " to run.");
			try {
				executorService.submit(runner);
				activeJobRunners.add(runner);
			} catch (RejectedExecutionException e) {
				logger.error(e);
			};
		}
	}
	
	/**
	 * Determines what the state of the next node should be.
	 * 
	 * @param node
	 * @return
	 */
	public Status getImpliedStatus(ExecutableNode node) {
		if (flowFailed && failureAction == ExecutionOptions.FailureAction.FINISH_CURRENTLY_RUNNING) {
			return Status.KILLED;
		}
		else if (node.getStatus() == Status.DISABLED) {
			return Status.DISABLED;
		}
		
		ExecutableFlowBase flow = node.getParentFlow();
		boolean shouldKill = false;
		for (String dependency: node.getInNodes()) {
			ExecutableNode dependencyNode = flow.getExecutableNode(dependency);
			Status depStatus = dependencyNode.getStatus();
			
			switch (depStatus) {
			case FAILED:
			case KILLED:
				shouldKill = true;
			case SKIPPED:
			case SUCCEEDED:
			case FAILED_SUCCEEDED:
				continue;
			default:
				// Should never come here.
				return null;
			}
		}

		if (shouldKill) {
			return Status.KILLED;
		}
		
		// If it's disabled but ready to run, we want to make sure it continues being disabled.
		if (node.getStatus() == Status.DISABLED) {
			return Status.DISABLED;
		}
		
		// All good to go, ready to run.
		return Status.READY;
	}
	
	private Props collectOutputProps(ExecutableNode node) {
		Props previousOutput = null;
		// Iterate the in nodes again and create the dependencies
		for (String dependency : node.getInNodes()) {
			Props output = node.getParentFlow().getExecutableNode(dependency).getOutputProps();
			if (output != null) {
				output = Props.clone(output);
				output.setParent(previousOutput);
				previousOutput = output;
			}
		}
		
		return previousOutput;
	}
	
	private JobRunner createJobRunner(ExecutableNode node) {
		// Load job file.
		File path = new File(execDir, node.getJobSource());
		
		JobRunner jobRunner = new JobRunner(node, path.getParentFile(), executorLoader, jobtypeManager);
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
			for (JobRunner runner : activeJobRunners) {
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
			retryFailures(flow);
			
			flow.setStatus(Status.RUNNING);
			flow.setUpdateTime(System.currentTimeMillis());
			flowFailed = false;
			
			updateFlow();
			interrupt();
		}
	}
	
	private void retryFailures(ExecutableFlowBase flow) {
		for (ExecutableNode node: flow.getExecutableNodes()) {
			if (node instanceof ExecutableFlowBase) {
				if (node.getStatus() == Status.FAILED || node.getStatus() == Status.FAILED_FINISHING || node.getStatus() == Status.KILLED) {
					retryFailures((ExecutableFlowBase)node);
				}
			}
			
			if (node.getStatus() == Status.FAILED) {
				node.resetForRetry();
				logger.info("Re-enabling job " + node.getPrintableId() + " attempt " + node.getAttempt());
				reEnableDependents(node);
			}
			else if (node.getStatus() == Status.KILLED) {
				node.setStartTime(-1);
				node.setEndTime(-1);
				node.setStatus(Status.READY);
			}
			else if (node.getStatus() == Status.FAILED_FINISHING) {
				node.setStartTime(-1);
				node.setEndTime(-1);
				node.setStatus(Status.READY);
			}
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

	private class JobRunnerEventListener implements EventListener {
		public JobRunnerEventListener() {
		}
		// TODO: HANDLE subflow execution
		@Override
		public synchronized void handleEvent(Event event) {
			JobRunner runner = (JobRunner)event.getRunner();
			
			if (event.getType() == Type.JOB_STATUS_CHANGED) {
				updateFlow();
			}
			else if (event.getType() == Type.JOB_FINISHED) {
				synchronized(mainSyncObj) {
					ExecutableNode node = runner.getNode();
					activeJobRunners.remove(node.getId());
					
					String id = node.getPrintableId(":");
					logger.info("Job Finished " + id + " with status " + node.getStatus());
					if (node.getOutputProps() != null) {
						logger.info("Job " + id + " had output props.");
					}

					if (node.getStatus() == Status.FAILED) {
						// Retry failure if conditions are met.
						if (!runner.isCancelled() && runner.getRetries() > node.getAttempt()) {
							logger.info("Job " + id + " will be retried. Attempt " + node.getAttempt() + " of " + runner.getRetries());
							node.setDelayedExecution(runner.getRetryBackoff());
							node.resetForRetry();
						}
						else {
							if (!runner.isCancelled() && runner.getRetries() > 0) {
								logger.info("Job " + id + " has run out of retry attempts");
								// Setting delayed execution to 0 in case this is manually re-tried.
								node.setDelayedExecution(0);
							}

							flowFailed = true;
							
							ExecutionOptions options = flow.getExecutionOptions();
							// The KILLED status occurs when cancel is invoked. We want to keep this
							// status even in failure conditions.
							if (flow.getStatus() != Status.KILLED && flow.getStatus() != Status.FAILED) {
								propagateStatus(node.getParentFlow(), Status.FAILED_FINISHING);

								if (options.getFailureAction() == FailureAction.CANCEL_ALL && !flowCancelled) {
									logger.info("Flow failed. Failure option is Cancel All. Stopping execution.");
									cancel();
								}
							}
						}
					}
					finalizeFlowIfFinished(node.getParentFlow());
					updateFlow();
					interrupt();
	
					fireEventListeners(event);
				}
			}
		}
		
		private void propagateStatus(ExecutableFlowBase base, Status status) {
			base.setStatus(status);
			if (base.getParentFlow() != null) {
				propagateStatus(base.getParentFlow(), status);
			}
		}

		private void finalizeFlowIfFinished(ExecutableFlowBase base) {
			// We let main thread finalize the main flow. 
			if (base == flow) {
				return;
			}
			
			if (base.isFlowFinished()) {
				Props previousOutput = null;
				for(String end: base.getEndNodes()) {
					ExecutableNode node = base.getExecutableNode(end);
		
					Props output = node.getOutputProps();
					if (output != null) {
						output = Props.clone(output);
						output.setParent(previousOutput);
						previousOutput = output;
					}
				}
				base.setOutputProps(previousOutput);
				finalizeFlow(base);
				
				if (base.getParentFlow() != null) {
					finalizeFlowIfFinished(base.getParentFlow());
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
		File path = new File(execDir, node.getJobSource());
		
		String logFileName = JobRunner.createLogFileName(execId, jobId, attempt);
		File logFile = new File(path.getParentFile(), logFileName);
		
		if (!logFile.exists()) {
			return null;
		}
		
		return logFile;
	}
	
	public File getJobMetaDataFile(String jobId, int attempt) {
		ExecutableNode node = flow.getExecutableNode(jobId);
		File path = new File(execDir, node.getJobSource());
		
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