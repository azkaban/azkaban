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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;

import java.util.HashSet;
import java.util.Set;

import java.util.Arrays;
import java.util.Collections;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import azkaban.execapp.event.BlockingStatus;
import azkaban.execapp.event.Event;
import azkaban.execapp.event.Event.Type;
import azkaban.execapp.event.EventHandler;
import azkaban.execapp.event.FlowWatcher;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobtype.JobTypeManager;
import azkaban.jobtype.JobTypeManagerException;

import azkaban.utils.Props;

public class JobRunner extends EventHandler implements Runnable {
	private static final Layout DEFAULT_LAYOUT = new PatternLayout("%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");
	
	private ExecutorLoader loader;
	private Props props;
	private Props outputProps;
	private ExecutableNode node;
	private File workingDir;

	private Logger logger = null;
	private Layout loggerLayout = DEFAULT_LAYOUT;
	private Logger flowLogger = null;
	
	private Appender jobAppender;
	private File logFile;
	
	private Job job;
	private int executionId = -1;
	
	private static final Object logCreatorLock = new Object();
	private Object syncObject = new Object();
	
	private final JobTypeManager jobtypeManager;

	// Used by the job to watch and block against another flow
	private Integer pipelineLevel = null;
	private FlowWatcher watcher = null;
	private Set<String> pipelineJobs = new HashSet<String>();

	private Set<String> proxyUsers = null;

	private String jobLogChunkSize;
	private int jobLogBackupIndex;

	private long delayStartMs = 0;
	private boolean cancelled = false;
	private BlockingStatus currentBlockStatus = null;
	
	public JobRunner(ExecutableNode node, Props props, File workingDir, ExecutorLoader loader, JobTypeManager jobtypeManager) {
		this.props = props;
		this.node = node;
		this.workingDir = workingDir;
		this.executionId = node.getExecutionId();
		this.loader = loader;
		this.jobtypeManager = jobtypeManager;
	}
	
	public void setValidatedProxyUsers(Set<String> proxyUsers) {
		this.proxyUsers = proxyUsers;
	}
	
	public void setLogSettings(Logger flowLogger, String logFileChuckSize, int numLogBackup ) {
		this.flowLogger = flowLogger;
		this.jobLogChunkSize = logFileChuckSize;
		this.jobLogBackupIndex = numLogBackup;
	}
	
	public Props getProps() {
		return props;
	}
	
	public void setPipeline(FlowWatcher watcher, int pipelineLevel) {
		this.watcher = watcher;
		this.pipelineLevel = pipelineLevel;

		if (this.pipelineLevel == 1) {
			pipelineJobs.add(node.getJobId());
		}
		else if (this.pipelineLevel == 2) {
			pipelineJobs.add(node.getJobId());
			pipelineJobs.addAll(node.getOutNodes());
		}
	}
	
	public void setDelayStart(long delayMS) {
		delayStartMs = delayMS;
	}
	
	public long getDelayStart() {
		return delayStartMs;
	}
	
	public ExecutableNode getNode() {
		return node;
	}
	
	public String getLogFilePath() {
		return logFile == null ? null : logFile.getPath();
	}
	
	private void createLogger() {
		// Create logger
		synchronized (logCreatorLock) {
			String loggerName = System.currentTimeMillis() + "." + executionId + "." + node.getJobId();
			logger = Logger.getLogger(loggerName);

			// Create file appender
			String logName = createLogFileName(node.getExecutionId(), node.getJobId(), node.getAttempt());
			logFile = new File(workingDir, logName);
			String absolutePath = logFile.getAbsolutePath();

			jobAppender = null;
			try {
				RollingFileAppender fileAppender = new RollingFileAppender(loggerLayout, absolutePath, true);
				fileAppender.setMaxBackupIndex(jobLogBackupIndex);
				fileAppender.setMaxFileSize(jobLogChunkSize);
				jobAppender = fileAppender;
				logger.addAppender(jobAppender);
				logger.setAdditivity(false);
			} catch (IOException e) {
				flowLogger.error("Could not open log file in " + workingDir + " for job " + node.getJobId(), e);
			}
		}
	}

	private void closeLogger() {
		if (jobAppender != null) {
			logger.removeAppender(jobAppender);
			jobAppender.close();
		}
	}
	
	private void writeStatus() {
		try {
			node.setUpdateTime(System.currentTimeMillis());
			loader.updateExecutableNode(node);
		} catch (ExecutorManagerException e) {
			flowLogger.error("Could not update job properties in db for " + node.getJobId(), e);
		}
	}
	
	@Override
	public void run() {
		Thread.currentThread().setName("JobRunner-" + node.getJobId() + "-" + executionId);
		
		if (node.getStatus() == Status.DISABLED) {
			node.setStartTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_STARTED, null, false));
			node.setStatus(Status.SKIPPED);
			node.setEndTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_FINISHED));
			return;
		} else if (this.cancelled) {
			node.setStartTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_STARTED, null, false));
			node.setStatus(Status.FAILED);
			node.setEndTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_FINISHED));
		} else if (node.getStatus() == Status.FAILED || node.getStatus() == Status.KILLED) {
			node.setStartTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_STARTED, null, false));
			node.setEndTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_FINISHED));
			return;
		}
		else {
			createLogger();
			node.setUpdateTime(System.currentTimeMillis());

			// For pipelining of jobs. Will watch other jobs.
			if (!pipelineJobs.isEmpty()) {
				String blockedList = "";
				ArrayList<BlockingStatus> blockingStatus = new ArrayList<BlockingStatus>();
				for (String waitingJobId : pipelineJobs) {
					Status status = watcher.peekStatus(waitingJobId);
					if (status != null && !Status.isStatusFinished(status)) {
						BlockingStatus block = watcher.getBlockingStatus(waitingJobId);
						blockingStatus.add(block);
						blockedList += waitingJobId + ",";
					}
				}
				if (!blockingStatus.isEmpty()) {
					logger.info("Pipeline job " + node.getJobId() + " waiting on " + blockedList + " in execution " + watcher.getExecId());
					
					for(BlockingStatus bStatus: blockingStatus) {
						logger.info("Waiting on pipelined job " + bStatus.getJobId());
						currentBlockStatus = bStatus;
						bStatus.blockOnFinishedStatus();
						logger.info("Pipelined job " + bStatus.getJobId() + " finished.");
						if (watcher.isWatchCancelled()) {
							break;
						}
					}
					writeStatus();	
					fireEvent(Event.create(this, Type.JOB_STATUS_CHANGED));
				}
				if (watcher.isWatchCancelled()) {
					logger.info("Job was cancelled while waiting on pipeline. Quiting.");
					node.setStartTime(System.currentTimeMillis());
					node.setEndTime(System.currentTimeMillis());
					node.setStatus(Status.FAILED);
					fireEvent(Event.create(this, Type.JOB_FINISHED));
					return;
				}
			}
			
			currentBlockStatus = null;
			long currentTime = System.currentTimeMillis();
			if (delayStartMs > 0) {
				logger.info("Delaying start of execution for " + delayStartMs + " milliseconds.");
				synchronized(this) {
					try {
						this.wait(delayStartMs);
						logger.info("Execution has been delayed for " + delayStartMs + " ms. Continuing with execution.");
					} catch (InterruptedException e) {
						logger.error("Job " + node.getJobId() + " was to be delayed for " + delayStartMs + ". Interrupted after " + (System.currentTimeMillis() - currentTime));
					}
				}
				
				if (cancelled) {
					logger.info("Job was cancelled while in delay. Quiting.");
					node.setStartTime(System.currentTimeMillis());
					node.setEndTime(System.currentTimeMillis());
					fireEvent(Event.create(this, Type.JOB_FINISHED));
					return;
				}
			}
			
			node.setStartTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_STARTED, null, false));
			try {
				loader.uploadExecutableNode(node, props);
			} catch (ExecutorManagerException e1) {
				logger.error("Error writing initial node properties");
			}
			
			if (prepareJob()) {
				writeStatus();
				fireEvent(Event.create(this, Type.JOB_STATUS_CHANGED), false);
				runJob();
			}
			else {
				node.setStatus(Status.FAILED);
				logError("Job run failed!");
			}
			
			node.setEndTime(System.currentTimeMillis());

			logInfo("Finishing job " + node.getJobId() + " at " + node.getEndTime());

			closeLogger();
			writeStatus();
			
			if (logFile != null) {
				try {
					File[] files = logFile.getParentFile().listFiles(new FilenameFilter() {
						
						@Override
						public boolean accept(File dir, String name) {
							return name.startsWith(logFile.getName());
						}
					} 
					);
					Arrays.sort(files, Collections.reverseOrder());
					
					
					loader.uploadLogFile(executionId, node.getJobId(), node.getAttempt(), files);
				} catch (ExecutorManagerException e) {
					flowLogger.error("Error writing out logs for job " + node.getJobId(), e);
				}
			}
			else {
				flowLogger.info("Log file for job " + node.getJobId() + " is null");
			}
		}
		fireEvent(Event.create(this, Type.JOB_FINISHED));
	}
	
	private void fireEvent(Event event) {
		fireEvent(event, true);
	}
	
	private void fireEvent(Event event, boolean updateTime) {
		if (updateTime) {
			node.setUpdateTime(System.currentTimeMillis());
		}
		this.fireEventListeners(event);
	}
	
	private boolean prepareJob() throws RuntimeException {
		// Check pre conditions
		if (props == null || cancelled) {
			logError("Failing job. The job properties don't exist");
			return false;
		}
		
		synchronized(syncObject) {
			if (node.getStatus() == Status.FAILED || cancelled) {
				return false;
			}

			if (node.getAttempt() > 0) {
				logInfo("Starting job " + node.getJobId() + " attempt " + node.getAttempt() + " at " + node.getStartTime());
			}
			else {
				logInfo("Starting job " + node.getJobId() + " at " + node.getStartTime());
			}
			props.put(CommonJobProperties.JOB_ATTEMPT, node.getAttempt());
			props.put(CommonJobProperties.JOB_METADATA_FILE, createMetaDataFileName(executionId, node.getJobId(), node.getAttempt()));
			node.setStatus(Status.RUNNING);

			// Ability to specify working directory
			if (!props.containsKey(AbstractProcessJob.WORKING_DIR)) {
				props.put(AbstractProcessJob.WORKING_DIR, workingDir.getAbsolutePath());
			}
			
			if(props.containsKey("user.to.proxy")) {
				String jobProxyUser = props.getString("user.to.proxy");
				if(proxyUsers != null && !proxyUsers.contains(jobProxyUser)) {
					logger.error("User " + jobProxyUser + " has no permission to execute this job " + node.getJobId() + "!");
					return false;
				}
			}
			
			//job = JobWrappingFactory.getJobWrappingFactory().buildJobExecutor(node.getJobId(), props, logger);
			try {
				job = jobtypeManager.buildJobExecutor(node.getJobId(), props, logger);
			}
			catch (JobTypeManagerException e) {
				logger.error("Failed to build job type, skipping this job");
				return false;
			}
		}
		
		return true;
	}

	private void runJob() {
		try {
			job.run();
		} catch (Exception e) {
			e.printStackTrace();

			node.setStatus(Status.FAILED);
			logError("Job run failed!");
			logError(e.getMessage() + e.getCause());
			return;
		}

		node.setStatus(Status.SUCCEEDED);
		if (job != null) {
			outputProps = job.getJobGeneratedProperties();
			node.setOutputProps(outputProps);
		}
	}
	
	public void cancel() {
		synchronized (syncObject) {
			logError("Cancel has been called.");
			this.cancelled = true;
			
			BlockingStatus status = currentBlockStatus;
			if (status != null) {
				status.unblock();
			}
			
			// Cancel code here
			if (job == null) {
				logError("Job hasn't started yet.");
				// Just in case we're waiting on the delay
				synchronized(this) {
					this.notify();
				}
				return;
			}
	
			try {
				job.cancel();
			} catch (Exception e) {
				logError(e.getMessage());
				logError("Failed trying to cancel job. Maybe it hasn't started running yet or just finished.");
			}
		}
	}
	
	public boolean isCancelled() {
		return cancelled;
	}
	
	public Status getStatus() {
		return node.getStatus();
	}

	public Props getOutputProps() {
		return outputProps;
	}

	private void logError(String message) {
		if (logger != null) {
			logger.error(message);
		}
	}

	private void logInfo(String message) {
		if (logger != null) {
			logger.info(message);
		}
	}
	
	public File getLogFile() {
		return logFile;
	}
	
	public int getRetries() {
		return props.getInt("retries", 0);
	}
	
	public long getRetryBackoff() {
		return props.getLong("retry.backoff", 0);
	}
	
	public static String createLogFileName(int executionId, String jobId, int attempt) {
		return attempt > 0 ? "_job." + executionId + "." + attempt + "." + jobId + ".log" : "_job." + executionId + "." + jobId + ".log";
	}
	
	public static String createMetaDataFileName(int executionId, String jobId, int attempt) {
		return attempt > 0 ? "_job." + executionId + "." + attempt + "." + jobId + ".meta" : "_job." + executionId + "." + jobId + ".meta";
	}
}
