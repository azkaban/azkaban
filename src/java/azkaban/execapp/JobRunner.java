package azkaban.execapp;
/*
 * Copyright 2012 LinkedIn, Inc
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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections.comparators.ComparatorChain;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import azkaban.execapp.event.Event;
import azkaban.execapp.event.Event.Type;
import azkaban.execapp.event.EventHandler;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
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
	private ExecutableFlow flow = null;
	
	private Appender jobAppender;
	private File logFile;
	
	private Job job;
	private int executionId = -1;
	
	private static final Object logCreatorLock = new Object();
	private Object syncObject = new Object();
	
	private final JobTypeManager jobtypeManager;
	private HashSet<String> proxyUsers = null;
	
	private boolean userLockDown;
	private String jobLogChunkSize;
	private int jobLogBackupIndex;

	public JobRunner(Props azkabanProps, ExecutableNode node, Props props, File workingDir, HashSet<String> proxyUsers, ExecutorLoader loader, JobTypeManager jobtypeManager, Logger flowLogger, ExecutableFlow flow) {
		this.props = props;
		this.node = node;
		this.workingDir = workingDir;
		this.executionId = node.getExecutionId();
		this.loader = loader;
		this.jobtypeManager = jobtypeManager;
		this.flowLogger = flowLogger;
		this.proxyUsers = proxyUsers;
		
		// default no lock down but warn
		this.userLockDown = azkabanProps.getBoolean("proxy.user.lock.down", false);
		// default 20MB log size rolling over.
		this.jobLogChunkSize = azkabanProps.getString("job.log.chunk.size", "5MB");
		this.jobLogBackupIndex = azkabanProps.getInt("job.log.backup.index", 4);
		
		this.flow = flow;
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
			String logName = node.getAttempt() > 0 ? "_job." + executionId + "." + node.getAttempt() + "." + node.getJobId() + ".log" : "_job." + executionId + "." + node.getJobId() + ".log";
			logFile = new File(workingDir, logName);
			String absolutePath = logFile.getAbsolutePath();

			jobAppender = null;
			try {
				RollingFileAppender fileAppender = new RollingFileAppender(loggerLayout, absolutePath, true);
				fileAppender.setMaxBackupIndex(jobLogBackupIndex);
				fileAppender.setMaxFileSize(jobLogChunkSize);
				jobAppender = fileAppender;
				logger.addAppender(jobAppender);
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

		node.setStartTime(System.currentTimeMillis());
		
		if (node.getStatus() == Status.DISABLED) {
			fireEvent(Event.create(this, Type.JOB_STARTED, null, false));
			node.setStatus(Status.SKIPPED);
			node.setEndTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_FINISHED));
			return;
		} else if (node.getStatus() == Status.KILLED) {
			fireEvent(Event.create(this, Type.JOB_STARTED, null, false));
			node.setEndTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_FINISHED));
			return;
		}
		else if (node.getStatus() == Status.QUEUED) {
			// check parent jobs' statuses
			for (String parentName : node.getInNodes()) {
				ExecutableNode parentNode = flow.getExecutableNode(parentName);
				Status parentStatus = parentNode.getStatus();
				if (parentStatus != Status.SUCCEEDED && parentStatus != Status.DISABLED && parentStatus != Status.SKIPPED) {
					// probably an error since parent failed
					flowLogger.error("Job " + node.getJobId() + " tried to run when parent node " + parentName + " has status:" + parentStatus);
					// Kill the job
					fireEvent(Event.create(this, Type.JOB_STARTED, null, false));
					node.setEndTime(System.currentTimeMillis());
					node.setStatus(Status.KILLED);
					fireEvent(Event.create(this, Type.JOB_FINISHED));
					return;
				}
			}
			
			createLogger();
			
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
			fireEvent(Event.create(this, Type.JOB_FINISHED));
		}
		else {
			flowLogger.warn("Job " + node.getJobId() + " tried to run with unhandled status " + node.getStatus());
			fireEvent(Event.create(this, Type.JOB_STARTED, null, false));
			node.setEndTime(System.currentTimeMillis());
			fireEvent(Event.create(this, Type.JOB_FINISHED));
		}
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
	
	private boolean prepareJob() throws RuntimeException{
		// Check pre conditions
		if (props == null) {
			node.setStatus(Status.FAILED);
			logError("Failing job. The job properties don't exist");
			return false;
		}
		
		synchronized(syncObject) {
			if (node.getStatus() == Status.FAILED) {
				return false;
			}

			if (node.getAttempt() > 0) {
				logInfo("Starting job " + node.getJobId() + " attempt " + node.getAttempt() + " at " + node.getStartTime());
			}
			else {
				logInfo("Starting job " + node.getJobId() + " at " + node.getStartTime());
			}
			props.put(CommonJobProperties.JOB_ATTEMPT, node.getAttempt());
			node.setStatus(Status.RUNNING);

			// Ability to specify working directory
			if (!props.containsKey(AbstractProcessJob.WORKING_DIR)) {
				props.put(AbstractProcessJob.WORKING_DIR, workingDir.getAbsolutePath());
			}
			
			if(props.containsKey("user.to.proxy")) {
				String jobProxyUser = props.getString("user.to.proxy");
				if(! proxyUsers.contains(jobProxyUser)) {
					logger.error("User " + jobProxyUser + " has no permission to execute this job " + node.getJobId() + "!");
					if(userLockDown) {
						return false;
					}
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
			
			// Cancel code here
			if (job == null) {
				node.setStatus(Status.FAILED);
				logError("Job hasn't started yet.");
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
}
