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
import java.io.IOException;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import azkaban.execapp.event.Event;
import azkaban.execapp.event.Event.Type;
import azkaban.execapp.event.EventHandler;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobtype.JobTypeManager;

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
	private Appender jobAppender;
	private File logFile;
	
	private Job job;
	private int executionId = -1;
	
	private static final Object logCreatorLock = new Object();
	private Object syncObject = new Object();
	
	private final JobTypeManager jobtypeManager;

	public JobRunner(ExecutableNode node, Props props, File workingDir, ExecutorLoader loader, JobTypeManager jobtypeManager) {
		this.props = props;
		this.node = node;
		this.workingDir = workingDir;
		this.executionId = node.getExecutionId();
		this.loader = loader;
		this.jobtypeManager = jobtypeManager;
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
				FileAppender fileAppender = new FileAppender(loggerLayout, absolutePath, true);
				
				jobAppender = fileAppender;
				logger.addAppender(jobAppender);
			} catch (IOException e) {
				logger.error("Could not open log file in " + workingDir, e);
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
			logger.error("Error writing node properties", e);
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
		else {
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
			
			node.setEndTime(System.currentTimeMillis());

			logInfo("Finishing job " + node.getJobId() + " at " + node.getEndTime());

			closeLogger();
			writeStatus();
			
			if (logFile != null) {
				try {
					loader.uploadLogFile(executionId, node.getJobId(), node.getAttempt(), logFile);
				} catch (ExecutorManagerException e) {
					System.err.println("Error writing out logs for job " + node.getJobId());
				}
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
			node.setStatus(Status.RUNNING);

			// Ability to specify working directory
			if (!props.containsKey(AbstractProcessJob.WORKING_DIR)) {
				props.put(AbstractProcessJob.WORKING_DIR, workingDir.getAbsolutePath());
			}

			//job = JobWrappingFactory.getJobWrappingFactory().buildJobExecutor(node.getJobId(), props, logger);
			job = jobtypeManager.buildJobExecutor(node.getJobId(), props, logger);
		}
		
		return true;
	}

	private void runJob() {
		try {
			job.run();
		} catch (Exception e) {
			node.setStatus(Status.FAILED);
			logError("Job run failed!");
			logError(e.getMessage());
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
