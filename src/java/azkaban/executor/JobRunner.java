package azkaban.executor;
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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.event.EventHandler;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobExecutor.utils.JobWrappingFactory;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class JobRunner extends EventHandler implements Runnable {
	private static final Layout DEFAULT_LAYOUT = new PatternLayout("%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");

	private Props props;
	private Props outputProps;
	private ExecutableNode node;
	private File workingDir;

	private Logger logger = null;
	private Layout loggerLayout = DEFAULT_LAYOUT;
	private Appender jobAppender;
	private File logFile;
	
	private Job job;
	private String executionId = null;
	private boolean testMode = false;
	
	private static final Object logCreatorLock = new Object();
	
	public JobRunner(ExecutableNode node, Props props, File workingDir) {
		this.props = props;
		this.node = node;
		this.workingDir = workingDir;
		this.executionId = node.getFlow().getExecutionId();
	}

	public JobRunner(String executionId, ExecutableNode node, Props props, File workingDir) {
		this.props = props;
		this.node = node;
		this.workingDir = workingDir;
		this.executionId = executionId;
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
			String loggerName = System.currentTimeMillis() + "." + executionId + "." + node.getId();
			logger = Logger.getLogger(loggerName);

			// Create file appender
			String logName = "_job." + executionId + "." + node.getId() + ".log";
			logFile = new File(workingDir, logName);
			String absolutePath = logFile.getAbsolutePath();

			jobAppender = null;
			try {
				jobAppender = new FileAppender(loggerLayout, absolutePath, false);
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
		NodeStatus status = new NodeStatus(this.node);
		String statusName = "_job." + executionId + "." + node.getId() + ".status";
		File statusFile = new File(workingDir, statusName);
		try {
			JSONUtils.toJSON(status.toObject(), statusFile);
		} catch (IOException e) {
			logger.error("Couldn't write status file.");
		}
	}
	
	@Override
	public void run() {
		node.setStartTime(System.currentTimeMillis());
		if (node.getStatus() == Status.DISABLED) {
			node.setStatus(Status.SKIPPED);
			node.setEndTime(System.currentTimeMillis());
			this.fireEventListeners(Event.create(this, Type.JOB_SUCCEEDED));
			return;
		} else if (node.getStatus() == Status.KILLED) {
			node.setEndTime(System.currentTimeMillis());
			this.fireEventListeners(Event.create(this, Type.JOB_KILLED));
			return;
		}

		createLogger();
		this.node.setStatus(Status.WAITING);
		
		logInfo("Starting job " + node.getId() + " at " + node.getStartTime());
		node.setStatus(Status.RUNNING);
		this.fireEventListeners(Event.create(this, Type.JOB_STARTED));
		writeStatus();
		
		boolean succeeded = true;

		props.put(AbstractProcessJob.WORKING_DIR, workingDir.getAbsolutePath());
		job = JobWrappingFactory.getJobWrappingFactory().buildJobExecutor(node.getId(), props, logger);

		if (testMode) {
			logInfo("Test Mode. Skipping.");
			synchronized(this) {
				try {
					wait(5000);
				} catch (InterruptedException e) {
				}
			}
		}
		else {
			try {
				job.run();
			} catch (Throwable e) {
				succeeded = false;
				node.setStatus(Status.FAILED);
				logError("Job run failed!");
				e.printStackTrace();
			}
		}

		node.setEndTime(System.currentTimeMillis());
		if (succeeded) {
			node.setStatus(Status.SUCCEEDED);
			if (job != null) {
				outputProps = job.getJobGeneratedProperties();
			}
			this.fireEventListeners(Event.create(this, Type.JOB_SUCCEEDED));
		} else {
			System.out.println("Setting FAILED to " + node.getId());
			this.fireEventListeners(Event.create(this, Type.JOB_FAILED));
		}
		logInfo("Finishing job " + node.getId() + " at " + node.getEndTime());
		closeLogger();
		writeStatus();
	}

	public synchronized void cancel() {
		logError("Cancel has been called.");
		// Cancel code here
		if (job == null) {
			logError("Job doesn't exist!");
			return;
		}

		try {
			job.cancel();
		} catch (Exception e) {
			logError(e.getMessage());
			logError("Failed trying to cancel job. Maybe it hasn't started running yet or just finished.");
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

	public boolean isTestMode() {
		return testMode;
	}

	public void setTestMode(boolean testMode) {
		this.testMode = testMode;
	}

}
