package azkaban.executor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import azkaban.executor.ExecutableFlow.ExecutableNode;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.event.EventHandler;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobExecutor.utils.JobWrappingFactory;
import azkaban.utils.Props;

public class JobRunner extends EventHandler implements Runnable {
	private static final Layout DEFAULT_LAYOUT = new PatternLayout(
			"%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");

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

		boolean succeeded = true;

		props.put(AbstractProcessJob.WORKING_DIR, workingDir.getAbsolutePath());
		job = JobWrappingFactory.getJobWrappingFactory().buildJobExecutor(node.getId(), props, logger);

		try {
			job.run();
		} catch (Throwable e) {
			succeeded = false;
			logError("Job run failed!");
			e.printStackTrace();
		}

		node.setEndTime(System.currentTimeMillis());
		if (succeeded) {
			node.setStatus(Status.SUCCEEDED);
			if (job != null) {
				outputProps = job.getJobGeneratedProperties();
			}
			this.fireEventListeners(Event.create(this, Type.JOB_SUCCEEDED));
		} else {
			node.setStatus(Status.FAILED);
			this.fireEventListeners(Event.create(this, Type.JOB_FAILED));
		}
		logInfo("Finishing job " + node.getId() + " at " + node.getEndTime());
		closeLogger();
	}

	public synchronized void cancel() {
		// Cancel code here
		if (job == null) {
			logError("Job doesn't exist!");
			return;
		}

		try {
			job.cancel();
		} catch (Exception e) {
			logError("Failed trying to cancel job!");
			e.printStackTrace();
		}

		// will just interrupt, I guess, until the code is finished.
		this.notifyAll();

		node.setStatus(Status.KILLED);
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

}
