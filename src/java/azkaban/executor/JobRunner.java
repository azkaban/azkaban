package azkaban.executor;

import java.io.File;
import java.io.IOException;

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
import azkaban.utils.Props;

public class JobRunner extends EventHandler implements Runnable {
	private static final Layout DEFAULT_LAYOUT = new PatternLayout(
			"%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");

	private Props props;
	private Props outputProps;
	private ExecutableNode node;
	private File workingDir;
	
	private Logger logger;
	private Layout loggerLayout = DEFAULT_LAYOUT;
	private Appender jobAppender;
	
	private static final Object logCreatorLock = new Object();
	
	public JobRunner(ExecutableNode node, Props props, File workingDir) {
		this.props = props;
		this.node = node;
		this.node.setStatus(Status.WAITING);
		this.workingDir = workingDir;

		createLogger();
	}

	public ExecutableNode getNode() {
		return node;
	}

	private void createLogger() {
		// Create logger
		synchronized(logCreatorLock) {
			String loggerName = System.currentTimeMillis() + "." + node.getFlow().getExecutionId() + "." + node.getId();
			logger = Logger.getLogger(loggerName);
	
			// Create file appender
			String logName = "_job." + node.getFlow().getExecutionId() + "." + node.getId() + ".log";
			File logFile = new File(workingDir, logName);
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
		logger.removeAppender(jobAppender);
		jobAppender.close();
	}
	
	@Override
	public void run() {
		if (node.getStatus() == Status.DISABLED) {
			this.fireEventListeners(Event.create(this, Type.JOB_SUCCEEDED));
			return;
		} else if (node.getStatus() == Status.KILLED) {
			this.fireEventListeners(Event.create(this, Type.JOB_KILLED));
			return;
		}
		node.setStartTime(System.currentTimeMillis());
		logger.info("Starting job " + node.getId() + " at " + node.getStartTime());
		node.setStatus(Status.RUNNING);
		this.fireEventListeners(Event.create(this, Type.JOB_STARTED));

		// Run Job
		boolean succeeded = true;

		node.setEndTime(System.currentTimeMillis());
		if (succeeded) {
			node.setStatus(Status.SUCCEEDED);
			this.fireEventListeners(Event.create(this, Type.JOB_SUCCEEDED));
		} else {
			node.setStatus(Status.FAILED);
			this.fireEventListeners(Event.create(this, Type.JOB_FAILED));
		}
		logger.info("Finishing job " + node.getId() + " at " + node.getEndTime());
		closeLogger();
	}

	public void cancel() {
		// Cancel code here

		node.setStatus(Status.KILLED);
	}

	public Status getStatus() {
		return node.getStatus();
	}

	public Props getOutputProps() {
		return outputProps;
	}
}
