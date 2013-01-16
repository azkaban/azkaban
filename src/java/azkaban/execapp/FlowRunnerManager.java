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

package azkaban.execapp;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import azkaban.project.ProjectLoader;
import azkaban.execapp.event.Event;
import azkaban.execapp.event.EventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.jobtype.JobTypeManager;

import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;

/**
 * Execution manager for the server side execution.
 * 
 */
public class FlowRunnerManager implements EventListener {
	private static Logger logger = Logger.getLogger(FlowRunnerManager.class);
	private File executionDirectory;
	private File projectDirectory;

	private static final long RECENTLY_FINISHED_TIME_TO_LIVE = 120000; // recently finished secs to clean up. 1 minute
	
	private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
	
	private static final int DEFAULT_NUM_EXECUTING_FLOWS = 30;
	private Map<Pair<Integer,Integer>, ProjectVersion> installedProjects = new ConcurrentHashMap<Pair<Integer,Integer>, ProjectVersion>();
	private Map<Integer, FlowRunner> runningFlows = new ConcurrentHashMap<Integer, FlowRunner>();
	private Map<Integer, ExecutableFlow> recentlyFinishedFlows = new ConcurrentHashMap<Integer, ExecutableFlow>();
	private LinkedBlockingQueue<FlowRunner> flowQueue = new LinkedBlockingQueue<FlowRunner>();
	private int numThreads = DEFAULT_NUM_EXECUTING_FLOWS;

	private ExecutorService executorService;
	private SubmitterThread submitterThread;
	private CleanerThread cleanerThread;
	
	private ExecutorLoader executorLoader;
	private ProjectLoader projectLoader;
	
	private JobTypeManager jobtypeManager;
	
	private Props globalProps;
	
	public FlowRunnerManager(Props props, ExecutorLoader executorLoader, ProjectLoader projectLoader, ClassLoader parentClassLoader) throws IOException {
		executionDirectory = new File(props.getString("azkaban.execution.dir", "executions"));
		projectDirectory = new File(props.getString("azkaban.project.dir", "projects"));
		
		//JobWrappingFactory.init(props, getClass().getClassLoader());
		
		if (!executionDirectory.exists()) {
			executionDirectory.mkdirs();
		}
		if (!projectDirectory.exists()) {
			projectDirectory.mkdirs();
		}

		//azkaban.temp.dir
		numThreads = props.getInt("executor.flow.threads", DEFAULT_NUM_EXECUTING_FLOWS);
		executorService = Executors.newFixedThreadPool(numThreads);
		
		this.executorLoader = executorLoader;
		this.projectLoader = projectLoader;
		
		submitterThread = new SubmitterThread(flowQueue);
		submitterThread.start();
		
		cleanerThread = new CleanerThread();
		cleanerThread.start();
		
		jobtypeManager = new JobTypeManager(props.getString(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR, null), parentClassLoader);
		
	}

	public Props getGlobalProps() {
		return globalProps;
	}
	
	public void setGlobalProps(Props globalProps) {
		this.globalProps = globalProps;
	}
	
	private class SubmitterThread extends Thread {
		private BlockingQueue<FlowRunner> queue;
		private boolean shutdown = false;
		
		public SubmitterThread(BlockingQueue<FlowRunner> queue) {
			this.setName("FlowRunnerManager-Submitter-Thread");
			this.queue = queue;
		}

		public void shutdown() {
			shutdown = true;
			this.interrupt();
		}

		public void run() {
			while (!shutdown) {
				try {
					FlowRunner flowRunner = queue.take();
					executorService.submit(flowRunner);
				} catch (InterruptedException e) {
					logger.info("Interrupted. Probably to shut down.");
				}
			}
		}
	}
	
	private class CleanerThread extends Thread {
		private boolean shutdown = false;
		
		public CleanerThread() {
			this.setName("FlowRunnerManager-Cleaner-Thread");
		}
		
		public void shutdown() {
			shutdown = true;
			this.interrupt();
		}
		
		public void run() {
			while (!shutdown) {
				synchronized (this) {
					try {
						wait(RECENTLY_FINISHED_TIME_TO_LIVE);
						// Cleanup old stuff.
						cleanRecentlyFinished();
						cleanOlderProjects();
					} catch (InterruptedException e) {
						logger.info("Interrupted. Probably to shut down.");
					}
				}
			}
		}
	
		private void cleanRecentlyFinished() {
			long cleanupThreshold = System.currentTimeMillis() - RECENTLY_FINISHED_TIME_TO_LIVE;
			ArrayList<Integer> executionToKill = new ArrayList<Integer>();
			for (ExecutableFlow flow : recentlyFinishedFlows.values()) {
				if (flow.getEndTime() < cleanupThreshold) {
					executionToKill.add(flow.getExecutionId());
				}
			}
			
			for (Integer id: executionToKill) {
				recentlyFinishedFlows.remove(id);
			}
		}
		
		private void cleanOlderProjects() {
			Map<Integer, ArrayList<ProjectVersion>> projectVersions = new HashMap<Integer, ArrayList<ProjectVersion>>();
			for (ProjectVersion version : installedProjects.values() ) {
				ArrayList<ProjectVersion> versionList = projectVersions.get(version.getProjectId());
				if (versionList == null) {
					versionList = new ArrayList<ProjectVersion>();
					projectVersions.put(version.getProjectId(), versionList);
				}
				versionList.add(version);
			}
			
			HashSet<Pair<Integer,Integer>> activeProjectVersions = new HashSet<Pair<Integer,Integer>>();
			for(FlowRunner runner: runningFlows.values()) {
				ExecutableFlow flow = runner.getExecutableFlow();
				activeProjectVersions.add(new Pair<Integer,Integer>(flow.getProjectId(), flow.getVersion()));
			}
			
			for (Map.Entry<Integer, ArrayList<ProjectVersion>> entry: projectVersions.entrySet()) {
				Integer projectId = entry.getKey();
				ArrayList<ProjectVersion> installedVersions = entry.getValue();
				
				// Keep one version of the project around.
				if (installedVersions.size() == 1) {
					continue;
				}
				
				Collections.sort(installedVersions);
				for (int i = 0; i < installedVersions.size() - 1; ++i) {
					ProjectVersion version = installedVersions.get(i);
					Pair<Integer,Integer> versionKey = new Pair<Integer,Integer>(version.getProjectId(), version.getVersion());
					if (!activeProjectVersions.contains(versionKey)) {
						try {
							logger.info("Removing old unused installed project " + version.getProjectId() + ":" + version.getVersion());
							version.deleteDirectory();
						} catch (IOException e) {
							e.printStackTrace();
						}
						
						installedVersions.remove(versionKey);
					}
				}
			}
		}
	}
	
	public void submitFlow(int execId) throws ExecutorManagerException {
		// Load file and submit
		if (runningFlows.containsKey(execId)) {
			throw new ExecutorManagerException("Execution " + execId + " is already running.");
		}
		
		ExecutableFlow flow = null;
		flow = executorLoader.fetchExecutableFlow(execId);
		if (flow == null) {
			throw new ExecutorManagerException("Error loading flow with exec " + execId);
		}
		
		// Sets up the project files and execution directory.
		setupFlow(flow);
		
		// Setup flow runner
		FlowRunner runner = new FlowRunner(flow, executorLoader, jobtypeManager);
		runner.setGlobalProps(globalProps);
		runner.addListener(this);
		
		// Check again.
		if (runningFlows.containsKey(execId)) {
			throw new ExecutorManagerException("Execution " + execId + " is already running.");
		}
		
		// Finally, queue the sucker.
		runningFlows.put(execId, runner);
		flowQueue.add(runner);
	}
	
	private void setupFlow(ExecutableFlow flow) throws ExecutorManagerException {
		int execId = flow.getExecutionId();
		File execPath = new File(executionDirectory, String.valueOf(execId));
		flow.setExecutionPath(execPath.getPath());
		logger.info("Flow " + execId + " submitted with path " + execPath.getPath());
		execPath.mkdirs();
		
		// We're setting up the installed projects. First time, it may take a while to set up.
		Pair<Integer, Integer> projectVersionKey = new Pair<Integer,Integer>(flow.getProjectId(), flow.getVersion());
		
		// We set up project versions this way
		ProjectVersion projectVersion = null;
		synchronized(installedProjects) {
			projectVersion = installedProjects.get(projectVersionKey);
			if (projectVersion == null) {
				projectVersion = new ProjectVersion(flow.getProjectId(), flow.getVersion());
				installedProjects.put(projectVersionKey, projectVersion);
			}
		}

		try {
			projectVersion.setupProjectFiles(projectLoader, projectDirectory, logger);
			projectVersion.copyCreateSymlinkDirectory(execPath);
		} catch (Exception e) {
			e.printStackTrace();
			if (execPath.exists()) {
				try {
					FileUtils.deleteDirectory(execPath);
				}
				catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			throw new ExecutorManagerException(e);
		}
	}
	
	public void cancelFlow(int execId, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		
		if (runner == null) {
			throw new ExecutorManagerException("Execution " + execId + " is not running.");
		}
		
		runner.cancel(user);
	}
	
	public void pauseFlow(int execId, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		
		if (runner == null) {
			throw new ExecutorManagerException("Execution " + execId + " is not running.");
		}
		
		runner.pause(user);
	}
	
	public void resumeFlow(int execId, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		
		if (runner == null) {
			throw new ExecutorManagerException("Execution " + execId + " is not running.");
		}
		
		runner.resume(user);
	}
	
	public ExecutableFlow getExecutableFlow(int execId) {
		FlowRunner runner = runningFlows.get(execId);
		if (runner == null) {
			return recentlyFinishedFlows.get(execId);
		}
		return runner.getExecutableFlow();
	}

	@Override
	public void handleEvent(Event event) {
		if (event.getType() == Event.Type.FLOW_FINISHED) {
			FlowRunner flowRunner = (FlowRunner)event.getRunner();
			ExecutableFlow flow = flowRunner.getExecutableFlow();
			recentlyFinishedFlows.put(flow.getExecutionId(), flow);

			File dir = flowRunner.getExecutionDir();
			if (dir != null && dir.exists()) {
				try {
					synchronized(dir) {
						if(flow.getStatus() == Status.SUCCEEDED)
							FileUtils.deleteDirectory(dir);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			runningFlows.remove(flow.getExecutionId());
		}
	}
	
	public LogData readFlowLogs(int execId, int startByte, int length) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		if (runner == null) {
			throw new ExecutorManagerException("Running flow " + execId + " not found.");
		}
		
		File dir = runner.getExecutionDir();
		if (dir != null && dir.exists()) {
			try {
				synchronized(dir) {
					if (!dir.exists()) {
						throw new ExecutorManagerException("Execution dir file doesn't exist. Probably has beend deleted");
					}
					
					File logFile = runner.getFlowLogFile();
					if (logFile != null && logFile.exists()) {
						return FileIOUtils.readUtf8File(logFile, startByte, length);
					}
					else {
						throw new ExecutorManagerException("Flow log file doesn't exist.");
					}
				}
			} catch (IOException e) {
				throw new ExecutorManagerException(e);
			}
		}
		
		throw new ExecutorManagerException("Error reading file. Log directory doesn't exist.");
	}
	
	public LogData readJobLogs(int execId, String jobId, int startByte, int length) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		if (runner == null) {
			throw new ExecutorManagerException("Running flow " + execId + " not found.");
		}
		
		File dir = runner.getExecutionDir();
		if (dir != null && dir.exists()) {
			try {
				synchronized(dir) {
					if (!dir.exists()) {
						throw new ExecutorManagerException("Execution dir file doesn't exist. Probably has beend deleted");
					}
					File logFile = runner.getJobLogFile(jobId);
					if (logFile != null && logFile.exists()) {
						return FileIOUtils.readUtf8File(logFile, startByte, length);
					}
					else {
						throw new ExecutorManagerException("Job log file doesn't exist.");
					}
				}
			} catch (IOException e) {
				throw new ExecutorManagerException(e);
			}
		}
		
		throw new ExecutorManagerException("Error reading file. Log directory doesn't exist.");
	}
}
