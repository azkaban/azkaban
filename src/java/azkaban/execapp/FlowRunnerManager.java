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
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.LocalFlowWatcher;
import azkaban.execapp.event.RemoteFlowWatcher;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.jobtype.JobTypeManager;

import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
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

	private static final long RECENTLY_FINISHED_TIME_TO_LIVE = 60*1000; // recently finished secs to clean up. 1 minute
	
	private static final int DEFAULT_NUM_EXECUTING_FLOWS = 30;
	private Map<Pair<Integer,Integer>, ProjectVersion> installedProjects = new ConcurrentHashMap<Pair<Integer,Integer>, ProjectVersion>();
	private Map<Integer, FlowRunner> runningFlows = new ConcurrentHashMap<Integer, FlowRunner>();
	private Map<Integer, ExecutableFlow> recentlyFinishedFlows = new ConcurrentHashMap<Integer, ExecutableFlow>();
	private LinkedBlockingQueue<FlowRunner> flowQueue = new LinkedBlockingQueue<FlowRunner>();
	private int numThreads = DEFAULT_NUM_EXECUTING_FLOWS;

	private ExecutorService executorService;
	private SubmitterThread submitterThread;
	private CleanerThread cleanerThread;
	private int numJobThreadPerFlow = 10;
	
	private ExecutorLoader executorLoader;
	private ProjectLoader projectLoader;
	
	private JobTypeManager jobtypeManager;
	
	private Props globalProps;
	
	private final Props azkabanProps;
	
	private long lastSubmitterThreadCheckTime = -1;
	private long lastCleanerThreadCheckTime = -1;
	private long executionDirRetention = 1*24*60*60*1000;
	
	// We want to limit the log sizes to about 20 megs
	private String jobLogChunkSize = "5MB";
	private int jobLogNumFiles = 4;
	
	// If true, jobs will validate proxy user against a list of valid proxy users.
	private boolean validateProxyUser = false;
	
	private Object executionDirDeletionSync = new Object();
		
	public FlowRunnerManager(Props props, ExecutorLoader executorLoader, ProjectLoader projectLoader, ClassLoader parentClassLoader) throws IOException {
		executionDirectory = new File(props.getString("azkaban.execution.dir", "executions"));
		projectDirectory = new File(props.getString("azkaban.project.dir", "projects"));
		
		azkabanProps = props;
		
		//JobWrappingFactory.init(props, getClass().getClassLoader());
		executionDirRetention = props.getLong("execution.dir.retention", executionDirRetention);
		logger.info("Execution dir retention set to " + executionDirRetention + " ms");
		
		if (!executionDirectory.exists()) {
			executionDirectory.mkdirs();
		}
		if (!projectDirectory.exists()) {
			projectDirectory.mkdirs();
		}

		installedProjects = loadExistingProjects();
		
		//azkaban.temp.dir
		numThreads = props.getInt("executor.flow.threads", DEFAULT_NUM_EXECUTING_FLOWS);
		numJobThreadPerFlow = props.getInt("flow.num.job.threads", numJobThreadPerFlow);
		executorService = Executors.newFixedThreadPool(numThreads);
		
		this.executorLoader = executorLoader;
		this.projectLoader = projectLoader;
		
		this.jobLogChunkSize = azkabanProps.getString("job.log.chunk.size", "5MB");
		this.jobLogNumFiles = azkabanProps.getInt("job.log.backup.index", 4);
		
		this.validateProxyUser = azkabanProps.getBoolean("proxy.user.lock.down", false);
		
		submitterThread = new SubmitterThread(flowQueue);
		submitterThread.start();
		
		cleanerThread = new CleanerThread();
		cleanerThread.start();
		
		jobtypeManager = new JobTypeManager(props.getString(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR, JobTypeManager.DEFAULT_JOBTYPEPLUGINDIR), parentClassLoader);
	}

	private Map<Pair<Integer, Integer>, ProjectVersion> loadExistingProjects() {
		Map<Pair<Integer, Integer>, ProjectVersion> allProjects = new HashMap<Pair<Integer,Integer>, ProjectVersion>();
		for(File project : projectDirectory.listFiles(new FilenameFilter() {
			
			String pattern = "[0-9]+\\.[0-9]+";
			@Override
			public boolean accept(File dir, String name) {
				return name.matches(pattern);
			}
		})) {
			if(project.isDirectory()) {
				try {
					String fileName = new File(project.getAbsolutePath()).getName();
					int projectId = Integer.parseInt(fileName.split("\\.")[0]);
					int versionNum = Integer.parseInt(fileName.split("\\.")[1]);
					ProjectVersion version = new ProjectVersion(projectId, versionNum, project);
					allProjects.put(new Pair<Integer, Integer>(projectId, versionNum), version);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return allProjects;
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

		@SuppressWarnings("unused")
		public void shutdown() {
			shutdown = true;
			this.interrupt();
		}

		public void run() {
			while (!shutdown) {
				try {
					lastSubmitterThreadCheckTime = System.currentTimeMillis();
					FlowRunner flowRunner = queue.take();
					executorService.submit(flowRunner);
				} catch (InterruptedException e) {
					logger.info("Interrupted. Probably to shut down.");
				}
			}
		}
	}
	
	private class CleanerThread extends Thread {
		// Every hour, clean execution dir.
		private static final long EXECUTION_DIR_CLEAN_INTERVAL_MS = 60*60*1000;
		// Every 5 mins clean the old project dir
		private static final long OLD_PROJECT_DIR_INTERVAL_MS = 5*60*1000;
		// Every 2 mins clean the recently finished list
		private static final long RECENTLY_FINISHED_INTERVAL_MS = 2*60*1000;
		
		private boolean shutdown = false;
		private long lastExecutionDirCleanTime = -1;
		private long lastOldProjectCleanTime = -1;
		private long lastRecentlyFinishedCleanTime = -1;
		
		public CleanerThread() {
			this.setName("FlowRunnerManager-Cleaner-Thread");
		}
		
		@SuppressWarnings("unused")
		public void shutdown() {
			shutdown = true;
			this.interrupt();
		}
		
		public void run() {
			while (!shutdown) {
				synchronized (this) {
					try {
						lastCleanerThreadCheckTime = System.currentTimeMillis();
						
						// Cleanup old stuff.
						long currentTime = System.currentTimeMillis();
						if (currentTime - RECENTLY_FINISHED_INTERVAL_MS > lastRecentlyFinishedCleanTime) {
							logger.info("Cleaning recently finished");
							cleanRecentlyFinished();
							lastRecentlyFinishedCleanTime = currentTime;
						}
						
						if (currentTime - OLD_PROJECT_DIR_INTERVAL_MS > lastOldProjectCleanTime) {
							logger.info("Cleaning old projects");
							cleanOlderProjects();
							
							lastOldProjectCleanTime = currentTime;
						}
						
						if (currentTime - EXECUTION_DIR_CLEAN_INTERVAL_MS > lastExecutionDirCleanTime) {
							logger.info("Cleaning old execution dirs");
							cleanOlderExecutionDirs();
							lastExecutionDirCleanTime = currentTime;
						}
						
						wait(RECENTLY_FINISHED_TIME_TO_LIVE);
					} catch (InterruptedException e) {
						logger.info("Interrupted. Probably to shut down.");
					}
				}
			}
		}
	
		private void cleanOlderExecutionDirs() {
			File dir = executionDirectory;
			
			final long pastTimeThreshold = System.currentTimeMillis() - executionDirRetention;
			File[] executionDirs = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File path) {
					if (path.isDirectory() && path.lastModified() < pastTimeThreshold) {
						return true;
					}
					return false;
				}
			});
			
			for (File exDir : executionDirs) {
				try {
					int execId = Integer.valueOf(exDir.getName());
					if (runningFlows.containsKey(execId) || recentlyFinishedFlows.containsKey(execId)) {
						continue;
					}
				}
				catch (NumberFormatException e) {
					logger.error("Can't delete exec dir " + exDir.getName() + " it is not a number");
					continue;
				}
			
				synchronized(executionDirDeletionSync) {
					try {
						FileUtils.deleteDirectory(exDir);
					} catch (IOException e) {
						logger.error("Error cleaning execution dir " + exDir.getPath(), e);
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
				logger.info("Cleaning execution " + id + " from recently finished flows list.");
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
				//Integer projectId = entry.getKey();
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
							installedProjects.remove(new Pair<Integer, Integer>(version.getProjectId(), version.getVersion()));
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
		FlowWatcher watcher = null;
		ExecutionOptions options = flow.getExecutionOptions();
		if (options.getPipelineExecutionId() != null) {
			Integer pipelineExecId = options.getPipelineExecutionId();
			FlowRunner runner = runningFlows.get(pipelineExecId);
			
			if (runner != null) {
				watcher = new LocalFlowWatcher(runner);
			}
			else {
				watcher = new RemoteFlowWatcher(pipelineExecId, executorLoader);
			}
		}

		int numJobThreads = numJobThreadPerFlow;
		if(options.getFlowParameters().containsKey("flow.num.job.threads")) {
			try{
				int numJobs = Integer.valueOf(options.getFlowParameters().get("flow.num.job.threads"));
				if(numJobs > 0 && numJobs <= numJobThreads) {
					numJobThreads = numJobs;
				}
			} catch (Exception e) {
				throw new ExecutorManagerException("Failed to set the number of job threads " + options.getFlowParameters().get("flow.num.job.threads") + " for flow " + execId, e);
			}
		}
		
		FlowRunner runner = new FlowRunner(flow, executorLoader, projectLoader, jobtypeManager);
		runner.setFlowWatcher(watcher)
			.setJobLogSettings(jobLogChunkSize, jobLogNumFiles)
			.setValidateProxyUser(validateProxyUser)
			.setGlobalProps(globalProps)
			.setNumJobThreads(numJobThreads)
			.addListener(this);
		
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
		
		runner.kill(user);
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
	
	public void retryFailures(int execId, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		
		if (runner == null) {
			throw new ExecutorManagerException("Execution " + execId + " is not running.");
		}
		
		runner.retryFailures(user);
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
			logger.info("Flow " + flow.getExecutionId() + " is finished. Adding it to recently finished flows list.");
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
				synchronized(executionDirDeletionSync) {
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
	
	public LogData readJobLogs(int execId, String jobId, int attempt, int startByte, int length) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		if (runner == null) {
			throw new ExecutorManagerException("Running flow " + execId + " not found.");
		}
		
		File dir = runner.getExecutionDir();
		if (dir != null && dir.exists()) {
			try {
				synchronized (executionDirDeletionSync) {
					if (!dir.exists()) {
						throw new ExecutorManagerException("Execution dir file doesn't exist. Probably has beend deleted");
					}
					File logFile = runner.getJobLogFile(jobId, attempt);
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

	public List<Object> readJobAttachments(int execId, String jobId, int attempt)
			throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		if (runner == null) {
			throw new ExecutorManagerException(
					"Running flow " + execId + " not found.");
		}

		File dir = runner.getExecutionDir();
		if (dir == null || !dir.exists()) {
			throw new ExecutorManagerException(
					"Error reading file. Log directory doesn't exist.");
		}

		try {
			synchronized (executionDirDeletionSync) {
				if (!dir.exists()) {
					throw new ExecutorManagerException(
							"Execution dir file doesn't exist. Probably has beend deleted");
				}

				File attachmentFile = runner.getJobAttachmentFile(jobId, attempt);
				if (attachmentFile == null || !attachmentFile.exists()) {
					return null;
				}
				
				@SuppressWarnings("unchecked")
        List<Object> jobAttachments = (ArrayList<Object>) JSONUtils.parseJSONFromFile(attachmentFile); 
				
				return jobAttachments;
			}
		}
		catch (IOException e) {
			throw new ExecutorManagerException(e);
		}
	}
	
	public JobMetaData readJobMetaData(int execId, String jobId, int attempt, int startByte, int length) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(execId);
		if (runner == null) {
			throw new ExecutorManagerException("Running flow " + execId + " not found.");
		}
		
		File dir = runner.getExecutionDir();
		if (dir != null && dir.exists()) {
			try {
				synchronized(executionDirDeletionSync) {
					if (!dir.exists()) {
						throw new ExecutorManagerException("Execution dir file doesn't exist. Probably has beend deleted");
					}
					File metaDataFile = runner.getJobMetaDataFile(jobId, attempt);
					if (metaDataFile != null && metaDataFile.exists()) {
						return FileIOUtils.readUtf8MetaDataFile(metaDataFile, startByte, length);
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

	public long getLastCleanerThreadCheckTime() {
		return lastCleanerThreadCheckTime;
	}

	public long getLastSubmitterThreadCheckTime() {
		return lastSubmitterThreadCheckTime;
	}

	public boolean isSubmitterThreadActive() {
		return this.submitterThread.isAlive();
	}

	public boolean isCleanerThreadActive() {
		return this.cleanerThread.isAlive();
	}
	
	public State getSubmitterThreadState() {
		return this.submitterThread.getState();
	}

	public State getCleanerThreadState() {
		return this.cleanerThread.getState();
	}
	
	public boolean isExecutorThreadPoolShutdown() {
		return executorService.isShutdown();
	}
	
	public int getNumExecutingFlows() {
		return runningFlows.size();
	}
	
	public String getRunningFlowIds() {
		ArrayList<Integer> ids = new ArrayList<Integer>(runningFlows.keySet());
		Collections.sort(ids);
		return ids.toString();
	}

	public int getNumExecutingJobs() {
		int jobCount = 0;
		for (FlowRunner runner: runningFlows.values()) {
			jobCount += runner.getNumRunningJobs();
		}
		
		return jobCount;
	}

	
	
}
