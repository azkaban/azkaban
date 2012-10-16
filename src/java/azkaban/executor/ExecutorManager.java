package azkaban.executor;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow.Status;
import azkaban.flow.Flow;
import azkaban.utils.ExecutableFlowLoader;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanExecutorServer;

/**
 * Executor manager used to manage the client side job.
 *
 */
public class ExecutorManager {
	private static final String ACTIVE_DIR = ".active";
	private static final String ARCHIVE_DIR = ".archive";
	private static final String JOB_EXECUTION_DIR = ".jobexecutions";
	
	private static Logger logger = Logger.getLogger(ExecutorManager.class);
	// 30 seconds of retry before failure.
	private static final long ACCESS_ERROR_THRESHOLD_MS = 30000;
	
	// log read buffer.
	private static final int LOG_BUFFER_READ_SIZE = 10*1024;
	
	private File basePath;

	private AtomicInteger counter = new AtomicInteger();
	private String token;
	private int portNumber;
	private String url = "localhost";
	
	private ConcurrentHashMap<String, ExecutableFlow> runningFlows = new ConcurrentHashMap<String, ExecutableFlow>();
	private ConcurrentHashMap<String, ExecutionReference> runningReference = new ConcurrentHashMap<String, ExecutionReference>();
	
	private CacheManager manager = CacheManager.create();
	private Cache recentFlowsCache;
	private static final int LIVE_SECONDS = 600;
	private Object BlockObj = new Object();
	
	ExecutingManagerUpdaterThread executingManager;
	
	public ExecutorManager(Props props) throws IOException, ExecutorManagerException {
		basePath = new File(props.getString("execution.directory"));
		if (!basePath.exists()) {
			logger.info("Execution directory " + basePath + " not found.");
			if (basePath.mkdirs()) {
				logger.info("Execution directory " + basePath + " created.");
			}
			else {
				throw new RuntimeException("Execution directory " + basePath + " does not exist and cannot be created.");
			}
		}
		setupCache();
		
		File activePath = new File(basePath, ACTIVE_DIR);
		if(!activePath.exists() && !activePath.mkdirs()) {
			throw new RuntimeException("Execution directory " + activePath + " does not exist and cannot be created.");
		}
		
		File archivePath = new File(basePath, ARCHIVE_DIR);
		if(!archivePath.exists() && !archivePath.mkdirs()) {
			throw new RuntimeException("Execution directory " + archivePath + " does not exist and cannot be created.");
		}
		
		portNumber = props.getInt("executor.port", AzkabanExecutorServer.DEFAULT_PORT_NUMBER);
		token = props.getString("executor.shared.token", "");
		counter.set(0);
		loadActiveExecutions();
		
		executingManager = new ExecutingManagerUpdaterThread();
		executingManager.start();
	}
	
	private void setupCache() {
		CacheConfiguration cacheConfig = new CacheConfiguration("recentFlowsCache",2000)
				.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.FIFO)
				.overflowToDisk(false)
				.eternal(false)
				.timeToLiveSeconds(LIVE_SECONDS)
				.diskPersistent(false)
				.diskExpiryThreadIntervalSeconds(0);

		recentFlowsCache = new Cache(cacheConfig);
		manager.addCache(recentFlowsCache);
	}
	
	public int getJobHistory(String projectId, String jobId, int numResults, int skip, List<NodeStatus> nodes) throws ExecutorManagerException{
		File flowProjectPath = new File(basePath, projectId);
		if (!flowProjectPath.exists()) {
			throw new ExecutorManagerException("Project " + projectId + " directory doesn't exist.");
		}

		File jobStatusDir = new File(flowProjectPath, JOB_EXECUTION_DIR + File.separator + jobId);
		
		File[] jobsStatusFiles = jobStatusDir.listFiles();
		
		if (jobsStatusFiles.length == 0 || skip >= jobsStatusFiles.length) {
			return 0;
		}
		
		Arrays.sort(jobsStatusFiles);
		int index = (jobsStatusFiles.length - skip - 1);
		
		for (int count = 0; count < numResults && index >= 0; ++count, --index) {
			File exDir = jobsStatusFiles[index];

			NodeStatus status;
			try {
				status = NodeStatus.createNodeFromObject(JSONUtils.parseJSONFromFile(exDir));
				if (status != null) {
					nodes.add(status);
				}
			} catch (IOException e) {
				throw new ExecutorManagerException(e.getMessage());
			}
		}

		return jobsStatusFiles.length;
	}
	
	public int getExecutableFlows(String projectId, String flowId, int from, int maxResults, List<ExecutableFlow> output) {
		String projectPath = projectId + File.separator + flowId;
		File flowProjectPath = new File(basePath, projectPath);

		if (!flowProjectPath.exists()) {
			return 0;
		}
		
		File[] executionFiles = flowProjectPath.listFiles();
		if (executionFiles.length == 0 || from >= executionFiles.length) {
			return 0;
		}
		
		// Sorts the file in ascending order, so we read the list backwards.
		Arrays.sort(executionFiles);
		int index = (executionFiles.length - from - 1);
		
		for (int count = 0; count < maxResults && index >= 0; ++count, --index) {
			File exDir = executionFiles[index];
			try {
				ExecutableFlow flow = ExecutableFlowLoader.loadExecutableFlowFromDir(exDir);
				output.add(flow);
			}
			catch (ExecutorManagerException e) {
				logger.error("Skipping loading " + exDir + ". Couldn't load execution.", e);
			}
		}
		
		return executionFiles.length;
	}
	
	public List<ExecutableFlow> getRecentlyFinishedFlows() {
		ArrayList<ExecutableFlow> flows = new ArrayList<ExecutableFlow>();
		for(Element elem : recentFlowsCache.getAll(recentFlowsCache.getKeys()).values()) {
			if (elem != null) {
				Object obj = elem.getObjectValue();
				flows.add((ExecutableFlow)obj);
			}
		}
		
		return flows;
	}
	
	public List<ExecutableFlow> getFlowRunningFlows(String projectId, String flowId) {
		ArrayList<ExecutableFlow> list = new ArrayList<ExecutableFlow>();
		for (ExecutableFlow flow: getRunningFlows()) {
			if (flow.getProjectId().equals(projectId) && flow.getFlowId().equals(flowId)) {
				list.add(flow);
			}
		}
		
		return list;
	}
	
	public List<ExecutableFlow> getRunningFlows() {
		ArrayList<ExecutableFlow> execFlows = new ArrayList<ExecutableFlow>(runningFlows.values());
		return execFlows;
	}

	public List<ExecutionReference> getFlowHistory(String regexPattern, int numResults, int skip) {
		ArrayList<ExecutionReference> searchFlows = new ArrayList<ExecutionReference>();

		Pattern pattern;
		try {
			pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
		} catch (PatternSyntaxException e) {
			logger.error("Bad regex pattern " + regexPattern);
			return searchFlows;
		}
		
		for (ExecutableFlow flow: runningFlows.values()) {
			if (skip > 0) {
				skip--;
			}
			else {
				ExecutionReference ref = new ExecutionReference(flow);
				if(pattern.matcher(ref.getFlowId()).find() ) {
					searchFlows.add(ref);
				}
				if (searchFlows.size() == numResults) {
					Collections.sort(searchFlows);
					return searchFlows;
				}
			}
		}
		
		File archivePath = new File(basePath, ARCHIVE_DIR);
		if (!archivePath.exists()) {
			return searchFlows;
		}
		
		File[] archivePartitionsDir = archivePath.listFiles();
		Arrays.sort(archivePartitionsDir, new Comparator<File>() {
			@Override
			public int compare(File arg0, File arg1) {
				return arg1.getName().compareTo(arg0.getName());
			}});

		for (File archivePartition: archivePartitionsDir) {
			File[] listArchivePartitions = archivePartition.listFiles();
			if (skip > listArchivePartitions.length) {
				skip -= listArchivePartitions.length;
				continue;
			}
			
			Arrays.sort(listArchivePartitions);
			for (int i = listArchivePartitions.length - 1; i >= 0; --i) {
				if (skip > 0) {
					skip--;
				}
				else {
					try {
						ExecutionReference ref = ExecutionReference.readFromDirectory(listArchivePartitions[i]);
						if(pattern.matcher(ref.getFlowId()).find() ) {
							searchFlows.add(ref);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					if (searchFlows.size() == numResults) {
						Collections.sort(searchFlows);
						return searchFlows;
					}
				}
			}
		}
		
		Collections.sort(searchFlows);
		return searchFlows;
	}
	
	private void loadActiveExecutions() throws IOException, ExecutorManagerException {
		File activeFlows = new File(basePath, ACTIVE_DIR);
		File[] activeFlowDirs = activeFlows.listFiles();
		if (activeFlowDirs == null) {
			return;
		}
		
		for (File activeFlowDir: activeFlowDirs) {
			if (activeFlowDir.isDirectory()) {
				ExecutionReference reference = ExecutionReference.readFromDirectory(activeFlowDir);
				if (reference.getExecutorUrl() == null) {
					reference.setExecutorPort(portNumber);
					reference.setExecutorUrl(url);
				}
				
				ExecutableFlow flow = this.getFlowFromReference(reference);
				if (flow == null) {
					logger.error("Flow " + reference.getExecId() + " not found.");
				}
				reference.setLastCheckedTime(System.currentTimeMillis());

				if (flow != null) {
					runningReference.put(reference.getExecId(), reference);
					runningFlows.put(flow.getExecutionId(), flow);
				}
			}
			else {
				logger.info("Path " + activeFlowDir + " not a directory.");
			}
		}
	}
	
	public synchronized ExecutableFlow createExecutableFlow(Flow flow) {
		String projectId = flow.getProjectId();
		String flowId = flow.getId();
		
		String flowExecutionDir = projectId + File.separator + flowId;
		File projectExecutionDir = new File(basePath, flowExecutionDir);

		// Find execution
		File executionDir;
		String executionId;

		int count = 0;

		do {
			String countString = String.format("%05d", count);
			executionId = String.valueOf(System.currentTimeMillis()) + "." + countString + "." + projectId + "." + flowId;
			executionDir = new File(projectExecutionDir, executionId);
			count++;
		}
		while(executionDir.exists());
		
		ExecutableFlow exFlow = new ExecutableFlow(executionId, flow);
		return exFlow;
	}
	
	public synchronized void setupExecutableFlow(ExecutableFlow exflow) throws ExecutorManagerException {
		String executionId = exflow.getExecutionId();

		File projectDir = new File(basePath, exflow.getProjectId());
		String executionDir = exflow.getFlowId() + File.separator + executionId;
		File executionPath = new File(projectDir, executionDir);
		if (executionPath.exists()) {
			throw new ExecutorManagerException("Execution path " + executionPath + " exists. Probably a simultaneous execution.");
		}
		
		executionPath.mkdirs();
		
		// create job reference dir
		File jobExecutionDir = new File(projectDir, JOB_EXECUTION_DIR);
		jobExecutionDir.mkdirs();
		
		exflow.setExecutionPath(executionPath.getPath());
	}

	public synchronized ExecutableFlow getExecutableFlow(String executionId) throws ExecutorManagerException {
		ExecutableFlow flow = runningFlows.get(executionId);
		if (flow != null) {
			return flow;
		}
		
		String[] split = executionId.split("\\.");
		// get project file from split.
		String projectId = split[2];
		File projectPath = new File(basePath, projectId);
		if (projectPath.exists()) {
			// Execution path sometimes looks like timestamp.count.projectId.flowId. Except flowId could have ..
			String flowId = executionId.substring(split[0].length() + split[1].length() + projectId.length() + 3);
			File flowPath = new File(projectPath, flowId);
			if (flowPath.exists()) {
				File executionPath = new File(flowPath, executionId);
				if (executionPath.exists()) {
					return ExecutableFlowLoader.loadExecutableFlowFromDir(executionPath);
				}
			}
		}
		
		// Check active
		File baseActiveDir = new File(basePath, ACTIVE_DIR);
		File referenceDir = new File(baseActiveDir, executionId);
		
		if (!referenceDir.exists()) {
			// Find the partition it would be in and search.
			String partition = getExecutableReferencePartition(executionId);
			
			File baseArchiveDir = new File(basePath, ARCHIVE_DIR + File.separator + partition);
			referenceDir = new File(baseArchiveDir, executionId);
			if (!referenceDir.exists()) {
				throw new ExecutorManagerException("Execution id '" + executionId + "' not found. Searching " + referenceDir);
			}
		}

		ExecutionReference reference = null;
		try {
			reference = ExecutionReference.readFromDirectory(referenceDir);
		} catch (IOException e) {
			throw new ExecutorManagerException(e.getMessage(), e);
		}

		
		return getFlowFromReference(reference);
	}
	
	private ExecutableFlow getFlowFromReference(ExecutionReference reference) throws ExecutorManagerException {
		File executionPath = new File(reference.getExecPath());
		if (executionPath.exists()) {
			return ExecutableFlowLoader.loadExecutableFlowFromDir(executionPath);
		}
		return null;
	}
	
	private synchronized ExecutionReference addActiveExecutionReference(ExecutableFlow flow) throws ExecutorManagerException {
		File activeDirectory = new File(basePath, ACTIVE_DIR);
		if (!activeDirectory.exists()) {
			activeDirectory.mkdirs();
		}

		// Create execution reference directory
		File referenceDir = new File(activeDirectory, flow.getExecutionId());
		referenceDir.mkdirs();

		// We don't really need to save the reference, 
		ExecutionReference reference = new ExecutionReference(flow);
		reference.setExecutorUrl(url);
		reference.setExecutorPort(portNumber);
		try {
			reference.writeToDirectory(referenceDir);
		} catch (IOException e) {
			throw new ExecutorManagerException("Couldn't write execution to directory.", e);
		}

		return reference;
	}
	
	private void forceFlowFailure(ExecutableFlow exFlow) throws ExecutorManagerException {
		String logFileName = "_flow." + exFlow.getExecutionId() + ".log";
		File executionDir = new File(exFlow.getExecutionPath());
		
		// Add a marker to the directory as an indicator to zombie processes that this is off limits.
		File forcedFailed = new File(executionDir, ConnectorParams.FORCED_FAILED_MARKER);
		if (!forcedFailed.exists()) {
			try {
				forcedFailed.createNewFile();
			} catch (IOException e) {
				logger.error("Error creating failed marker in execution directory",e);
			}
		}
		
		// Load last update
		updateFlowFromFile(exFlow);

		// Return if already finished.
		if (exFlow.getStatus() == Status.FAILED || 
			exFlow.getStatus() == Status.SUCCEEDED || 
			exFlow.getStatus() == Status.KILLED) {
			return;
		}

		// Finish log file
		File logFile = new File(executionDir, logFileName);
		if (logFile.exists()) {
			// Finally add to log
			FileWriter writer = null;
			try {
				writer = new FileWriter(logFile, true);
				writer.append("\n" + System.currentTimeMillis() + " ERROR: Can't reach executor. Killing Flow!!!!");
			} catch (IOException e) {
				if (writer != null) {
					try {
						writer.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
		}

		// We mark every untouched job with KILLED, and running jobs with FAILED.
		long time = System.currentTimeMillis();
		for (ExecutableNode node: exFlow.getExecutableNodes()) {
			switch(node.getStatus()) {
				case SUCCEEDED:
				case FAILED:
				case KILLED:
				case SKIPPED:
				case DISABLED:
					continue;
				case UNKNOWN:
				case READY:
					node.setStatus(Status.KILLED);
					break;
				default:
					node.setStatus(Status.FAILED);
					break;
			}

			if (node.getStartTime() == -1) {
				node.setStartTime(time);
			}
			if (node.getEndTime() == -1) {
				node.setEndTime(time);
			}
		}

		if (exFlow.getEndTime() == -1) {
			exFlow.setEndTime(time);
		}
		
		exFlow.setStatus(Status.FAILED);
	}

	@SuppressWarnings("unchecked")
	public void cancelFlow(String execId, String user) throws ExecutorManagerException {
		logger.info("Calling cancel");
		ExecutionReference reference = runningReference.get(execId);
		if (reference == null) {
			throw new ExecutorManagerException("Execution " + execId + " not running.");
		}
		
		Map<String, Object> respObj = null;
		try {
			String response = callExecutorServer(reference, ConnectorParams.CANCEL_ACTION);
			respObj = (Map<String, Object>)JSONUtils.parseJSONFromString(response);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExecutorManagerException("Error cancelling flow.", e);
		}
		
		if (respObj.containsKey(ConnectorParams.RESPONSE_ERROR)) {
			throw new ExecutorManagerException((String)respObj.get(ConnectorParams.RESPONSE_ERROR));
		}
	}

	@SuppressWarnings("unchecked")
	public void pauseFlow(String execId, String user) throws ExecutorManagerException {
		logger.info("Calling pause");
		ExecutionReference reference = runningReference.get(execId);
		if (reference == null) {
			throw new ExecutorManagerException("Execution " + execId + " not running.");
		}
		
		Map<String, Object> respObj = null;
		try {
			String response = callExecutorServer(reference, ConnectorParams.PAUSE_ACTION);
			respObj = (Map<String, Object>)JSONUtils.parseJSONFromString(response);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExecutorManagerException("Error pausing flow.", e);
		}

		if (respObj.containsKey(ConnectorParams.RESPONSE_ERROR)) {
			throw new ExecutorManagerException((String)respObj.get(ConnectorParams.RESPONSE_ERROR));
		}
	}

	@SuppressWarnings("unchecked")
	public void resumeFlow(String execId, String user) throws ExecutorManagerException {
		logger.info("Calling resume");
		ExecutionReference reference = runningReference.get(execId);
		if (reference == null) {
			throw new ExecutorManagerException("Execution " + execId + " not running.");
		}
		
		Map<String, Object> respObj = null;
		try {
			String response = callExecutorServer(reference, ConnectorParams.RESUME_ACTION);
			respObj = (Map<String, Object>)JSONUtils.parseJSONFromString(response);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExecutorManagerException("Error resuming flow.", e);
		}

		if (respObj.containsKey(ConnectorParams.RESPONSE_ERROR)) {
			throw new ExecutorManagerException((String)respObj.get(ConnectorParams.RESPONSE_ERROR));
		}
	}
	
	private synchronized String callExecutorServer(ExecutionReference reference, String action) throws IOException {
		return callExecutorServer(
				reference.getExecutorUrl(),
				reference.getExecutorPort(),
				action,
				reference.getExecId(),
				reference.getExecPath(),
				reference.userId);
	}
	
	private synchronized String callExecutorServer(String url, int port, String action, String execid, String execPath, String user) throws IOException {
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(url)
			.setPort(port)
			.setPath("/executor")
			.setParameter(ConnectorParams.SHAREDTOKEN_PARAM, token)
			.setParameter(ConnectorParams.ACTION_PARAM, action);

		if (execid != null) {
			builder.setParameter(ConnectorParams.EXECID_PARAM, execid);
		}
		
		if (user != null) {
			builder.setParameter(ConnectorParams.USER_PARAM, user);
		}
		
		if (execPath != null) {
			builder.setParameter(ConnectorParams.EXECPATH_PARAM, execPath);
		}

		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			e.printStackTrace();
			return response;
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}
		
		return response;
	}
	
	public void executeFlow(ExecutableFlow flow) throws ExecutorManagerException {
		String executionPath = flow.getExecutionPath();
		File executionDir = new File(executionPath);
		flow.setSubmitTime(System.currentTimeMillis());
		
		writeResourceFile(executionDir, flow);
		ExecutableFlowLoader.writeExecutableFlowFile(executionDir, flow, null);
		ExecutionReference reference = addActiveExecutionReference(flow);
		
		logger.info("Setting up " + flow.getExecutionId() + " for execution.");
		
		String response;
		try {
			response = callExecutorServer(reference, ConnectorParams.EXECUTE_ACTION);
			reference.setSubmitted(true);
			runningReference.put(flow.getExecutionId(), reference);
			runningFlows.put(flow.getExecutionId(), flow);
		} catch (IOException e) {
			e.printStackTrace();
			// Clean up.
			forceFlowFailure(flow);
			cleanFinishedJob(flow);
			return;
		}
		
		logger.debug("Submitted Response: " + response);

		reference.setStartTime(System.currentTimeMillis());
		synchronized(BlockObj) {
			BlockObj.notify();
		}
	}
	
	private long readLog(File logFile, Writer writer, long startChar, long maxSize) {
		if (!logFile.exists()) {
			logger.error("Execution log for " + logFile + " doesn't exist.");
			return -1;
		}
		
		BufferedReader reader = null;
		FileReader fileReader = null;
		char[] charBuffer = new char[LOG_BUFFER_READ_SIZE];
		
		try {
			fileReader = new FileReader(logFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		long charPosition = startChar;
		int charRead = 0;
		long totalCharRead = 0;
		try {
			reader = new BufferedReader(fileReader);
			reader.skip(startChar);

			do {
				charRead = reader.read(charBuffer);
				if (charRead == -1) {
					break;
				}
				totalCharRead += charRead;
				charPosition += charRead;
				writer.write(charBuffer, 0, charRead);
			} while (charRead == charBuffer.length && totalCharRead < maxSize);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return charPosition;
	}
	
	public long getExecutionJobLog(ExecutableFlow flow, String jobid, Writer writer, long startChar, long maxSize) throws ExecutorManagerException {
		String path = flow.getExecutionPath();
		File execPath = new File(path);
		if (!execPath.exists()) {
			logger.error("Execution dir for " + flow + " doesn't exist.");
			return -1;
		}
		
		String logFileName = "_job." + flow.getExecutionId() + "." + jobid + ".log";
		File logFile = new File(execPath, logFileName);
		
		if (!logFile.exists()) {
			logger.error("Execution log for " + logFile + " doesn't exist.");
			return -1;
		}

		long charPosition = readLog(logFile, writer, startChar, maxSize);
		return charPosition;
	}
	
	public long getExecutableFlowLog(ExecutableFlow flow, Writer writer, long startChar, long maxSize) throws ExecutorManagerException {
		String path = flow.getExecutionPath();
		File execPath = new File(path);
		if (!execPath.exists()) {
			logger.error("Execution dir for " + flow + " doesn't exist.");
			return -1;
		}
		
		String logFileName = "_flow." + flow.getExecutionId() + ".log";
		File flowLogFile = new File(execPath, logFileName);
		
		if (!flowLogFile.exists()) {
			logger.error("Execution log for " + flowLogFile + " doesn't exist.");
			return -1;
		}

		long charPosition = readLog(flowLogFile, writer, startChar, maxSize);
		return charPosition;
	}
	
	public void cleanupAll(ExecutableFlow exflow) throws ExecutorManagerException{
		String path = exflow.getExecutionPath();
		File executionPath = new File(path);
		if (executionPath.exists()) {
			try {
				logger.info("Deleting resource path " + executionPath);
				FileUtils.deleteDirectory(executionPath);
			} catch (IOException e) {
				throw new ExecutorManagerException(e.getMessage(), e);
			}
		}
	}
	
	private File writeResourceFile(File executionDir, ExecutableFlow flow) throws ExecutorManagerException {
		// Create a source list.
		Set<String> sourceFiles = flow.getSources();
		
		// Write out the resource files
		File resourceFile = new File(executionDir, "_" + flow.getExecutionId() + ".resources");
		if (resourceFile.exists()) {
			throw new ExecutorManagerException("The resource file " + resourceFile + " already exists. Race condition?");
		}
		HashMap<String, Object> resources = createResourcesList(executionDir, executionDir, sourceFiles);
		BufferedOutputStream out = null;
		try {
			logger.info("Writing resource file " + resourceFile);
			out = new BufferedOutputStream(new FileOutputStream(resourceFile));
			JSONUtils.toJSON(resources, out, true);
		} 
		catch (IOException e) {
			throw new ExecutorManagerException(e.getMessage(), e);
		}
		finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return resourceFile;
	}
	
	private HashMap<String, Object> createResourcesList(File baseDir, File dir, Set<String> sourceFiles) {
		boolean containsSource = false;
		
		HashMap<String, Object> directoryMap = new HashMap<String, Object>();
		String relative = dir.getPath().substring(baseDir.getPath().length(), dir.getPath().length());
		directoryMap.put("name", dir.getName());
		directoryMap.put("relative.path", relative);
		directoryMap.put("type", "directory");
		
		ArrayList<Object> children = new ArrayList<Object>();
		for (File file: dir.listFiles()) {
			if (file.isDirectory()) {
				HashMap<String, Object> subDir = createResourcesList(baseDir, file, sourceFiles);
				containsSource |= (Boolean)subDir.get("used.source");
				children.add(subDir);
			}
			else {
				HashMap<String, Object> subFile = new HashMap<String, Object>();
				String subFileName = file.getName();
				String subFilePath = file.getPath().substring(baseDir.getPath().length() + 1, file.getPath().length());
				boolean source =  sourceFiles.contains(subFilePath);
				containsSource |= source;
				
				subFile.put("name", subFileName);
				subFile.put("relative.path", subFilePath);
				subFile.put("type", "file");
				subFile.put("used.source", source);
				subFile.put("size", file.length());
				subFile.put("modified.date", file.lastModified());
				children.add(subFile);
			}
		}
		
		directoryMap.put("children", children);
		directoryMap.put("used.source", containsSource);
		return directoryMap;
	}
	
	@SuppressWarnings("unchecked")
	private void getDeletableResourceList(HashMap<String, Object> sourceTree, Set<String> deletableResourcePaths) {
		boolean usedSource = (Boolean)sourceTree.get("used.source");

		if (!usedSource) {
			String relativePath = (String)sourceTree.get("relative.path");
			deletableResourcePaths.add(relativePath);
		}
		else {
			List<Object> children = (List<Object>)sourceTree.get("children");
			if (children != null) {
				for (Object obj: children) {
					HashMap<String, Object> child = (HashMap<String,Object>)obj;
					getDeletableResourceList(child, deletableResourcePaths);
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public void cleanupUnusedFiles(ExecutableFlow exflow) throws ExecutorManagerException {
		String path = exflow.getExecutionPath();
		File executionPath = new File(path);
		
		String flowFilename = "_" + exflow.getExecutionId() + ".flow";
		String resourceFilename = "_" + exflow.getExecutionId() + ".resources";

		File resourceFile = new File(executionPath, resourceFilename);
		if (!resourceFile.exists()) {
			throw new ExecutorManagerException("Cleaning failed. Resource file " + flowFilename + " doesn't exist.");
		}
		
		HashSet<String> deletableResources = new HashSet<String>();
		try {
			HashMap<String, Object> resourceObj = (HashMap<String, Object>)JSONUtils.parseJSONFromFile(resourceFile);
			getDeletableResourceList(resourceObj, deletableResources);
		} catch (IOException e) {
			throw new ExecutorManagerException("Cleaning failed. Resource file " + flowFilename + " parse error.", e);
		}
		
		logger.info("Deleting resources for " + exflow.getFlowId());
		for (String deletable: deletableResources) {
			File deleteFile = new File(executionPath, deletable);
			if (deleteFile.exists()) {
				if (deleteFile.isDirectory()) {
					try {
						FileUtils.deleteDirectory(deleteFile);
					} catch (IOException e) {
						logger.error("Failed deleting '" + deleteFile + "'", e);
					}
				}
				else {
					if(!deleteFile.delete()) {
						logger.error("Deleting of resource file '" + deleteFile + "' failed.");
					}
				}
			}
			else {
				logger.error("Failed deleting '" + deleteFile + "'. File doesn't exist.");
			}
		}
	}
	
	private String getExecutableReferencePartition(String execID) {
		// We're partitioning the archive by the first part of the id, which should be a timestamp.
		// Then we're taking a substring of length - 6 to lop off the bottom 5 digits effectively partitioning
		// by 100000 millisec. We do this to have quicker searchs by pulling partitions, not full directories.
		int index = execID.indexOf('.');
		return execID.substring(0, index - 8);
	}

	private void cleanExecutionReferenceJob(ExecutionReference reference) throws ExecutorManagerException {
		// Write final file
		String exId = reference.getExecId();
		String activeReferencePath = ACTIVE_DIR + File.separator + exId; 
		File activeDirectory = new File(basePath, activeReferencePath);
		if (!activeDirectory.exists()) {
			logger.error("WTF!! Active reference " + activeDirectory + " directory doesn't exist.");
			throw new ExecutorManagerException("Active reference " + activeDirectory + " doesn't exists.");
		}
		
		String partitionVal = getExecutableReferencePartition(exId);
		
		String archiveDatePartition = ARCHIVE_DIR + File.separator + partitionVal;
		File archivePartitionDir = new File(basePath, archiveDatePartition);
		if (!archivePartitionDir.exists()) {
			archivePartitionDir.mkdirs();
		}

		File archiveDirectory = new File(archivePartitionDir, exId);
		if (archiveDirectory.exists()) {
			logger.error("Archive reference already exists. Cleaning up.");
			try {
				FileUtils.deleteDirectory(activeDirectory);
			} catch (IOException e) {
				logger.error(e);
			}
			
			return;
		}
		
		// Make new archive dir
		if (!archiveDirectory.mkdirs()) {
			throw new ExecutorManagerException("Cannot create " + archiveDirectory);
		}
		
		try {
			reference.writeToDirectory(archiveDirectory);
		} catch (IOException e) {
			throw new ExecutorManagerException("Couldn't write execution to directory.", e);
		}
		
		// Move file.
		try {
			FileUtils.deleteDirectory(activeDirectory);
		} catch (IOException e) {
			throw new ExecutorManagerException("Cannot cleanup active directory " + activeDirectory);
		}
	
		runningReference.remove(exId);
	}
	
	private void cleanFinishedJob(ExecutableFlow exFlow) throws ExecutorManagerException {
		// Write final file
		int updateNum = exFlow.getUpdateNumber();
		updateNum++;
		String exId = exFlow.getExecutionId();
		ExecutableFlowLoader.writeExecutableFlowFile(new File(exFlow.getExecutionPath()), exFlow, updateNum);

		// Clean up reference
		ExecutionReference reference = runningReference.get(exId);
		if (reference != null) {
			reference.setStartTime(exFlow.getStartTime());
			reference.setEndTime(exFlow.getEndTime());
			reference.setStatus(exFlow.getStatus());
			cleanExecutionReferenceJob(reference);
		}
		
		runningFlows.remove(exFlow.getExecutionId());
		recentFlowsCache.put(new Element(exFlow.getExecutionId(), exFlow));
		cleanupUnusedFiles(exFlow);
	}
	
	private class ExecutingManagerUpdaterThread extends Thread {
		private boolean shutdown = false;
		private int waitTimeIdleMs = 1000;
		private int waitTimeMs = 100;
		
		@SuppressWarnings("unchecked")
		public void run() {
			while(!shutdown) {
				for (ExecutionReference reference: runningReference.values()) {
					// Don't do anything if not submitted
					if (!reference.isSubmitted()) {
						continue;
					}
					
					String execId = reference.getExecId();
					ExecutableFlow flow = runningFlows.get(execId);
					if (flow != null) {
						// Why doesn't flow exist?
					}
					
					File executionDir = new File(flow.getExecutionPath());
					// The execution dir doesn't exist. So we clean up.
					if (!executionDir.exists()) {
						logger.error("WTF!! Execution dir " + executionDir + " doesn't exist!");
						// Removing reference.
						reference.setStatus(Status.FAILED);
						try {
							cleanExecutionReferenceJob(reference);
						} catch (ExecutorManagerException e) {
							logger.error("The execution dir " + executionDir.getPath() + " doesn't exist for " + reference.toRefString(), e);
						}
						continue;
					}
					
					// Get status from the server. If the server response are errors, than we clean up after 30 seconds of errors.
					HashMap<String,Object> map = null;
					try {
						String string = callExecutorServer(reference, ConnectorParams.STATUS_ACTION);
						map = (HashMap<String,Object>)JSONUtils.parseJSONFromString(string);
						reference.setLastCheckedTime(System.currentTimeMillis());
					} catch (IOException e) {
						if (System.currentTimeMillis() - reference.getLastCheckedTime() > ACCESS_ERROR_THRESHOLD_MS) {
							logger.error("Error: Can't connect to server." + reference.toRefString() + ". Might be dead. Cleaning up project.", e);
							// Cleanup. Since we haven't seen anyone.
							try {
								forceFlowFailure(flow);
								cleanFinishedJob(flow);
							} catch (ExecutorManagerException e1) {
								logger.error("Foreced Fail: Error while cleaning up flow and job. " + reference.toRefString(), e1);
							}
							
							continue;
						}
					}
					
					// If the response from the server is an error, we print out the response and continue.
					String error = (String)map.get(ConnectorParams.RESPONSE_ERROR);
					if (error != null) {
						logger.error("Server status response for " + reference.toRefString() + " was an error: " + error);
						continue;
					}
					
					// If not found, then we clean up.
					String statusStr = (String)map.get(ConnectorParams.STATUS_PARAM);
					boolean forceFail = false;
					if (statusStr.equals(ConnectorParams.RESPONSE_NOTFOUND)) {
						logger.info("Server status response for " + reference.toRefString() + " was 'notfound'. Cleaning up");
						try {
							updateFlowFromFile(flow);
							forceFail = true;
						} catch (ExecutorManagerException e) {
							logger.error("Error updating flow status " + flow.getExecutionId() + " from file.", e);
							continue;
						}
					}
					else {
						// It's found, so we check the status.
						long time = JSONUtils.getLongFromObject(map.get(ConnectorParams.RESPONSE_UPDATETIME));
						
						if (time > flow.getUpdateTime()) {
							try {
								updateFlowFromFile(flow);
								// Update reference
								reference.setStartTime(flow.getStartTime());
								reference.setEndTime(flow.getEndTime());
								reference.setStatus(flow.getStatus());
							} catch (ExecutorManagerException e) {
								logger.error("Error updating flow status " + flow.getExecutionId() + " from file.", e);
							}
							
							flow.setUpdateTime(time);
						}
					}

					switch(flow.getStatus()) {
						case SUCCEEDED:
						case FAILED:
						case KILLED:
							try {
								cleanFinishedJob(flow);
							} catch (ExecutorManagerException e) {
								logger.error("Error while cleaning up flow and job. " + reference.toRefString(), e);
							}
							
							break;
						default:{
							// We force the failure.
							if (forceFail) {
								try {
									forceFlowFailure(flow);
									cleanFinishedJob(flow);
								} catch (ExecutorManagerException e) {
									logger.error("Foreced Fail: Error while cleaning up flow and job. " + reference.toRefString(), e);
								}
							}
						}
					}
				}

				// Change to rotating queue?
				synchronized(BlockObj) {
					try {
						if (runningReference.isEmpty()) {
							BlockObj.wait(waitTimeIdleMs);
						}
						else {
							BlockObj.wait(waitTimeMs);
						}
					} catch (InterruptedException e) {
					}
				}
			}
		}
	}
	
	private void updateFlowFromFile(ExecutableFlow exFlow) throws ExecutorManagerException {
		File executionDir = new File(exFlow.getExecutionPath());
		if (ExecutableFlowLoader.updateFlowStatusFromFile(executionDir, exFlow, true)) {
			// Move all the static directories if the update has changed.
			File statusDir = new File(basePath, exFlow.getProjectId() + File.separator + JOB_EXECUTION_DIR);
			ExecutableFlowLoader.moveJobStatusFiles(executionDir, statusDir);
		}
	}
	
	/**
	 * Reference to a Flow Execution.
	 * It allows us to search for Flow and Project with only the Execution Id, it references
	 * a file in the execution directories.
	 */
	public static class ExecutionReference implements Comparable<ExecutionReference> {
		private String execId;
		private String projectId;
		private String flowId;
		private String userId;
		private String execPath;
		private long startTime;
		private long endTime;
		private Status status;
		private String executorUrl;
		
		private int executorPort;
		private boolean isSubmitted = true;
		private long lastCheckedTime = -1;
		
		public ExecutionReference() {
			this.lastCheckedTime = System.currentTimeMillis();
		}
		
		public ExecutionReference(ExecutableFlow flow) {
			this();
			this.execId = flow.getExecutionId();
			this.projectId = flow.getProjectId();
			this.flowId = flow.getFlowId();
			this.userId = flow.getSubmitUser();
			this.execPath = flow.getExecutionPath();
			
			this.startTime = flow.getStartTime();
			this.endTime = flow.getEndTime();
			this.status = flow.getStatus();
			this.isSubmitted = false;
		}
		
		private Object toObject() {
			HashMap<String, Object> obj = new HashMap<String, Object>();
			obj.put("execId", execId);
			obj.put("projectId", projectId);
			obj.put("flowId", flowId);
			obj.put("userId", userId);
			obj.put("execPath", execPath);
			obj.put("startTime", startTime);
			obj.put("endTime", endTime);
			obj.put("status", status);
			obj.put("executorUrl", executorUrl);
			obj.put("executorPort", executorPort);
			return obj;
		}
		
		public void writeToDirectory(File directory) throws IOException {
			File file = new File(directory, "execution.json");
			if (!file.exists()) {
				JSONUtils.toJSON(this.toObject(), file);
			}
		}
		
		public static ExecutionReference readFromDirectory(File directory) throws IOException {
			File file = new File(directory, "execution.json");
			if (!file.exists()) {
				throw new IOException("Execution file execution.json does not exist.");
			}
			
			@SuppressWarnings("unchecked")
			HashMap<String, Object> obj = (HashMap<String, Object>)JSONUtils.parseJSONFromFile(file);
			ExecutionReference reference = new ExecutionReference();
			reference.execId = (String)obj.get("execId");
			reference.projectId = (String)obj.get("projectId");
			reference.flowId = (String)obj.get("flowId");
			reference.userId = (String)obj.get("userId");
			reference.execPath = (String)obj.get("execPath");
			reference.startTime = JSONUtils.getLongFromObject(obj.get("startTime"));
			reference.endTime = JSONUtils.getLongFromObject(obj.get("endTime"));
			reference.status = Status.valueOf((String)obj.get("status"));
			
			if (obj.containsKey("executorUrl")) {
				reference.executorUrl = (String)obj.get("executorUrl");
				reference.executorPort = (Integer)obj.get("executorPort");
			}

			return reference;
		}
		
		public String toRefString() {
			return execId + ":" + executorUrl + ":" + executorPort;
		}
		
		public String getExecId() {
			return execId;
		}
		
		public String getProjectId() {
			return projectId;
		}
		
		public String getFlowId() {
			return flowId;
		}
		
		public String getUserId() {
			return userId;
		}
		
		public String getExecPath() {
			return execPath;
		}

		public Long getStartTime() {
			return startTime;
		}

		public void setStartTime(Long startTime) {
			this.startTime = startTime;
		}

		public Long getEndTime() {
			return endTime;
		}

		public void setEndTime(Long endTime) {
			this.endTime = endTime;
		}

		@Override
		public int compareTo(ExecutionReference arg0) {
			return arg0.getExecId().compareTo(execId);
		}
		
		public Status getStatus() {
			return status;
		}

		public void setStatus(Status status) {
			this.status = status;
		}

		public String getExecutorUrl() {
			return executorUrl;
		}

		public void setExecutorUrl(String executorUrl) {
			this.executorUrl = executorUrl;
		}

		public int getExecutorPort() {
			return executorPort;
		}

		public void setExecutorPort(int port) {
			this.executorPort = port;
		}

		public boolean isSubmitted() {
			return isSubmitted;
		}

		public void setSubmitted(boolean isSubmitted) {
			this.isSubmitted = isSubmitted;
		}

		public long getLastCheckedTime() {
			return lastCheckedTime;
		}

		public void setLastCheckedTime(long lastCheckedTime) {
			this.lastCheckedTime = lastCheckedTime;
		}
	}

}
