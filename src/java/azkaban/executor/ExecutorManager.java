package azkaban.executor;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
	private static Logger logger = Logger.getLogger(ExecutorManager.class);
	// 30 seconds of retry before failure.
	private static final long ACCESS_ERROR_THRESHOLD = 30000;
	private static final int UPDATE_THREAD_MS = 1000;

	// log read buffer.
	private static final int LOG_BUFFER_READ_SIZE = 10*1024;
	
	private File basePath;

	private AtomicInteger counter = new AtomicInteger();
	private String token;
	private int portNumber;
	private String url = "localhost";
	
	private ConcurrentHashMap<String, ExecutableFlow> runningFlows = new ConcurrentHashMap<String, ExecutableFlow>();
	
	private CacheManager manager = CacheManager.create();
	private Cache recentFlowsCache;
	private static final int LIVE_SECONDS = 600;
	
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
		
		ExecutingManagerUpdaterThread executingManager = new ExecutingManagerUpdaterThread();
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

	public List<ExecutionReference> getFlowHistory(int numResults, int skip) {
		ArrayList<ExecutionReference> searchFlows = new ArrayList<ExecutionReference>();

		for (ExecutableFlow flow: runningFlows.values()) {
			if (skip > 0) {
				skip--;
			}
			else {
				ExecutionReference ref = new ExecutionReference(flow);
				searchFlows.add(ref);
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
				// TODO Auto-generated method stub
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
						searchFlows.add(ref);
					} catch (IOException e) {
						// TODO Auto-generated catch block
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
				
				ExecutableFlow flow = this.getFlowFromReference(reference);
				if (flow == null) {
					logger.error("Flow " + reference.getExecId() + " not found.");
				}
				flow.setLastCheckedTime(System.currentTimeMillis());
				flow.setSubmitted(true);
				if (flow != null) {
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
		int count = counter.getAndIncrement() % 100000;
		String countString = String.format("%05d", count);
		do {
			executionId = String.valueOf(System.currentTimeMillis()) + "." + countString + "." + flowId;
			executionDir = new File(projectExecutionDir, executionId);
		}
		while(executionDir.exists());
		
		ExecutableFlow exFlow = new ExecutableFlow(executionId, flow);
		return exFlow;
	}
	
	public synchronized void setupExecutableFlow(ExecutableFlow exflow) throws ExecutorManagerException {
		String executionId = exflow.getExecutionId();

		String projectFlowDir = exflow.getProjectId() + File.separator + exflow.getFlowId() + File.separator + executionId;
		File executionPath = new File(basePath, projectFlowDir);
		if (executionPath.exists()) {
			throw new ExecutorManagerException("Execution path " + executionPath + " exists. Probably a simultaneous execution.");
		}
		
		executionPath.mkdirs();
		exflow.setExecutionPath(executionPath.getPath());
	}

	public synchronized ExecutableFlow getExecutableFlow(String executionId) throws ExecutorManagerException {
		ExecutableFlow flow = runningFlows.get(executionId);
		if (flow != null) {
			return flow;
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
	
	private synchronized void addActiveExecutionReference(ExecutableFlow flow) throws ExecutorManagerException {
		File activeDirectory = new File(basePath, ACTIVE_DIR);
		if (!activeDirectory.exists()) {
			activeDirectory.mkdirs();
		}

		// Create execution reference directory
		File referenceDir = new File(activeDirectory, flow.getExecutionId());
		referenceDir.mkdirs();

		// We don't really need to save the reference, 
		ExecutionReference reference = new ExecutionReference(flow);
		try {
			reference.writeToDirectory(referenceDir);
		} catch (IOException e) {
			throw new ExecutorManagerException("Couldn't write execution to directory.", e);
		}
		runningFlows.put(flow.getExecutionId(), flow);
	}
	
	public void cancelFlow(ExecutableFlow flow, String user) throws ExecutorManagerException {
		logger.info("Calling cancel");
		String response = null;
		try {
			response = callExecutionServer("cancel", flow, user);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExecutorManagerException("Error cancelling flow.", e);
		}
	}
	
	public void pauseFlow(ExecutableFlow flow, String user) throws ExecutorManagerException {
		logger.info("Calling pause");
		String response = null;
		try {
			response = callExecutionServer("pause", flow, user);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExecutorManagerException("Error cancelling flow.", e);
		}
	}
	
	public void resumeFlow(ExecutableFlow flow, String user) throws ExecutorManagerException {
		logger.info("Calling resume");
		String response = null;
		try {
			response = callExecutionServer("resume", flow, user);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExecutorManagerException("Error cancelling flow.", e);
		}

	}
	
	private String callExecutionServer(String action, ExecutableFlow flow) throws IOException{
		return callExecutionServer(action, flow, null);
	}
	
	private String callExecutionServer(String action, ExecutableFlow flow, String user) throws IOException{
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(url)
			.setPort(portNumber)
			.setPath("/executor")
			.setParameter("sharedToken", token)
			.setParameter("action", action)
			.setParameter("execid", flow.getExecutionId())
			.setParameter("execpath", flow.getExecutionPath());
		
		if (user != null) {
			builder.setParameter("user", user);
		}
		
		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		logger.info("Remotely querying " + flow.getExecutionId() + " for status.");
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			flow.setStatus(ExecutableFlow.Status.FAILED);
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
		addActiveExecutionReference(flow);
		flow.setLastCheckedTime(System.currentTimeMillis());
		runningFlows.put(flow.getExecutionId(), flow);
		
		logger.info("Setting up " + flow.getExecutionId() + " for execution.");
		
		String response;
		try {
			response = callExecutionServer("execute", flow);
		} catch (IOException e) {
			e.printStackTrace();
			flow.setStatus(ExecutableFlow.Status.FAILED);
			return;
		}
		
		logger.debug("Submitted Response: " + response);
		flow.setLastCheckedTime(System.currentTimeMillis());
		flow.setSubmitted(true);
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
					// TODO Auto-generated catch block
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
	
	private void cleanFinishedJob(ExecutableFlow exFlow) throws ExecutorManagerException {
		
		// Write final file
		int updateNum = exFlow.getUpdateNumber();
		updateNum++;
		ExecutableFlowLoader.writeExecutableFlowFile(new File(exFlow.getExecutionPath()), exFlow, updateNum);
		
		String activeReferencePath = ACTIVE_DIR + File.separator + exFlow.getExecutionId(); 
		File activeDirectory = new File(basePath, activeReferencePath);
		if (!activeDirectory.exists()) {
			logger.error("WTF!! Active reference " + activeDirectory + " directory doesn't exist.");
			throw new ExecutorManagerException("Active reference " + activeDirectory + " doesn't exists.");
		}
		
		String exId = exFlow.getExecutionId();
		String partitionVal = getExecutableReferencePartition(exId);
		
		String archiveDatePartition = ARCHIVE_DIR + File.separator + partitionVal;
		File archivePartitionDir = new File(basePath, archiveDatePartition);
		if (!archivePartitionDir.exists()) {
			archivePartitionDir.mkdirs();
		}

		File archiveDirectory = new File(archivePartitionDir, exFlow.getExecutionId());
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
		
		ExecutionReference reference = new ExecutionReference(exFlow);
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
		
		runningFlows.remove(exFlow.getExecutionId());
		recentFlowsCache.put(new Element(exFlow.getExecutionId(), exFlow));
		cleanupUnusedFiles(exFlow);
	}
	
	/**
	 * Thread that polls the executor for executing jobs.
	 * It is also cleans up the flow execution files after it's done.
	 */
	private class ExecutingManagerUpdaterThread extends Thread {
		private boolean shutdown = false;
		private int updateTimeMs = UPDATE_THREAD_MS;
		public void run() {
			while (!shutdown) {
				ArrayList<ExecutableFlow> flows = new ArrayList<ExecutableFlow>(runningFlows.values());
				for(ExecutableFlow exFlow : flows) {
					if (!exFlow.isSubmitted()) {
						continue;
					}
					
					File executionDir = new File(exFlow.getExecutionPath());
					
					if (!executionDir.exists()) {
						logger.error("WTF!! Execution dir " + executionDir + " doesn't exist!");
						// @TODO probably should handle this error case somehow. Cleanup?
						continue;
					}

					// Query the executor service to see if the item is running.
					String responseString = null;
					try {
						responseString = callExecutionServer("status", exFlow);
					} catch (IOException e) {
						e.printStackTrace();
						// Connection issue. Backoff 1 sec.
						synchronized(this) {
							try {
								wait(1000);
							} catch (InterruptedException ie) {
							}
						}
						continue;
					}
					catch (Exception e) {
						e.printStackTrace();
					}
					
					Object executorResponseObj;
					try {
						executorResponseObj = JSONUtils.parseJSONFromString(responseString);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						continue;
					}
					
					@SuppressWarnings("unchecked")
					HashMap<String, Object> response = (HashMap<String, Object>)executorResponseObj;
					String status = (String)response.get("status");
					
					try {
						ExecutableFlowLoader.updateFlowStatusFromFile(executionDir, exFlow, true);
					} catch (ExecutorManagerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						continue;
					}

					// If it's completed, and not running, we clean up.
					if (exFlow.getStatus() == Status.SUCCEEDED || exFlow.getStatus() == Status.FAILED || exFlow.getStatus() == Status.KILLED) {
						if (status.equals("notfound")) {
							// Cleanup
							logger.info("Flow " + exFlow.getExecutionId() + " has succeeded. Cleaning Up.");
							try {
								ExecutableFlowLoader.updateFlowStatusFromFile(executionDir, exFlow, true);
								cleanFinishedJob(exFlow);						
							} catch (ExecutorManagerException e) {
								e.printStackTrace();
								continue;
							}
							exFlow.setLastCheckedTime(System.currentTimeMillis());
						}
						else {
							logger.error("Flow " + exFlow.getExecutionId() + " has succeeded, but the Executor says its still running with msg: " + status);
							if (System.currentTimeMillis() - exFlow.getLastCheckedTime() > ACCESS_ERROR_THRESHOLD) {
								exFlow.setStatus(Status.FAILED);
								exFlow.setEndTime(System.currentTimeMillis());
								logger.error("It's been " + ACCESS_ERROR_THRESHOLD + " ms since last update. Auto-failing the job.");
							}
						}
					}
					else {
						// If it's not finished, and not running, we will fail it and clean up.
						if (status.equals("notfound")) {
							logger.error("Flow " + exFlow.getExecutionId() + " is running, but the Executor can't find it.");
							if (System.currentTimeMillis() - exFlow.getLastCheckedTime() > ACCESS_ERROR_THRESHOLD) {
								exFlow.setStatus(Status.FAILED);
								exFlow.setEndTime(System.currentTimeMillis());
								logger.error("It's been " + ACCESS_ERROR_THRESHOLD + " ms since last update. Auto-failing the job.");
							}
						}
						else {
							exFlow.setLastCheckedTime(System.currentTimeMillis());
						}
					}
					
					synchronized(this) {
						try {
							wait(updateTimeMs);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
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

		public ExecutionReference() {
		}
		
		public ExecutionReference(ExecutableFlow flow) {
			this.execId = flow.getExecutionId();
			this.projectId = flow.getProjectId();
			this.flowId = flow.getFlowId();
			this.userId = flow.getSubmitUser();
			this.execPath = flow.getExecutionPath();
			
			this.startTime = flow.getStartTime();
			this.endTime = flow.getEndTime();
			this.status = flow.getStatus();
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
			
			HashMap<String, Object> obj = (HashMap<String, Object>)JSONUtils.parseJSONFromFile(file);
			ExecutionReference reference = new ExecutionReference();
			reference.execId = (String)obj.get("execId");
			reference.projectId = (String)obj.get("projectId");
			reference.flowId = (String)obj.get("flowId");
			reference.userId = (String)obj.get("userId");
			reference.execPath = (String)obj.get("execPath");
			reference.startTime = getLongFromObject(obj.get("startTime"));
			reference.endTime = getLongFromObject(obj.get("endTime"));
			reference.status = Status.valueOf((String)obj.get("status"));
			return reference;
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
	}
	
	private static long getLongFromObject(Object obj) {
		if (obj instanceof Integer) {
			return Long.valueOf((Integer)obj);
		}
		
		return (Long)obj;
	}
}
