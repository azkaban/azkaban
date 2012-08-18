package azkaban.executor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
	private static Logger logger = Logger.getLogger(ExecutorManager.class);
	private File basePath;

	private AtomicInteger counter = new AtomicInteger();
	private String token;
	private int portNumber;
	private String url = "localhost";
	
	private ConcurrentHashMap<String, ExecutableFlow> runningFlows = new ConcurrentHashMap<String, ExecutableFlow>();
	private LinkedList<ExecutableFlow> recentlyFinished = new LinkedList<ExecutableFlow>();
	private int recentlyFinishedSize = 10;
	
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
		
		File activePath = new File(basePath, "active");
		if(!activePath.exists() && !activePath.mkdirs()) {
			throw new RuntimeException("Execution directory " + activePath + " does not exist and cannot be created.");
		}
		
		File archivePath = new File(basePath, "archive");
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

	
	private void loadActiveExecutions() throws IOException, ExecutorManagerException {
		File activeFlows = new File(basePath, "active");
		File[] activeFlowDirs = activeFlows.listFiles();
		
		for (File activeFlowDir: activeFlowDirs) {
			if (activeFlowDir.isDirectory()) {
				ExecutionReference reference = ExecutionReference.readFromDirectory(activeFlowDir);
				
				ExecutableFlow flow = this.getFlowFromReference(reference);
				if (flow != null) {
					runningFlows.put(flow.getExecutionId(), flow);
				}
				else {
					logger.error("Flow " + reference.getExecId() + " not found.");
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
		File baseActiveDir = new File(basePath, "active");
		File referenceDir = new File(baseActiveDir, executionId);
		
		if (!referenceDir.exists()) {
			File baseArchiveDir = new File(basePath, "archive");
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
		File activeDirectory = new File(basePath, "active");
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
	
	public void executeFlow(ExecutableFlow flow) throws ExecutorManagerException {
		String executionPath = flow.getExecutionPath();
		File executionDir = new File(executionPath);
		flow.setSubmitTime(System.currentTimeMillis());
		
		writeResourceFile(executionDir, flow);
		ExecutableFlowLoader.writeExecutableFlowFile(executionDir, flow, null);
		addActiveExecutionReference(flow);
		runningFlows.put(flow.getExecutionId(), flow);
		
		logger.info("Setting up " + flow.getExecutionId() + " for execution.");
		
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(url)
			.setPort(portNumber)
			.setPath("/executor")
			.setParameter("sharedToken", token)
			.setParameter("action", "execute")
			.setParameter("execid", flow.getExecutionId())
			.setParameter("execpath", flow.getExecutionPath());
		
		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			e.printStackTrace();
			flow.setStatus(ExecutableFlow.Status.FAILED);
			return;
		}

		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		logger.info("Submitting flow " + flow.getExecutionId() + " for execution.");
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			flow.setStatus(ExecutableFlow.Status.FAILED);
			e.printStackTrace();
			return;
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}
		
		logger.debug("Submitted Response: " + response);
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
		
		for (String deletable: deletableResources) {
			File deleteFile = new File(executionPath, deletable);
			if (deleteFile.exists()) {
				if (deleteFile.isDirectory()) {
					logger.info("Deleting directory " + deleteFile);
					try {
						FileUtils.deleteDirectory(deleteFile);
					} catch (IOException e) {
						logger.error("Failed deleting '" + deleteFile + "'", e);
					}
				}
				else {
					logger.info("Deleting file " + deleteFile);
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

	private String getFlowStatusInExecutor(ExecutableFlow flow) throws IOException {
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(url)
			.setPort(portNumber)
			.setPath("/executor")
			.setParameter("sharedToken", token)
			.setParameter("action", "status")
			.setParameter("execid", flow.getExecutionId());
		
		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			e.printStackTrace();
			throw new IOException("Bad URI", e);
		}
		
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Connection problem", e);
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}
		return response;
	}
	
	private void cleanFinishedJob(ExecutableFlow exFlow) throws ExecutorManagerException {
		
		// Write final file
		int updateNum = exFlow.getUpdateNumber();
		updateNum++;
		ExecutableFlowLoader.writeExecutableFlowFile(new File(exFlow.getExecutionPath()), exFlow, updateNum);
		
		String activeReferencePath = "active" + File.separator + exFlow.getExecutionId(); 
		File activeDirectory = new File(basePath, activeReferencePath);
		if (!activeDirectory.exists()) {
			logger.error("WTF!! Active reference " + activeDirectory + " directory doesn't exist.");
			throw new ExecutorManagerException("Active reference " + activeDirectory + " doesn't exists.");
		}
		
		String archiveReferencePath = "archive" + File.separator + exFlow.getExecutionId(); 
		File archiveDirectory = new File(basePath, archiveReferencePath);
		if (archiveDirectory.exists()) {
			logger.error("WTF!! Archive reference already exists!");
			throw new ExecutorManagerException("Active reference " + archiveDirectory + " already exists.");
		}
		
		// Move file.
		if (!activeDirectory.renameTo(archiveDirectory)) {
			throw new ExecutorManagerException("Cannot move " + activeDirectory + " to " + archiveDirectory);
		}
		
		runningFlows.remove(exFlow.getExecutionId());
		cleanupUnusedFiles(exFlow);
	}
	
	private class ExecutingManagerUpdaterThread extends Thread {
		private boolean shutdown = false;
		private int updateTimeMs = 100;
		public void run() {
			while (!shutdown) {
				ArrayList<ExecutableFlow> flows = new ArrayList<ExecutableFlow>(runningFlows.values());
				for(ExecutableFlow exFlow : flows) {
					File executionDir = new File(exFlow.getExecutionPath());
					
					if (!executionDir.exists()) {
						logger.error("WTF!! Execution dir " + executionDir + " doesn't exist!");
						// @TODO probably should handle this error case somehow. Cleanup?
						continue;
					}

					// Query the executor service to see if the item is running.
					String responseString = null;
					try {
						responseString = getFlowStatusInExecutor(exFlow);
					} catch (IOException e) {
						// Connection issue. Backoff 1 sec.
						synchronized(this) {
							try {
								wait(1000);
							} catch (InterruptedException ie) {
							}
						}
						continue;
					}
					
					Object executorResponseObj;
					try {
						executorResponseObj = JSONUtils.parseJSONFromString(responseString);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						continue;
					}
					
					@SuppressWarnings("unchecked")
					HashMap<String, Object> response = (HashMap<String, Object>)executorResponseObj;
					String status = (String)response.get("status");
					
					try {
						ExecutableFlowLoader.updateFlowStatusFromFile(executionDir, exFlow);
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
								cleanFinishedJob(exFlow);
							} catch (ExecutorManagerException e) {
								e.printStackTrace();
								continue;
							}
						}
						else {
							logger.error("Flow " + exFlow.getExecutionId() + " has succeeded, but the Executor says its still running");
						}
					}
					else {
						// If it's not finished, and not running, we will fail it and clean up.
						if (status.equals("notfound")) {
							logger.error("Flow " + exFlow.getExecutionId() + " is running, but the Executor can't find it.");
							exFlow.setEndTime(System.currentTimeMillis());
							exFlow.setStatus(Status.FAILED);
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
	
	private static class ExecutionReference {
		private String execId;
		private String projectId;
		private String flowId;
		private String userId;
		private String execPath;
		
		public ExecutionReference() {
		}
		
		public ExecutionReference(ExecutableFlow flow) {
			this.execId = flow.getExecutionId();
			this.projectId = flow.getProjectId();
			this.flowId = flow.getFlowId();
			this.userId = flow.getSubmitUser();
			this.execPath = flow.getExecutionPath();
		}
		
		private Object toObject() {
			HashMap<String, Object> obj = new HashMap<String, Object>();
			obj.put("execId", execId);
			obj.put("projectId", projectId);
			obj.put("flowId", flowId);
			obj.put("userId", userId);
			obj.put("execPath", execPath);
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
	}
}
