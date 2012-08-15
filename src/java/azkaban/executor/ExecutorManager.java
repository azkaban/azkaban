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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

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
	
	private HashMap<String, ExecutableFlow> runningFlows = new HashMap<String, ExecutableFlow>();
	
	public ExecutorManager(Props props) {
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
		
		portNumber = props.getInt("executor.port", AzkabanExecutorServer.DEFAULT_PORT_NUMBER);
		token = props.getString("executor.shared.token", "");
		counter.set(0);
		loadActiveExecutions();
	}
	
	public List<ExecutableFlow> getExecutableFlowByProject(String projectId, int from, int maxResults) {
		File activeFlows = new File(basePath, projectId + File.separatorChar + "active");
		
		if (!activeFlows.exists()) {
			return Collections.emptyList();
		}
		
		File[] executionFiles = activeFlows.listFiles();
		if (executionFiles.length == 0 || from >= executionFiles.length) {
			return Collections.emptyList();
		}

		Arrays.sort(executionFiles);

		ArrayList<ExecutableFlow> executionFlows = new ArrayList<ExecutableFlow>();
		
		int index = (executionFiles.length - from - 1);
		for (int count = 0; count < maxResults && index >= 0; ++count, --index) {
			File exDir = executionFiles[index];
			ExecutableFlow flow = ExecutableFlowLoader.loadExecutableFlowFromDir(exDir);
			
			if (flow != null) {
				executionFlows.add(flow);
			}
			else {
				logger.info("Skipping loading " + exDir + ". Couldn't load execution.");
			}
		}
		
		return executionFlows;
	}
	
	public int getExecutableFlowByProjectFlow(String projectId, String flowName, int from, int maxResults, List<ExecutableFlow> results) {
		File activeFlows = new File(basePath, projectId + File.separatorChar + "active");
		
		if (!activeFlows.exists()) {
			return 0;
		}
		
		File[] executionFiles = activeFlows.listFiles(new SuffixFilter(flowName, false));
		//File[] executionFiles = activeFlows.listFiles();
		if (executionFiles.length == 0 || from >= executionFiles.length) {
			return 0;
		}
		Arrays.sort(executionFiles);

		int count = 0;
		for (int index = executionFiles.length - from - 1; count < maxResults && index>=0; --index ) {
			File exDir = executionFiles[index];
			ExecutableFlow flow = ExecutableFlowLoader.loadExecutableFlowFromDir(exDir);
			
			if (flow != null) {
				results.add(flow);
				count++;
			}
			else {
				logger.info("Skipping loading " + exDir + ". Couldn't load execution.");
			}
		}
		
		return executionFiles.length;
	}
//	
//	private ExecutableFlow loadExecutableFlowFromDir(File exDir) {
//		logger.info("Loading execution " + exDir.getName());
//		String exFlowName = exDir.getName();
//		
//		String flowFileName = "_" + exFlowName + ".flow";
//		File[] exFlowFiles = exDir.listFiles(new PrefixFilter(flowFileName));
//		Arrays.sort(exFlowFiles);
//		
//		if (exFlowFiles.length <= 0) {
//			logger.error("Execution flow " + exFlowName + " missing flow file.");
//			return null;
//		}
//		File lastExFlow = exFlowFiles[exFlowFiles.length-1];
//		
//		Object exFlowObj = null;
//		try {
//			exFlowObj = JSONUtils.parseJSONFromFile(lastExFlow);
//		} catch (IOException e) {
//			logger.error("Error loading execution flow " + exFlowName + ". Problems parsing json file.");
//			return null;
//		}
//		
//		ExecutableFlow flow = ExecutableFlow.createExecutableFlowFromObject(exFlowObj);
//		return flow;
//	}
//	
	private void loadActiveExecutions() {
		File[] executingProjects = basePath.listFiles();
		for (File project: executingProjects) {
			File activeFlows = new File(project, "active");
			if (!activeFlows.exists()) {
				continue;
			}
			
			for (File exflow: activeFlows.listFiles()) {
				logger.info("Loading execution " + exflow.getName());
				ExecutableFlow flow = ExecutableFlowLoader.loadExecutableFlowFromDir(exflow);
				
				if (flow != null) {
					logger.info("Adding active execution flow " + flow.getExecutionId());
					runningFlows.put(flow.getExecutionId(), flow);
				}
			}
		}
	}
	
	public synchronized ExecutableFlow createExecutableFlow(Flow flow) {
		String projectId = flow.getProjectId();
		
		File projectExecutionDir = new File(basePath, projectId);
		String id = flow.getId();
		
		// Find execution
		File executionDir;
		String executionId;
		int count = counter.getAndIncrement();
		String countString = String.format("%05d", count);
		do {
			executionId = String.valueOf(System.currentTimeMillis()) + "." + countString + "." + id;
			executionDir = new File(projectExecutionDir, executionId);
		}
		while(executionDir.exists());
		
		ExecutableFlow exFlow = new ExecutableFlow(executionId, flow);
		return exFlow;
	}
	
	public synchronized void setupExecutableFlow(ExecutableFlow exflow) throws ExecutorManagerException {
		String executionId = exflow.getExecutionId();
		String projectFlowDir = exflow.getProjectId() + File.separator + "active" + File.separator + executionId;
		File executionPath = new File(basePath, projectFlowDir);
		if (executionPath.exists()) {
			throw new ExecutorManagerException("Execution path " + executionPath + " exists. Probably a simultaneous execution.");
		}
		
		executionPath.mkdirs();
		exflow.setExecutionPath(executionPath.getPath());
		runningFlows.put(executionId, exflow);
	}
	
	public synchronized ExecutableFlow getExecutableFlow(String flowId) throws ExecutorManagerException {
		ExecutableFlow flow = runningFlows.get(flowId);
		
		return flow;
	}
	
	public void executeFlow(ExecutableFlow flow) throws ExecutorManagerException {
		String executionPath = flow.getExecutionPath();
		File executionDir = new File(executionPath);
		flow.setSubmitTime(System.currentTimeMillis());
		
		File resourceFile = writeResourceFile(executionDir, flow);
		File executableFlowFile = writeExecutableFlowFile(executionDir, flow);
		logger.info("Setting up " + flow.getExecutionId() + " for execution.");
		
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(url)
			.setPort(portNumber)
			.setPath("/submit")
			.setParameter("sharedToken", token)
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

		logger.info("Submitting flow " + flow.getExecutionId() + " for execution.");
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		HttpResponse response = null;
		try {
			response = httpclient.execute(httpget);
		} catch (IOException e) {
			flow.setStatus(ExecutableFlow.Status.FAILED);
			e.printStackTrace();
			return;
		}
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
	
	private File writeExecutableFlowFile(File executionDir, ExecutableFlow flow) throws ExecutorManagerException {
		// Write out the execution file
		String flowFileName = "_" + flow.getExecutionId() + ".flow";
		File flowFile = new File(executionDir, flowFileName);
		if (flowFile.exists()) {
			throw new ExecutorManagerException("The flow file " + flowFileName + " already exists. Race condition?");
		}

		BufferedOutputStream out = null;
		try {
			logger.info("Writing executable file " + flowFile);
			out = new BufferedOutputStream(new FileOutputStream(flowFile));
			JSONUtils.toJSON(flow.toObject(), out, true);
		} catch (IOException e) {
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
		
		return flowFile;
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

	private class ExecutingFlow implements Runnable {
		public void run() {
			
		}
	}
	
	private void updateRunningJobs() {
		
	}
	
	private String createUniqueId(String projectId, String flowId) {
		return null;
	}
	
	private static class PrefixFilter implements FileFilter {
		private String prefix;

		public PrefixFilter(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public boolean accept(File pathname) {
			String name = pathname.getName();

			return pathname.isFile() && !pathname.isHidden() && name.length() >= prefix.length() && name.startsWith(prefix);
		}
	}
	
	private static class SuffixFilter implements FileFilter {
		private String suffix;
		private boolean filesOnly = false;

		public SuffixFilter(String suffix, boolean filesOnly) {
			this.suffix = suffix;
		}

		@Override
		public boolean accept(File pathname) {
			String name = pathname.getName();
			return (pathname.isFile() || !filesOnly) && !pathname.isHidden() && name.length() >= suffix.length() && name.endsWith(suffix);
		}
	}
}
