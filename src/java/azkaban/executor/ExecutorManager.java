package azkaban.executor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class ExecutorManager {
	private static String FLOW_PATH = "flows";
	private static Logger logger = Logger.getLogger(ExecutorManager.class);
	private File basePath;
	
	private AtomicLong counter = new AtomicLong();
	private String token;
	
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
		
		token = props.getString("executor.shared.token", "");
		counter.set(0);
	}
	
	public synchronized ExecutableFlow createExecutableFlow(Flow flow) {
		String projectId = flow.getProjectId();
		
		File projectExecutionDir = new File(basePath, projectId);
		String id = flow.getId();
		
		// Find execution
		File executionDir;
		String executionId;
		do {
			executionId = String.valueOf(System.currentTimeMillis()) + "." + id;
			executionDir = new File(projectExecutionDir, executionId);
		}
		while(executionDir.exists());
		
		ExecutableFlow exFlow = new ExecutableFlow(executionId, flow);
		return exFlow;
	}
	
	public synchronized void setupExecutableFlow(ExecutableFlow exflow) throws ExecutorManagerException {
		String path = exflow.getExecutionId();
		String projectFlowDir = exflow.getProjectId() + File.separator + path;
		File executionPath = new File(basePath, projectFlowDir);
		if (executionPath.exists()) {
			throw new ExecutorManagerException("Execution path " + executionPath + " exists. Probably a simultaneous execution.");
		}
		
		executionPath.mkdirs();
		exflow.setExecutionPath(executionPath.getPath());
	}
	
	public void executeFlow(ExecutableFlow flow) throws ExecutorManagerException {
		String executionPath = flow.getExecutionPath();
		File executionDir = new File(executionPath);

		File resourceFile = writeResourceFile(executionDir, flow);
		File executableFlowFile = writeExecutableFlowFile(executionDir, flow);
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
}
