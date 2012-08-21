package azkaban.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;

public class ExecutableFlowLoader {
	private static final Logger logger = Logger.getLogger(ExecutableFlowLoader.class.getName());
	
	/**
	 * Loads and create ExecutableFlow from the latest execution file.
	 * 
	 * @param exDir
	 * @return
	 * @throws ExecutorManagerException
	 */
	public static ExecutableFlow loadExecutableFlowFromDir(File exDir) throws ExecutorManagerException {
		File flowFile = getLatestExecutableFlowDir(exDir);
		Object exFlowObj = getFlowObjectFromFile(flowFile);

		int updateNumber = getFlowUpdateNumber(flowFile);
		ExecutableFlow flow = ExecutableFlow.createExecutableFlowFromObject(exFlowObj);
		flow.setUpdateNumber(updateNumber);
		flow.setExecutionPath(exDir.getPath());
		return flow;
	}
	
	/**
	 * Get the latest update number from file.
	 * @param file
	 * @return
	 */
	private static int getFlowUpdateNumber(File file) {
		String[] namesplit = file.getName().split("\\.");
		
		Integer number = 0;
		try {
			number = Integer.parseInt(namesplit[namesplit.length - 1]);
		}
		catch(NumberFormatException e) {
		}
		
		return number;
	}
	
	/**
	 * Get Flow object from file
	 * 
	 * @param file
	 * @return
	 * @throws ExecutorManagerException
	 */
	private static Object getFlowObjectFromFile(File file) throws ExecutorManagerException {
		Object exFlowObj = null;
		try {
			exFlowObj = JSONUtils.parseJSONFromFile(file);
		} catch (IOException e) {
			logger.error("Error loading execution flow " + file.getName() + ". Problems parsing json file.");
			throw new ExecutorManagerException(e.getMessage(), e);
		}
		
		return exFlowObj;
	}
	
	/**
	 * Get the latest executable flow dir
	 * 
	 * @param exDir
	 * @return
	 * @throws ExecutorManagerException
	 */
	private static File getLatestExecutableFlowDir(File exDir) throws ExecutorManagerException {
		String exFlowName = exDir.getName();
		
		String flowFileName = "_" + exFlowName + ".flow";
		File[] exFlowFiles = exDir.listFiles(new PrefixFilter(flowFileName));
		Arrays.sort(exFlowFiles);
		
		if (exFlowFiles.length <= 0) {
			logger.error("Execution flow " + exFlowName + " missing flow file.");
			throw new ExecutorManagerException("Execution flow " + exFlowName + " missing flow file.");
		}
		File lastExFlow = exFlowFiles[exFlowFiles.length-1];
		return lastExFlow;
	}
	
	/**
	 * Update Flow status
	 * 
	 * @param exDir
	 * @param flow
	 * @return
	 * @throws ExecutorManagerException
	 */
	public static boolean updateFlowStatusFromFile(File exDir, ExecutableFlow flow) throws ExecutorManagerException {
		File file = getLatestExecutableFlowDir(exDir);
		System.out.println("Loading from: " + file);
		int number =  getFlowUpdateNumber(file);
		if (flow.getUpdateNumber() >= number) {
			return false;
		}
		
		Object exFlowObj = getFlowObjectFromFile(file);
		flow.updateExecutableFlowFromObject(exFlowObj);
		flow.setUpdateNumber(number);
		
		return true;
	}
	
	/**
	 * Write executable flow file
	 * 
	 * @param executionDir
	 * @param flow
	 * @param commitValue
	 * @return
	 * @throws ExecutorManagerException
	 */
	public static File writeExecutableFlowFile(File executionDir, ExecutableFlow flow, Integer commitValue) throws ExecutorManagerException {
		// Write out the execution file
		String flowFileName =  "_" + flow.getExecutionId() + ".flow";
		if (commitValue != null) {
			String countString = String.format("%05d", commitValue);
			flowFileName += "." + countString;
		}
		
		File flowFile = new File(executionDir, flowFileName);
		if (flowFile.exists()) {
			throw new ExecutorManagerException("The flow file " + flowFileName + " already exists. Race condition?");
		}

		File tempFlowFile = new File(executionDir, "_tmp" + flowFileName);
		BufferedOutputStream out = null;
		try {
			logger.debug("Writing executable file " + flowFile);
			out = new BufferedOutputStream(new FileOutputStream(tempFlowFile));
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
		
		tempFlowFile.renameTo(flowFile);
		return flowFile;
	}
	
	/**
	 *
	 */
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

}
