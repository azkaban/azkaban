package azkaban.utils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow;

public class ExecutableFlowLoader {
	private static final Logger logger = Logger.getLogger(ExecutableFlowLoader.class.getName());
	
	public static ExecutableFlow loadExecutableFlowFromDir(File exDir) {
		String exFlowName = exDir.getName();
		
		String flowFileName = "_" + exFlowName + ".flow";
		File[] exFlowFiles = exDir.listFiles(new PrefixFilter(flowFileName));
		Arrays.sort(exFlowFiles);
		
		if (exFlowFiles.length <= 0) {
			logger.error("Execution flow " + exFlowName + " missing flow file.");
			return null;
		}
		File lastExFlow = exFlowFiles[exFlowFiles.length-1];
		
		Object exFlowObj = null;
		try {
			exFlowObj = JSONUtils.parseJSONFromFile(lastExFlow);
		} catch (IOException e) {
			logger.error("Error loading execution flow " + exFlowName + ". Problems parsing json file.");
			return null;
		}
		
		ExecutableFlow flow = ExecutableFlow.createExecutableFlowFromObject(exFlowObj);
		return flow;
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
