package azkaban;

import java.io.File;

import org.apache.log4j.Logger;

import azkaban.utils.DirectoryFlowLoader;

public class Scrubber {
	private static Logger logger = Logger.getLogger(Scrubber.class);
	
	public static void main(String[] args) {
		DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);
		
		File baseDir = new File(args[0]);
		loader.loadProjectFlow(baseDir);
	
		loader.getFlowMap();
	}
}