package azkaban.test.utils;

import java.io.File;

import org.apache.log4j.Logger;
import org.junit.Test;

import azkaban.utils.DirectoryFlowLoader;

public class DirectoryFlowLoaderTest {

	@Test
	public void testDirectoryLoad() {
		Logger logger = Logger.getLogger(this.getClass());
		DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);
		
		loader.loadProjectFlow(new File("unit/executions/exectest1"));
		logger.info(loader.getFlowMap().size());
	}
	
}
