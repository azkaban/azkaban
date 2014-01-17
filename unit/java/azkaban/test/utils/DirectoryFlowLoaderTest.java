package azkaban.test.utils;

import java.io.File;

import junit.framework.Assert;

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
	
	@Test
	public void testLoadEmbeddedFlow() {
		Logger logger = Logger.getLogger(this.getClass());
		DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);
		
		loader.loadProjectFlow(new File("unit/executions/embedded"));
		Assert.assertEquals(0, loader.getErrors().size());
	}
	
	@Test
	public void testRecursiveLoadEmbeddedFlow() {
		Logger logger = Logger.getLogger(this.getClass());
		DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);
		
		loader.loadProjectFlow(new File("unit/executions/embeddedBad"));
		for (String error: loader.getErrors()) {
			System.out.println(error);
		}
		
		// Should be 3 errors: jobe->innerFlow, innerFlow->jobe, innerFlow
		Assert.assertEquals(3, loader.getErrors().size());
	}
}
