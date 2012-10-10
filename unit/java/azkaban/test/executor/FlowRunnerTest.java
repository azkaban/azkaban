package azkaban.test.executor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.FlowRunner;
import azkaban.flow.Flow;
import azkaban.utils.JSONUtils;

public class FlowRunnerTest {
	private File workingDir;
	private Logger logger = Logger.getLogger(FlowRunnerTest.class);
	
	public FlowRunnerTest() {
		
	}
	
	@Before
	public void setUp() throws Exception {
		System.out.println("Create temp dir");
		workingDir = new File("_AzkabanTestDir_" + System.currentTimeMillis());
		if (workingDir.exists()) {
			FileUtils.deleteDirectory(workingDir);
		}
		workingDir.mkdirs();
	}
	
	@After
	public void tearDown() throws IOException {
		System.out.println("Teardown temp dir");
		if (workingDir != null) {
			FileUtils.deleteDirectory(workingDir);
			workingDir = null;
		}
	}
	
	@Test
	public void exec1() throws Exception {
		File testDir = new File("unit/executions/exectest1");
		ExecutableFlow exFlow = prepareExecDir(testDir, "exec1");
		
		FlowRunner runner = new FlowRunner(exFlow);
		
		
	}
	
	private ExecutableFlow prepareExecDir(File execDir, String execName) throws IOException {
		FileUtils.copyDirectory(execDir, workingDir);
		
		File jsonFlowFile = new File(workingDir, execName + ".flow");
		HashMap<String, Object> flowObj = (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);
		
		Flow flow = Flow.flowFromObject(flowObj);
		ExecutableFlow execFlow = new ExecutableFlow(execName, flow);
		execFlow.setExecutionPath(workingDir.getPath());
		return execFlow;
	}

}
