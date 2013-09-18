package azkaban.test.execapp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.execapp.FlowRunner;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.jobtype.JobTypeManager;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.test.executor.InteractiveTestJob;
import azkaban.test.executor.JavaJob;
import azkaban.utils.DirectoryFlowLoader;
import azkaban.utils.Props;

public class FlowRunnerTest2 {
	private File workingDir;
	private JobTypeManager jobtypeManager;
	private ProjectLoader fakeProjectLoader;
	private ExecutorLoader fakeExecutorLoader;
	private Logger logger = Logger.getLogger(FlowRunnerTest2.class);
	private Project project;
	private Map<String, Flow> flowMap;
	private static int id=101;
	
	public FlowRunnerTest2() {
	}
	
	@Before
	public void setUp() throws Exception {
		System.out.println("Create temp dir");
		workingDir = new File("_AzkabanTestDir_" + System.currentTimeMillis());
		if (workingDir.exists()) {
			FileUtils.deleteDirectory(workingDir);
		}
		workingDir.mkdirs();
		jobtypeManager = new JobTypeManager(null, this.getClass().getClassLoader());
		jobtypeManager.registerJobType("java", JavaJob.class);
		jobtypeManager.registerJobType("test", InteractiveTestJob.class);
		fakeProjectLoader = new MockProjectLoader(workingDir);
		fakeExecutorLoader = new MockExecutorLoader();
		project = new Project(1, "testProject");
		
		File dir = new File("unit/executions/embedded2");
		prepareProject(dir);
		
		InteractiveTestJob.clearTestJobs();
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
	public void testBasicRun() throws Exception {
		EventCollectorListener eventCollector = new EventCollectorListener();
		FlowRunner runner = createFlowRunner(eventCollector, "jobf");
		
		Map<String, Status> expectedStateMap = new HashMap<String, Status>();
		Map<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();
		
		// 1. STEP ONE: START FLOW
		ExecutableFlow flow = runner.getExecutableFlow();
		createExpectedStateMap(flow, expectedStateMap, nodeMap);
		runFlowRunnerInThread(runner);
		pause(1000);
		
		// After it starts up, only joba should be running
		expectedStateMap.put("joba", Status.RUNNING);
		expectedStateMap.put("joba1", Status.RUNNING);
		
		compareStates(expectedStateMap, nodeMap);
		ExecutableNode node = nodeMap.get("joba");
		Props props = node.getInputProps();
		Assert.assertEquals("joba.1", props.get("param1"));
		Assert.assertEquals("test1.2", props.get("param2"));
		Assert.assertEquals("test1.3", props.get("param3"));
		Assert.assertEquals("override.4", props.get("param4"));
		Assert.assertEquals("test2.5", props.get("param5"));
		Assert.assertEquals("test2.6", props.get("param6"));
		Assert.assertEquals("test2.7", props.get("param7"));
		Assert.assertEquals("test2.8", props.get("param8"));
		
		// Make joba successful
		
		
	}
	
	private Thread runFlowRunnerInThread(FlowRunner runner) {
		Thread thread = new Thread(runner);
		thread.start();
		return thread;
	}
	
	private void pause(long millisec) {
		synchronized(this) {
			try {
				wait(millisec);
			}
			catch (InterruptedException e) {
			}
		}
	}
	
	private void createExpectedStateMap(ExecutableFlowBase flow, Map<String, Status> expectedStateMap, Map<String, ExecutableNode> nodeMap) {
		for (ExecutableNode node: flow.getExecutableNodes()) {
			expectedStateMap.put(node.getPrintableId(), node.getStatus());
			nodeMap.put(node.getPrintableId(), node);
			
			if (node instanceof ExecutableFlowBase) {
				createExpectedStateMap((ExecutableFlowBase)node, expectedStateMap, nodeMap);
			}
		}
	}
	
	private void compareStates(Map<String, Status> expectedStateMap, Map<String, ExecutableNode> nodeMap) {
		for (String printedId: expectedStateMap.keySet()) {
			Status expectedStatus = expectedStateMap.get(printedId);
			ExecutableNode node = nodeMap.get(printedId);
			
			if (expectedStatus != node.getStatus()) {
				Assert.fail("Expected values do not match for " + printedId + ". Expected " + expectedStatus + ", instead received " + node.getStatus());
			}
		}
	}
	
	private void prepareProject(File directory) throws ProjectManagerException, IOException {
		DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);
		loader.loadProjectFlow(directory);
		if (!loader.getErrors().isEmpty()) {
			for (String error: loader.getErrors()) {
				System.out.println(error);
			}
			
			throw new RuntimeException("Errors found in setup");
		}
		
		flowMap = loader.getFlowMap();
		project.setFlows(flowMap);
		FileUtils.copyDirectory(directory, workingDir);
	}
	
	private FlowRunner createFlowRunner(EventCollectorListener eventCollector, String flowName) throws Exception {
		Flow flow = flowMap.get(flowName);

		int exId = id++;
		ExecutableFlow exFlow = new ExecutableFlow(project, flow);
		exFlow.setExecutionPath(workingDir.getPath());
		exFlow.setExecutionId(exId);

		Map<String, String> flowParam = new HashMap<String, String>();
		flowParam.put("param4", "override.4");
		flowParam.put("param10", "override.10");
		flowParam.put("param11", "override.11");
		exFlow.getExecutionOptions().addAllFlowParameters(flowParam);
		fakeExecutorLoader.uploadExecutableFlow(exFlow);
	
		FlowRunner runner = new FlowRunner(fakeExecutorLoader.fetchExecutableFlow(exId), fakeExecutorLoader, fakeProjectLoader, jobtypeManager);

		runner.addListener(eventCollector);
		
		return runner;
	}
}
