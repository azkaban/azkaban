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
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.LocalFlowWatcher;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
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

public class FlowRunnerPipelineTest {
	private File workingDir;
	private JobTypeManager jobtypeManager;
	private ProjectLoader fakeProjectLoader;
	private ExecutorLoader fakeExecutorLoader;
	private Logger logger = Logger.getLogger(FlowRunnerTest2.class);
	private Project project;
	private Map<String, Flow> flowMap;
	private static int id=101;
	
	public FlowRunnerPipelineTest() {
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
	public void testBasicPipelineLevel1Run() throws Exception {
		EventCollectorListener eventCollector = new EventCollectorListener();
		FlowRunner previousRunner = createFlowRunner(eventCollector, "jobf", "prev");
		
		ExecutionOptions options = new ExecutionOptions();
		options.setPipelineExecutionId(previousRunner.getExecutableFlow().getExecutionId());
		options.setPipelineLevel(1);
		FlowWatcher watcher = new LocalFlowWatcher(previousRunner);
		FlowRunner pipelineRunner = createFlowRunner(eventCollector, "jobf", "pipe", options);
		pipelineRunner.setFlowWatcher(watcher);
		
		Map<String, Status> previousExpectedStateMap = new HashMap<String, Status>();
		Map<String, Status> pipelineExpectedStateMap = new HashMap<String, Status>();
		Map<String, ExecutableNode> previousNodeMap = new HashMap<String, ExecutableNode>();
		Map<String, ExecutableNode> pipelineNodeMap = new HashMap<String, ExecutableNode>();
		
		// 1. START FLOW
		ExecutableFlow pipelineFlow = pipelineRunner.getExecutableFlow();
		ExecutableFlow previousFlow = previousRunner.getExecutableFlow();
		createExpectedStateMap(previousFlow, previousExpectedStateMap, previousNodeMap);
		createExpectedStateMap(pipelineFlow, pipelineExpectedStateMap, pipelineNodeMap);
		
		Thread thread1 = runFlowRunnerInThread(previousRunner);
		pause(250);
		Thread thread2 = runFlowRunnerInThread(pipelineRunner);
		pause(500);
		
		previousExpectedStateMap.put("joba", Status.RUNNING);
		previousExpectedStateMap.put("joba1", Status.RUNNING);
		pipelineExpectedStateMap.put("joba", Status.QUEUED);
		pipelineExpectedStateMap.put("joba1", Status.QUEUED);
		compareStates(previousExpectedStateMap, previousNodeMap);
		compareStates(pipelineExpectedStateMap, pipelineNodeMap);
		
		InteractiveTestJob.getTestJob("prev:joba").succeedJob();
		pause(250);
		previousExpectedStateMap.put("joba", Status.SUCCEEDED);
		previousExpectedStateMap.put("jobb", Status.RUNNING);
		previousExpectedStateMap.put("jobb:innerJobA", Status.RUNNING);
		previousExpectedStateMap.put("jobd", Status.RUNNING);
		previousExpectedStateMap.put("jobc", Status.RUNNING);
		previousExpectedStateMap.put("jobd:innerJobA", Status.RUNNING);
		pipelineExpectedStateMap.put("joba", Status.RUNNING);
		compareStates(previousExpectedStateMap, previousNodeMap);
		compareStates(pipelineExpectedStateMap, pipelineNodeMap);

		InteractiveTestJob.getTestJob("prev:jobb:innerJobA").succeedJob();
		pause(250);
		previousExpectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
		previousExpectedStateMap.put("jobb:innerJobB", Status.RUNNING);
		previousExpectedStateMap.put("jobb:innerJobC", Status.RUNNING);
		compareStates(previousExpectedStateMap, previousNodeMap);
		compareStates(pipelineExpectedStateMap, pipelineNodeMap);
		
		InteractiveTestJob.getTestJob("pipe:joba").succeedJob();
		pause(250);
		pipelineExpectedStateMap.put("joba", Status.SUCCEEDED);
		pipelineExpectedStateMap.put("jobb", Status.RUNNING);
		pipelineExpectedStateMap.put("jobd", Status.RUNNING);
		pipelineExpectedStateMap.put("jobc", Status.QUEUED);
		pipelineExpectedStateMap.put("jobd:innerJobA", Status.QUEUED);
		pipelineExpectedStateMap.put("jobb:innerJobA", Status.RUNNING);
		compareStates(previousExpectedStateMap, previousNodeMap);
		compareStates(pipelineExpectedStateMap, pipelineNodeMap);
		
		InteractiveTestJob.getTestJob("prev:jobd:innerJobA").succeedJob();
		pause(250);
		previousExpectedStateMap.put("jobd:innerJobA", Status.SUCCEEDED);
		previousExpectedStateMap.put("jobd:innerFlow2", Status.RUNNING);
		pipelineExpectedStateMap.put("jobd:innerJobA", Status.RUNNING);
		compareStates(previousExpectedStateMap, previousNodeMap);
		compareStates(pipelineExpectedStateMap, pipelineNodeMap);
		
		// Finish the previous d side
		InteractiveTestJob.getTestJob("prev:jobd:innerFlow2").succeedJob();
		pause(250);
		previousExpectedStateMap.put("jobd:innerFlow2", Status.SUCCEEDED);
		previousExpectedStateMap.put("jobd", Status.SUCCEEDED);
		compareStates(previousExpectedStateMap, previousNodeMap);
		
		InteractiveTestJob.getTestJob("prev:jobb:innerJobB").succeedJob();
		InteractiveTestJob.getTestJob("prev:jobb:innerJobC").succeedJob();
		InteractiveTestJob.getTestJob("prev:jobc").succeedJob();
		pause(250);
		InteractiveTestJob.getTestJob("pipe:jobb:innerJobA").succeedJob();
		pause(250);
		previousExpectedStateMap.put("jobb:innerJobB", Status.SUCCEEDED);
		previousExpectedStateMap.put("jobb:innerJobC", Status.SUCCEEDED);
		previousExpectedStateMap.put("jobb:innerFlow", Status.RUNNING);
		previousExpectedStateMap.put("jobc", Status.SUCCEEDED);
		pipelineExpectedStateMap.put("jobb:innerJobA", Status.SUCCEEDED);
		pipelineExpectedStateMap.put("jobc", Status.RUNNING);
		pipelineExpectedStateMap.put("jobb:innerJobB", Status.RUNNING);
		pipelineExpectedStateMap.put("jobb:innerJobC", Status.RUNNING);
		compareStates(previousExpectedStateMap, previousNodeMap);
		compareStates(pipelineExpectedStateMap, pipelineNodeMap);
		
//		Assert.assertEquals(Status.SUCCEEDED, pipelineFlow.getStatus());
//		Assert.assertEquals(Status.SUCCEEDED, previousFlow.getStatus());
//		Assert.assertFalse(thread1.isAlive());
//		Assert.assertFalse(thread2.isAlive());
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
			expectedStateMap.put(node.getNestedId(), node.getStatus());
			nodeMap.put(node.getNestedId(), node);
			
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
	
	private FlowRunner createFlowRunner(EventCollectorListener eventCollector, String flowName, String groupName) throws Exception {
		return createFlowRunner(eventCollector, flowName, groupName, new ExecutionOptions());
	}
	
	private FlowRunner createFlowRunner(EventCollectorListener eventCollector, String flowName, String groupName, ExecutionOptions options) throws Exception {
		Flow flow = flowMap.get(flowName);

		int exId = id++;
		ExecutableFlow exFlow = new ExecutableFlow(project, flow);
		exFlow.setExecutionPath(workingDir.getPath());
		exFlow.setExecutionId(exId);

		Map<String, String> flowParam = new HashMap<String, String>();
		flowParam.put("group", groupName);
		options.addAllFlowParameters(flowParam);
		exFlow.setExecutionOptions(options);
		fakeExecutorLoader.uploadExecutableFlow(exFlow);
	
		FlowRunner runner = new FlowRunner(fakeExecutorLoader.fetchExecutableFlow(exId), fakeExecutorLoader, fakeProjectLoader, jobtypeManager);

		runner.addListener(eventCollector);
		
		return runner;
	}

}
