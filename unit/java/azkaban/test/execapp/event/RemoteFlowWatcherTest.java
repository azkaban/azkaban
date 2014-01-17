package azkaban.test.execapp.event;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.execapp.FlowRunner;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.RemoteFlowWatcher;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.jobtype.JobTypeManager;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.test.execapp.EventCollectorListener;
import azkaban.test.execapp.MockExecutorLoader;
import azkaban.test.execapp.MockProjectLoader;
import azkaban.test.executor.JavaJob;
import azkaban.utils.JSONUtils;

public class RemoteFlowWatcherTest {
	private File workingDir;
	private JobTypeManager jobtypeManager;
	private ProjectLoader fakeProjectLoader;
	private int dirVal= 0;
	
	@Before
	public void setUp() throws Exception {
		jobtypeManager = new JobTypeManager(null, this.getClass().getClassLoader());
		jobtypeManager.registerJobType("java", JavaJob.class);
		fakeProjectLoader = new MockProjectLoader(workingDir);
	}
	
	@After
	public void tearDown() throws IOException {
	}
	
	public File setupDirectory() throws IOException {
		System.out.println("Create temp dir");
		File workingDir = new File("_AzkabanTestDir_" + dirVal );
		if (workingDir.exists()) {
			FileUtils.deleteDirectory(workingDir);
		}
		workingDir.mkdirs();
		dirVal++;
		
		return workingDir;
	}
	
	@Test
	public void testBasicRemoteFlowWatcher() throws Exception {
		MockExecutorLoader loader = new MockExecutorLoader();
		
		EventCollectorListener eventCollector = new EventCollectorListener();

		File workingDir1 = setupDirectory();
		FlowRunner runner1 = createFlowRunner(workingDir1, loader, eventCollector, "exec1", 1, null, null);
		Thread runner1Thread = new Thread(runner1);
		
		File workingDir2 = setupDirectory();
		RemoteFlowWatcher watcher = new RemoteFlowWatcher(1, loader, 100);
		FlowRunner runner2 = createFlowRunner(workingDir2, loader, eventCollector, "exec1", 2, watcher, 2);
		Thread runner2Thread = new Thread(runner2);
		
		printCurrentState("runner1 ", runner1.getExecutableFlow());
		runner1Thread.start();
		runner2Thread.start();
		
		runner2Thread.join();
		
		FileUtils.deleteDirectory(workingDir1);
		FileUtils.deleteDirectory(workingDir2);
		
		testPipelineLevel2(runner1.getExecutableFlow(), runner2.getExecutableFlow());
	}
	
	@Test
	public void testLevel1RemoteFlowWatcher() throws Exception {
		MockExecutorLoader loader = new MockExecutorLoader();
		
		EventCollectorListener eventCollector = new EventCollectorListener();

		File workingDir1 = setupDirectory();
		FlowRunner runner1 = createFlowRunner(workingDir1, loader, eventCollector, "exec1", 1, null, null);
		Thread runner1Thread = new Thread(runner1);
		
		File workingDir2 = setupDirectory();
		RemoteFlowWatcher watcher = new RemoteFlowWatcher(1, loader, 100);
		FlowRunner runner2 = createFlowRunner(workingDir2, loader, eventCollector, "exec1", 2, watcher, 1);
		Thread runner2Thread = new Thread(runner2);
		
		runner1Thread.start();
		runner2Thread.start();
		runner2Thread.join();
		
		FileUtils.deleteDirectory(workingDir1);
		FileUtils.deleteDirectory(workingDir2);
		
		testPipelineLevel1(runner1.getExecutableFlow(), runner2.getExecutableFlow());
	}
	
	@Test
	public void testLevel2DiffRemoteFlowWatcher() throws Exception {
		MockExecutorLoader loader = new MockExecutorLoader();
		
		EventCollectorListener eventCollector = new EventCollectorListener();

		File workingDir1 = setupDirectory();
		FlowRunner runner1 = createFlowRunner(workingDir1, loader, eventCollector, "exec1", 1, null, null);
		Thread runner1Thread = new Thread(runner1);
		
		File workingDir2 = setupDirectory();
		
		RemoteFlowWatcher watcher = new RemoteFlowWatcher(1, loader, 100);
		FlowRunner runner2 = createFlowRunner(workingDir2, loader, eventCollector, "exec1-mod", 2, watcher, 1);
		Thread runner2Thread = new Thread(runner2);
		
		runner1Thread.start();
		runner2Thread.start();
		runner2Thread.join();
		
		FileUtils.deleteDirectory(workingDir1);
		FileUtils.deleteDirectory(workingDir2);
		
		testPipelineLevel1(runner1.getExecutableFlow(), runner2.getExecutableFlow());
	}
	
	private void testPipelineLevel1(ExecutableFlow first, ExecutableFlow second) {
		for (ExecutableNode node: second.getExecutableNodes()) {
			Assert.assertEquals(node.getStatus(), Status.SUCCEEDED);
			
			// check it's start time is after the first's children.
			ExecutableNode watchedNode = first.getExecutableNode(node.getId());
			if (watchedNode == null) {
				continue;
			}
			Assert.assertEquals(watchedNode.getStatus(), Status.SUCCEEDED);
			
			System.out.println("Node " + node.getId() + 
					" start: " + node.getStartTime() + 
					" dependent on " + watchedNode.getId() + 
					" " + watchedNode.getEndTime() + 
					" diff: " + (node.getStartTime() - watchedNode.getEndTime()));

			Assert.assertTrue(node.getStartTime() >= watchedNode.getEndTime());
			
			long minParentDiff = 0;
			if (node.getInNodes().size() > 0) {
				minParentDiff = Long.MAX_VALUE;
				for (String dependency: node.getInNodes()) {
					ExecutableNode parent = second.getExecutableNode(dependency);
					long diff = node.getStartTime() - parent.getEndTime();
					minParentDiff = Math.min(minParentDiff, diff);
				}
			}
			long diff = node.getStartTime() - watchedNode.getEndTime();
			Assert.assertTrue(minParentDiff < 500 || diff < 500);
		}
	}
	
	private void testPipelineLevel2(ExecutableFlow first, ExecutableFlow second) {
		for (ExecutableNode node: second.getExecutableNodes()) {
			Assert.assertEquals(node.getStatus(), Status.SUCCEEDED);
			
			// check it's start time is after the first's children.
			ExecutableNode watchedNode = first.getExecutableNode(node.getId());
			if (watchedNode == null) {
				continue;
			}
			Assert.assertEquals(watchedNode.getStatus(), Status.SUCCEEDED);
			
			long minDiff = Long.MAX_VALUE;
			for (String watchedChild: watchedNode.getOutNodes()) {
				ExecutableNode child = first.getExecutableNode(watchedChild);
				if (child == null) {
					continue;
				}
				Assert.assertEquals(child.getStatus(), Status.SUCCEEDED);
				long diff = node.getStartTime() - child.getEndTime();
				minDiff = Math.min(minDiff, diff);
				System.out.println("Node " + node.getId() + 
						" start: " + node.getStartTime() + 
						" dependent on " + watchedChild + " " + child.getEndTime() +
						" diff: " + diff);
				Assert.assertTrue(node.getStartTime() >= child.getEndTime());
			}
			
			long minParentDiff = Long.MAX_VALUE;
			for (String dependency: node.getInNodes()) {
				ExecutableNode parent = second.getExecutableNode(dependency);
				long diff = node.getStartTime() - parent.getEndTime();
				minParentDiff = Math.min(minParentDiff, diff);
			}
			System.out.println("   minPipelineTimeDiff:" + minDiff + " minDependencyTimeDiff:" + minParentDiff);
			Assert.assertTrue(minParentDiff < 500 || minDiff < 500);
		}
	}
	
	private FlowRunner createFlowRunner(File workingDir, ExecutorLoader loader, EventCollectorListener eventCollector, String flowName, int execId, FlowWatcher watcher, Integer pipeline) throws Exception {
		File testDir = new File("unit/executions/exectest1");
		ExecutableFlow exFlow = prepareExecDir(workingDir, testDir, flowName, execId);
		ExecutionOptions options = exFlow.getExecutionOptions();
		if (watcher != null) {
			options.setPipelineLevel(pipeline);
			options.setPipelineExecutionId(watcher.getExecId());
		}
		//MockProjectLoader projectLoader = new MockProjectLoader(new File(exFlow.getExecutionPath()));
		
		loader.uploadExecutableFlow(exFlow);
		FlowRunner runner = new FlowRunner(exFlow,  loader, fakeProjectLoader, jobtypeManager);
		runner.setFlowWatcher(watcher);
		runner.addListener(eventCollector);
		
		return runner;
	}
	
	private void printCurrentState(String prefix, ExecutableFlowBase flow) {
		for(ExecutableNode node: flow.getExecutableNodes()) {

			System.err.println(prefix + node.getNestedId() + "->" + node.getStatus().name());
			if (node instanceof ExecutableFlowBase) {
				printCurrentState(prefix, (ExecutableFlowBase)node);
			}
		}
	}
	
	private ExecutableFlow prepareExecDir(File workingDir, File execDir, String flowName, int execId) throws IOException {
		FileUtils.copyDirectory(execDir, workingDir);
		
		File jsonFlowFile = new File(workingDir, flowName + ".flow");
		@SuppressWarnings("unchecked")
		HashMap<String, Object> flowObj = (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);
		
		Project project = new Project(1, "test");
		Flow flow = Flow.flowFromObject(flowObj);
		ExecutableFlow execFlow = new ExecutableFlow(project, flow);
		execFlow.setExecutionId(execId);
		execFlow.setExecutionPath(workingDir.getPath());
		return execFlow;
	}
}
