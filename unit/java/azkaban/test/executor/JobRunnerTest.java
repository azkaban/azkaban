package azkaban.test.executor;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.JobRunner;
import azkaban.jobExecutor.JavaJob;
import azkaban.jobExecutor.ProcessJob;
import azkaban.utils.Props;

public class JobRunnerTest {
	private File workingDir;
	
	public JobRunnerTest() {

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
	public void testBasicRun() {
		EventCollectorListener eventCollector = new EventCollectorListener();
		Props props = createProps(1, false);
		ExecutableNode node = new ExecutableNode();
		node.setId("myjobid");
		
		eventCollector.handleEvent(Event.create(null, Event.Type.JOB_STARTED));
		JobRunner runner = new JobRunner("myexecutionid", node, props, workingDir);
		runner.addListener(eventCollector);
		Assert.assertTrue(runner.getStatus() != Status.SUCCEEDED || runner.getStatus() != Status.FAILED);
		
		runner.run();
		eventCollector.handleEvent(Event.create(null, Event.Type.JOB_SUCCEEDED));
		
		Assert.assertTrue(runner.getStatus() == node.getStatus());
		Assert.assertTrue(node.getStatus() == Status.SUCCEEDED);
		Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
		Assert.assertTrue( node.getEndTime() - node.getStartTime() > 1000);
		
		File logFile = new File(runner.getLogFilePath());
		Props outputProps = runner.getOutputProps();
		Assert.assertTrue(outputProps != null);
		Assert.assertTrue(outputProps.getKeySet().isEmpty());
		Assert.assertTrue(logFile.exists());
		
		Assert.assertTrue(eventCollector.checkOrdering());
		try {
			eventCollector.checkEventExists(new Type[] {Type.JOB_STARTED, Type.JOB_SUCCEEDED});
		}
		catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testFailedRun() {
		EventCollectorListener eventCollector = new EventCollectorListener();
		Props props = createProps(1, true);
		ExecutableNode node = new ExecutableNode();
		node.setId("myjobid");
		
		JobRunner runner = new JobRunner("myexecutionid", node, props, workingDir);
		runner.addListener(eventCollector);
		Assert.assertTrue(runner.getStatus() != Status.SUCCEEDED || runner.getStatus() != Status.FAILED);
		eventCollector.handleEvent(Event.create(null, Type.JOB_STARTED));
		runner.run();
		eventCollector.handleEvent(Event.create(null, Type.JOB_FAILED));
		
		Assert.assertTrue(runner.getStatus() == node.getStatus());
		Assert.assertTrue(node.getStatus() == Status.FAILED);
		Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
		Assert.assertTrue(node.getEndTime() - node.getStartTime() > 1000);
		
		File logFile = new File(runner.getLogFilePath());
		Props outputProps = runner.getOutputProps();
		Assert.assertTrue(outputProps == null);
		Assert.assertTrue(logFile.exists());
		Assert.assertTrue(eventCollector.checkOrdering());
		
		try {
			eventCollector.checkEventExists(new Type[] {Type.JOB_STARTED, Type.JOB_FAILED});
		}
		catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testDisabledRun() {
		EventCollectorListener eventCollector = new EventCollectorListener();
		Props props = createProps(1, true);
		ExecutableNode node = new ExecutableNode();
		node.setId("myjobid");
		
		node.setStatus(Status.DISABLED);
		JobRunner runner = new JobRunner("myexecutionid", node, props, workingDir);
		runner.addListener(eventCollector);
		
		// Should be disabled.
		Assert.assertTrue(runner.getStatus() == Status.DISABLED);
		eventCollector.handleEvent(Event.create(null, Type.JOB_STARTED));
		runner.run();
		eventCollector.handleEvent(Event.create(null, Type.JOB_SUCCEEDED));
		
		Assert.assertTrue(runner.getStatus() == node.getStatus());
		Assert.assertTrue(node.getStatus() == Status.SKIPPED);
		Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
		// Give it 10 ms to fail.
		Assert.assertTrue( node.getEndTime() - node.getStartTime() < 10);
		
		// Log file and output files should not exist.
		Props outputProps = runner.getOutputProps();
		Assert.assertTrue(outputProps == null);
		Assert.assertTrue(runner.getLogFilePath() == null);
		Assert.assertTrue(eventCollector.checkOrdering());
		
		try {
			eventCollector.checkEventExists(new Type[] {Type.JOB_SUCCEEDED});
		}
		catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testPreKilledRun() {
		EventCollectorListener eventCollector = new EventCollectorListener();
		Props props = createProps(1, true);
		ExecutableNode node = new ExecutableNode();
		node.setId("myjobid");
		
		node.setStatus(Status.KILLED);
		JobRunner runner = new JobRunner("myexecutionid", node, props, workingDir);
		runner.addListener(eventCollector);
		
		// Should be killed.
		Assert.assertTrue(runner.getStatus() == Status.KILLED);
		eventCollector.handleEvent(Event.create(null, Type.JOB_STARTED));
		runner.run();
		eventCollector.handleEvent(Event.create(null, Type.JOB_KILLED));
		
		// Should just skip the run and not change
		Assert.assertTrue(runner.getStatus() == node.getStatus());
		Assert.assertTrue(node.getStatus() == Status.KILLED);
		Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
		// Give it 10 ms to fail.
		Assert.assertTrue( node.getEndTime() - node.getStartTime() < 10);
		
		// Log file and output files should not exist.
		Props outputProps = runner.getOutputProps();
		Assert.assertTrue(outputProps == null);
		Assert.assertTrue(runner.getLogFilePath() == null);
		
		try {
			eventCollector.checkEventExists(new Type[] {Type.JOB_KILLED});
		}
		catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelRun() {
		EventCollectorListener eventCollector = new EventCollectorListener();
		Props props = createProps(5, true);
		ExecutableNode node = new ExecutableNode();
		node.setId("myjobid");
		
		JobRunner runner = new JobRunner("myexecutionid", node, props, workingDir);
		runner.addListener(eventCollector);
		Assert.assertTrue(runner.getStatus() != Status.SUCCEEDED || runner.getStatus() != Status.FAILED);
		
		eventCollector.handleEvent(Event.create(null, Type.JOB_STARTED));
		Thread thread = new Thread(runner);
		thread.start();
		
		eventCollector.handleEvent(Event.create(null, Type.JOB_KILLED));
		synchronized(this) {
			try {
				wait(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			runner.cancel();
		}
		
		Assert.assertTrue(runner.getStatus() == node.getStatus());
		Assert.assertTrue(node.getStatus() == Status.FAILED);
		Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
		// Give it 10 ms to fail.
		Assert.assertTrue(node.getEndTime() - node.getStartTime() < 3000);
		
		// Log file and output files should not exist.
		File logFile = new File(runner.getLogFilePath());
		Props outputProps = runner.getOutputProps();
		Assert.assertTrue(outputProps == null);
		Assert.assertTrue(logFile.exists());
		Assert.assertTrue(eventCollector.checkOrdering());
		
		try {
			eventCollector.checkEventExists(new Type[] {Type.JOB_STARTED, Type.JOB_FAILED});
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			
			Assert.fail(e.getMessage());
		}
	}
	
	private Props createProps( int sleepSec, boolean fail) {
		Props props = new Props();
		props.put("type", "java");
		props.put(JavaJob.JOB_CLASS, "azkaban.test.executor.SleepJavaJob");
		props.put("seconds", 1);
		props.put(ProcessJob.WORKING_DIR, workingDir.getPath());
		props.put("fail", String.valueOf(fail));

		return props;
	}
}