/*
 * Copyright 2014 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.execapp;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.event.EventData;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.JavaJob;
import azkaban.executor.MockExecutorLoader;
import azkaban.executor.SleepJavaJob;
import azkaban.executor.Status;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobtype.JobTypeManager;
import azkaban.utils.Props;

public class JobRunnerTest {
  private File workingDir;
  private JobTypeManager jobtypeManager;
  private Logger logger = Logger.getLogger("JobRunnerTest");

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
    jobtypeManager =
        new JobTypeManager(null, null, this.getClass().getClassLoader());

    jobtypeManager.getJobTypePluginSet().addPluginClass("java", JavaJob.class);
  }

  @After
  public void tearDown() throws IOException {
    System.out.println("Teardown temp dir");
    if (workingDir != null) {
      FileUtils.deleteDirectory(workingDir);
      workingDir = null;
    }
  }

  @Ignore @Test
  public void testBasicRun() {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    JobRunner runner =
        createJobRunner(1, "testJob", 1, false, loader, eventCollector);
    ExecutableNode node = runner.getNode();

    eventCollector.handleEvent(Event.create(null, Event.Type.JOB_STARTED, new EventData(node.getStatus())));
    Assert.assertTrue(runner.getStatus() != Status.SUCCEEDED
        || runner.getStatus() != Status.FAILED);

    runner.run();
    eventCollector.handleEvent(Event.create(null, Event.Type.JOB_FINISHED, new EventData(node.getStatus())));

    Assert.assertTrue(runner.getStatus() == node.getStatus());
    Assert.assertTrue("Node status is " + node.getStatus(),
        node.getStatus() == Status.SUCCEEDED);
    Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
    Assert.assertTrue(node.getEndTime() - node.getStartTime() > 1000);

    File logFile = new File(runner.getLogFilePath());
    Props outputProps = runner.getNode().getOutputProps();
    Assert.assertTrue(outputProps != null);
    Assert.assertTrue(logFile.exists());

    Assert.assertTrue(loader.getNodeUpdateCount(node.getId()) == 3);

    Assert.assertTrue(eventCollector.checkOrdering());
    try {
      eventCollector.checkEventExists(new Type[] { Type.JOB_STARTED,
          Type.JOB_STATUS_CHANGED, Type.JOB_FINISHED });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Ignore @Test
  public void testFailedRun() {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    JobRunner runner =
        createJobRunner(1, "testJob", 1, true, loader, eventCollector);
    ExecutableNode node = runner.getNode();

    Assert.assertTrue(runner.getStatus() != Status.SUCCEEDED
        || runner.getStatus() != Status.FAILED);
    runner.run();

    Assert.assertTrue(runner.getStatus() == node.getStatus());
    Assert.assertTrue(node.getStatus() == Status.FAILED);
    Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
    Assert.assertTrue(node.getEndTime() - node.getStartTime() > 1000);

    File logFile = new File(runner.getLogFilePath());
    Props outputProps = runner.getNode().getOutputProps();
    Assert.assertTrue(outputProps == null);
    Assert.assertTrue(logFile.exists());
    Assert.assertTrue(eventCollector.checkOrdering());
    Assert.assertTrue(!runner.isKilled());
    Assert.assertTrue(loader.getNodeUpdateCount(node.getId()) == 3);

    try {
      eventCollector.checkEventExists(new Type[] { Type.JOB_STARTED,
          Type.JOB_STATUS_CHANGED, Type.JOB_FINISHED });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDisabledRun() {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    JobRunner runner =
        createJobRunner(1, "testJob", 1, false, loader, eventCollector);
    ExecutableNode node = runner.getNode();

    node.setStatus(Status.DISABLED);

    // Should be disabled.
    Assert.assertTrue(runner.getStatus() == Status.DISABLED);
    runner.run();

    Assert.assertTrue(runner.getStatus() == node.getStatus());
    Assert.assertTrue(node.getStatus() == Status.SKIPPED);
    Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
    // Give it 10 ms to fail.
    Assert.assertTrue(node.getEndTime() - node.getStartTime() < 10);

    // Log file and output files should not exist.
    Props outputProps = runner.getNode().getOutputProps();
    Assert.assertTrue(outputProps == null);
    Assert.assertTrue(runner.getLogFilePath() == null);
    Assert.assertTrue(eventCollector.checkOrdering());

    Assert.assertTrue(loader.getNodeUpdateCount(node.getId()) == null);

    try {
      eventCollector.checkEventExists(new Type[] { Type.JOB_STARTED,
          Type.JOB_FINISHED });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testPreKilledRun() {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    JobRunner runner =
        createJobRunner(1, "testJob", 1, false, loader, eventCollector);
    ExecutableNode node = runner.getNode();

    node.setStatus(Status.KILLED);

    // Should be killed.
    Assert.assertTrue(runner.getStatus() == Status.KILLED);
    runner.run();

    // Should just skip the run and not change
    Assert.assertTrue(runner.getStatus() == node.getStatus());
    Assert.assertTrue(node.getStatus() == Status.KILLED);
    Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
    // Give it 10 ms to fail.
    Assert.assertTrue(node.getEndTime() - node.getStartTime() < 10);

    Assert.assertTrue(loader.getNodeUpdateCount(node.getId()) == null);

    // Log file and output files should not exist.
    Props outputProps = runner.getNode().getOutputProps();
    Assert.assertTrue(outputProps == null);
    Assert.assertTrue(runner.getLogFilePath() == null);
    Assert.assertTrue(!runner.isKilled());
    try {
      eventCollector.checkEventExists(new Type[] { Type.JOB_STARTED,
          Type.JOB_FINISHED });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Ignore @Test
  public void testCancelRun() {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    JobRunner runner =
        createJobRunner(13, "testJob", 10, false, loader, eventCollector);
    ExecutableNode node = runner.getNode();

    Assert.assertTrue(runner.getStatus() != Status.SUCCEEDED
        || runner.getStatus() != Status.FAILED);

    Thread thread = new Thread(runner);
    thread.start();

    synchronized (this) {
      try {
        wait(2000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      runner.kill();
      try {
        wait(500);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    Assert.assertTrue(runner.getStatus() == node.getStatus());
    Assert.assertTrue("Status is " + node.getStatus(),
        node.getStatus() == Status.KILLED);
    Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
    // Give it 10 ms to fail.
    Assert.assertTrue(node.getEndTime() - node.getStartTime() < 3000);
    Assert.assertTrue(loader.getNodeUpdateCount(node.getId()) == 3);

    // Log file and output files should not exist.
    File logFile = new File(runner.getLogFilePath());
    Props outputProps = runner.getNode().getOutputProps();
    Assert.assertTrue(outputProps == null);
    Assert.assertTrue(logFile.exists());
    Assert.assertTrue(eventCollector.checkOrdering());
    Assert.assertTrue(runner.isKilled());
    try {
      eventCollector.checkEventExists(new Type[] { Type.JOB_STARTED,
          Type.JOB_STATUS_CHANGED, Type.JOB_FINISHED });
    } catch (Exception e) {
      System.out.println(e.getMessage());

      Assert.fail(e.getMessage());
    }
  }

  @Ignore @Test
  public void testDelayedExecutionJob() {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    JobRunner runner =
        createJobRunner(1, "testJob", 1, false, loader, eventCollector);
    runner.setDelayStart(5000);
    long startTime = System.currentTimeMillis();
    ExecutableNode node = runner.getNode();

    eventCollector.handleEvent(Event.create(null, Event.Type.JOB_STARTED, new EventData(node.getStatus())));
    Assert.assertTrue(runner.getStatus() != Status.SUCCEEDED);

    runner.run();
    eventCollector.handleEvent(Event.create(null, Event.Type.JOB_FINISHED, new EventData(node.getStatus())));

    Assert.assertTrue(runner.getStatus() == node.getStatus());
    Assert.assertTrue("Node status is " + node.getStatus(),
        node.getStatus() == Status.SUCCEEDED);
    Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
    Assert.assertTrue(node.getEndTime() - node.getStartTime() > 1000);
    Assert.assertTrue(node.getStartTime() - startTime >= 5000);

    File logFile = new File(runner.getLogFilePath());
    Props outputProps = runner.getNode().getOutputProps();
    Assert.assertTrue(outputProps != null);
    Assert.assertTrue(logFile.exists());
    Assert.assertFalse(runner.isKilled());
    Assert.assertTrue(loader.getNodeUpdateCount(node.getId()) == 3);

    Assert.assertTrue(eventCollector.checkOrdering());
    try {
      eventCollector.checkEventExists(new Type[] { Type.JOB_STARTED,
          Type.JOB_STATUS_CHANGED, Type.JOB_FINISHED });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDelayedExecutionCancelledJob() {
    MockExecutorLoader loader = new MockExecutorLoader();
    EventCollectorListener eventCollector = new EventCollectorListener();
    JobRunner runner =
        createJobRunner(1, "testJob", 1, false, loader, eventCollector);
    runner.setDelayStart(5000);
    long startTime = System.currentTimeMillis();
    ExecutableNode node = runner.getNode();

    eventCollector.handleEvent(Event.create(null, Event.Type.JOB_STARTED, new EventData(node.getStatus())));
    Assert.assertTrue(runner.getStatus() != Status.SUCCEEDED);

    Thread thread = new Thread(runner);
    thread.start();

    synchronized (this) {
      try {
        wait(2000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      runner.kill();
      try {
        wait(500);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    eventCollector.handleEvent(Event.create(null, Event.Type.JOB_FINISHED, new EventData(node.getStatus())));

    Assert.assertTrue(runner.getStatus() == node.getStatus());
    Assert.assertTrue("Node status is " + node.getStatus(),
        node.getStatus() == Status.KILLED);
    Assert.assertTrue(node.getStartTime() > 0 && node.getEndTime() > 0);
    Assert.assertTrue(node.getEndTime() - node.getStartTime() < 1000);
    Assert.assertTrue(node.getStartTime() - startTime >= 2000);
    Assert.assertTrue(node.getStartTime() - startTime <= 5000);
    Assert.assertTrue(runner.isKilled());

    File logFile = new File(runner.getLogFilePath());
    Props outputProps = runner.getNode().getOutputProps();
    Assert.assertTrue(outputProps == null);
    Assert.assertTrue(logFile.exists());

    Assert.assertTrue(eventCollector.checkOrdering());
    try {
      eventCollector.checkEventExists(new Type[] { Type.JOB_FINISHED });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private Props createProps(int sleepSec, boolean fail) {
    Props props = new Props();
    props.put("type", "java");

    props.put(JavaJob.JOB_CLASS, SleepJavaJob.class.getName());
    props.put("seconds", sleepSec);
    props.put(ProcessJob.WORKING_DIR, workingDir.getPath());
    props.put("fail", String.valueOf(fail));

    return props;
  }

  private JobRunner createJobRunner(int execId, String name, int time,
      boolean fail, ExecutorLoader loader, EventCollectorListener listener) {
    ExecutableFlow flow = new ExecutableFlow();
    flow.setExecutionId(execId);
    ExecutableNode node = new ExecutableNode();
    node.setId(name);
    node.setParentFlow(flow);

    Props props = createProps(time, fail);
    node.setInputProps(props);
    HashSet<String> proxyUsers = new HashSet<String>();
    proxyUsers.add(flow.getSubmitUser());
    JobRunner runner = new JobRunner(node, workingDir, loader, jobtypeManager);
    runner.setLogSettings(logger, "5MB", 4);

    runner.addListener(listener);
    return runner;
  }

}
