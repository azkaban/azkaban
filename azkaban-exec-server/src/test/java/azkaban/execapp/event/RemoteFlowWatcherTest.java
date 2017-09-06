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

package azkaban.execapp.event;

import static org.mockito.Mockito.mock;

import azkaban.execapp.EventCollectorListener;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.FlowRunnerTestUtil;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.MockExecutorLoader;
import azkaban.executor.Status;
import azkaban.jobtype.JobTypeManager;
import azkaban.project.ProjectLoader;
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RemoteFlowWatcherTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private JobTypeManager jobtypeManager;

  @Before
  public void setUp() throws Exception {
    this.jobtypeManager =
        new JobTypeManager(null, null, this.getClass().getClassLoader());
    this.jobtypeManager.getJobTypePluginSet().addPluginClass("test", InteractiveTestJob.class);
    Utils.initServiceProvider();
    JmxJobMBeanManager.getInstance().initialize(new Props());
    InteractiveTestJob.setQuickSuccess(true);
  }

  @After
  public void tearDown() throws IOException {
    InteractiveTestJob.resetQuickSuccess();
  }

  @Test
  public void testBasicRemoteFlowWatcher() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();

    final EventCollectorListener eventCollector = new EventCollectorListener();

    final File workingDir1 = this.temporaryFolder.newFolder();
    final FlowRunner runner1 =
        createFlowRunner(workingDir1, loader, eventCollector, "exec1", 1, null,
            null);
    final Thread runner1Thread = new Thread(runner1);

    final File workingDir2 = this.temporaryFolder.newFolder();
    final RemoteFlowWatcher watcher = new RemoteFlowWatcher(1, loader, 100);
    final FlowRunner runner2 =
        createFlowRunner(workingDir2, loader, eventCollector, "exec1", 2,
            watcher, 2);
    final Thread runner2Thread = new Thread(runner2);

    printCurrentState("runner1 ", runner1.getExecutableFlow());
    runner1Thread.start();
    runner2Thread.start();

    runner2Thread.join();

    assertPipelineLevel2(runner1.getExecutableFlow(), runner2.getExecutableFlow());
  }

  @Test
  public void testLevel1RemoteFlowWatcher() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();

    final EventCollectorListener eventCollector = new EventCollectorListener();

    final File workingDir1 = this.temporaryFolder.newFolder();
    final FlowRunner runner1 =
        createFlowRunner(workingDir1, loader, eventCollector, "exec1", 1, null,
            null);
    final Thread runner1Thread = new Thread(runner1);

    final File workingDir2 = this.temporaryFolder.newFolder();
    final RemoteFlowWatcher watcher = new RemoteFlowWatcher(1, loader, 100);
    final FlowRunner runner2 =
        createFlowRunner(workingDir2, loader, eventCollector, "exec1", 2,
            watcher, 1);
    final Thread runner2Thread = new Thread(runner2);

    runner1Thread.start();
    runner2Thread.start();
    runner2Thread.join();

    testPipelineLevel1(runner1.getExecutableFlow(), runner2.getExecutableFlow());
  }

  @Test
  public void testLevel2DiffRemoteFlowWatcher() throws Exception {
    final MockExecutorLoader loader = new MockExecutorLoader();

    final EventCollectorListener eventCollector = new EventCollectorListener();

    final File workingDir1 = this.temporaryFolder.newFolder();
    final FlowRunner runner1 =
        createFlowRunner(workingDir1, loader, eventCollector, "exec1", 1, null,
            null);
    final Thread runner1Thread = new Thread(runner1);

    final File workingDir2 = this.temporaryFolder.newFolder();

    final RemoteFlowWatcher watcher = new RemoteFlowWatcher(1, loader, 100);
    final FlowRunner runner2 =
        createFlowRunner(workingDir2, loader, eventCollector, "exec1-mod", 2,
            watcher, 1);
    final Thread runner2Thread = new Thread(runner2);

    runner1Thread.start();
    runner2Thread.start();
    runner2Thread.join();

    testPipelineLevel1(runner1.getExecutableFlow(), runner2.getExecutableFlow());
  }

  private void testPipelineLevel1(final ExecutableFlow first, final ExecutableFlow second) {
    for (final ExecutableNode node : second.getExecutableNodes()) {
      Assert.assertEquals(node.getStatus(), Status.SUCCEEDED);

      // check it's start time is after the first's children.
      final ExecutableNode watchedNode = first.getExecutableNode(node.getId());
      if (watchedNode == null) {
        continue;
      }
      Assert.assertEquals(watchedNode.getStatus(), Status.SUCCEEDED);

      System.out.println("Node " + node.getId() + " start: "
          + node.getStartTime() + " dependent on " + watchedNode.getId() + " "
          + watchedNode.getEndTime() + " diff: "
          + (node.getStartTime() - watchedNode.getEndTime()));

      Assert.assertTrue(node.getStartTime() >= watchedNode.getEndTime());

      long minParentDiff = 0;
      if (node.getInNodes().size() > 0) {
        minParentDiff = Long.MAX_VALUE;
        for (final String dependency : node.getInNodes()) {
          final ExecutableNode parent = second.getExecutableNode(dependency);
          final long diff = node.getStartTime() - parent.getEndTime();
          minParentDiff = Math.min(minParentDiff, diff);
        }
      }
      final long diff = node.getStartTime() - watchedNode.getEndTime();
      Assert.assertTrue(minParentDiff < 500 || diff < 500);
    }
  }

  private void assertPipelineLevel2(final ExecutableFlow first, final ExecutableFlow second) {
    for (final ExecutableNode node : second.getExecutableNodes()) {
      Assert.assertEquals(node.getStatus(), Status.SUCCEEDED);

      // check it's start time is after the first's children.
      final ExecutableNode watchedNode = first.getExecutableNode(node.getId());
      if (watchedNode == null) {
        continue;
      }
      Assert.assertEquals(watchedNode.getStatus(), Status.SUCCEEDED);

      long minDiff = Long.MAX_VALUE;
      for (final String watchedChild : watchedNode.getOutNodes()) {
        final ExecutableNode child = first.getExecutableNode(watchedChild);
        if (child == null) {
          continue;
        }
        Assert.assertEquals(child.getStatus(), Status.SUCCEEDED);
        final long diff = node.getStartTime() - child.getEndTime();
        minDiff = Math.min(minDiff, diff);
        Assert.assertTrue(
            "Node " + node.getId() + " start: " + node.getStartTime() + " dependent on "
                + watchedChild + " " + child.getEndTime() + " diff: " + diff,
            node.getStartTime() >= child.getEndTime());
      }

      long minParentDiff = Long.MAX_VALUE;
      for (final String dependency : node.getInNodes()) {
        final ExecutableNode parent = second.getExecutableNode(dependency);
        final long diff = node.getStartTime() - parent.getEndTime();
        minParentDiff = Math.min(minParentDiff, diff);
      }
      Assert.assertTrue("minPipelineTimeDiff:" + minDiff
              + " minDependencyTimeDiff:" + minParentDiff,
          minParentDiff < 5000 || minDiff < 5000);
    }
  }

  private FlowRunner createFlowRunner(final File workingDir, final ExecutorLoader loader,
      final EventCollectorListener eventCollector, final String flowName, final int execId,
      final FlowWatcher watcher, final Integer pipeline) throws Exception {
    return createFlowRunner(workingDir, loader, eventCollector, flowName, execId, watcher, pipeline,
        new Props());
  }

  private FlowRunner createFlowRunner(final File workingDir, final ExecutorLoader loader,
      final EventCollectorListener eventCollector, final String flowName, final int execId,
      final FlowWatcher watcher, final Integer pipeline, final Props azkabanProps)
      throws Exception {
    final File testDir = ExecutionsTestUtil.getFlowDir("exectest1");
    final ExecutableFlow exFlow =
        FlowRunnerTestUtil.prepareExecDir(workingDir, testDir, flowName, execId);
    final ExecutionOptions options = exFlow.getExecutionOptions();
    if (watcher != null) {
      options.setPipelineLevel(pipeline);
      options.setPipelineExecutionId(watcher.getExecId());
    }

    loader.uploadExecutableFlow(exFlow);
    final FlowRunner runner = new FlowRunner(exFlow, loader, mock(ProjectLoader.class),
        this.jobtypeManager, azkabanProps);
    runner.setFlowWatcher(watcher);
    runner.addListener(eventCollector);

    return runner;
  }

  private void printCurrentState(final String prefix, final ExecutableFlowBase flow) {
    for (final ExecutableNode node : flow.getExecutableNodes()) {

      System.err.println(prefix + node.getNestedId() + "->"
          + node.getStatus().name());
      if (node instanceof ExecutableFlowBase) {
        printCurrentState(prefix, (ExecutableFlowBase) node);
      }
    }
  }

}
