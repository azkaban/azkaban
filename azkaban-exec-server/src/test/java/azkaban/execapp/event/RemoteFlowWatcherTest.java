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

import azkaban.execapp.FlowRunner;
import azkaban.execapp.FlowRunnerTestUtil;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.MockExecutorLoader;
import azkaban.executor.Status;
import azkaban.logs.MockExecutionLogsLoader;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RemoteFlowWatcherTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private FlowRunnerTestUtil testUtil;

  @Before
  public void setUp() throws Exception {
    this.testUtil = new FlowRunnerTestUtil("exectest1", this.temporaryFolder);
    this.testUtil.setExecutorLoader(new MockExecutorLoader());
    InteractiveTestJob.setQuickSuccess(true);
  }

  @After
  public void tearDown() throws IOException {
    InteractiveTestJob.resetQuickSuccess();
  }

  @Test
  public void testBasicRemoteFlowWatcher() throws Exception {
    final FlowRunner runner1 = this.testUtil.createFromFlowFile("exec1");
    final FlowRunner runner2 = this.testUtil.createFromFlowFile("exec1", watcher(runner1), 2);
    FlowWatcherTestUtil.assertPipelineLevel2(runner1, runner2, false);
  }

  @Test
  public void testRemoteFlowWatcherBlockingJobsLeftInReadyState() throws Exception {
    final FlowRunner runner1 = this.testUtil.createFromFlowFile("exec1");
    final FlowRunner runner2 = this.testUtil.createFromFlowFile("exec1", watcher(runner1), 2);

    final Thread runner1Thread = new Thread(runner1);
    runner1Thread.start();
    runner1Thread.join();

    // simulate a real life scenario - this can happen for disabled jobs inside subflows:
    // flow has finished otherwise but a "blocking" job has READY status
    // the test gets stuck here without the fix in FlowWatcher.peekStatus
    runner1.getExecutableFlow().getExecutableNodePath("job4").setStatus(Status.READY);
    this.testUtil.getExecutorLoader().updateExecutableFlow(runner1.getExecutableFlow());

    final Thread runner2Thread = new Thread(runner2);
    runner2Thread.start();
    runner2Thread.join();

    FlowWatcherTestUtil
        .assertPipelineLevel2(runner1.getExecutableFlow(), runner2.getExecutableFlow(), true);
  }

  @Test
  public void testLevel1RemoteFlowWatcher() throws Exception {
    final FlowRunner runner1 = this.testUtil.createFromFlowFile("exec1");
    final FlowRunner runner2 = this.testUtil.createFromFlowFile("exec1", watcher(runner1), 1);
    FlowWatcherTestUtil.assertPipelineLevel1(runner1, runner2);
  }

  @Test
  public void testLevel2DiffRemoteFlowWatcher() throws Exception {
    final FlowRunner runner1 = this.testUtil.createFromFlowFile("exec1");
    final FlowRunner runner2 = this.testUtil.createFromFlowFile("exec1-mod", watcher(runner1), 2);
    FlowWatcherTestUtil.assertPipelineLevel2(runner1, runner2, false);
  }

  private RemoteFlowWatcher watcher(final FlowRunner previousRunner) {
    return new RemoteFlowWatcher(previousRunner.getExecutionId(),
        this.testUtil.getExecutorLoader(), 10);
  }
}
