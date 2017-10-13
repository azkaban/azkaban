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
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalFlowWatcherTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private FlowRunnerTestUtil testUtil;

  @Before
  public void setUp() throws Exception {
    this.testUtil = new FlowRunnerTestUtil("exectest1", this.temporaryFolder);
    InteractiveTestJob.setQuickSuccess(true);
  }

  @After
  public void tearDown() throws IOException {
    InteractiveTestJob.resetQuickSuccess();
  }

  @Test
  public void testBasicLocalFlowWatcher() throws Exception {
    final FlowRunner runner1 = this.testUtil.createFromFlowFile("exec1");
    final FlowRunner runner2 = this.testUtil.createFromFlowFile("exec1", watcher(runner1), 2);
    FlowWatcherTestUtil.assertPipelineLevel2(runner1, runner2, false);
  }

  @Test
  public void testLevel1LocalFlowWatcher() throws Exception {
    final FlowRunner runner1 = this.testUtil.createFromFlowFile("exec1");
    final FlowRunner runner2 = this.testUtil.createFromFlowFile("exec1", watcher(runner1), 1);
    FlowWatcherTestUtil.assertPipelineLevel1(runner1, runner2);
  }

  @Test
  public void testLevel2DiffLocalFlowWatcher() throws Exception {
    final FlowRunner runner1 = this.testUtil.createFromFlowFile("exec1");
    final FlowRunner runner2 = this.testUtil.createFromFlowFile("exec1-mod", watcher(runner1), 2);
    FlowWatcherTestUtil.assertPipelineLevel2(runner1, runner2, false);
  }

  private LocalFlowWatcher watcher(final FlowRunner previousRunner) {
    return new LocalFlowWatcher(previousRunner);
  }
}
