/*
 * Copyright 2017 LinkedIn Corp.
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

import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.LocalFlowWatcher;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.Status;
import org.junit.Before;
import org.junit.Test;

/**
 * Flows in this test: joba jobb joba1 jobc->joba jobd->joba jobe->jobb,jobc,jobd jobf->jobe,joba1
 *
 * jobb = innerFlow innerJobA innerJobB->innerJobA innerJobC->innerJobB
 * innerFlow->innerJobB,innerJobC
 *
 * jobd=innerFlow2 innerFlow2->innerJobA
 *
 * @author rpark
 */
public class FlowRunnerPipelineTest extends FlowRunnerTestBase {

  private FlowRunnerTestUtil testUtil;

  @Before
  public void setUp() throws Exception {
    this.testUtil = new FlowRunnerTestUtil("embedded2", this.temporaryFolder);
  }

  @Test
  public void testBasicPipelineLevel1RunDisabledJobs() throws Exception {
    final FlowRunner previousRunner = this.testUtil.createFromFlowMap("jobf", "prev");

    final ExecutionOptions options = new ExecutionOptions();
    options.setPipelineExecutionId(previousRunner.getExecutableFlow()
        .getExecutionId());
    options.setPipelineLevel(1);
    final FlowWatcher watcher = new LocalFlowWatcher(previousRunner);
    final FlowRunner pipelineRunner = this.testUtil.createFromFlowMap("jobf", "pipe", options);
    pipelineRunner.setFlowWatcher(watcher);

    // 1. START FLOW
    final ExecutableFlow pipelineFlow = pipelineRunner.getExecutableFlow();
    final ExecutableFlow previousFlow = previousRunner.getExecutableFlow();
    // disable the innerFlow (entire sub-flow)
    previousFlow.getExecutableNodePath("jobb").setStatus(Status.DISABLED);

    FlowRunnerTestUtil.startThread(previousRunner);
    assertStatus(previousFlow, "joba", Status.RUNNING);
    assertStatus(previousFlow, "joba", Status.RUNNING);
    assertStatus(previousFlow, "joba1", Status.RUNNING);

    FlowRunnerTestUtil.startThread(pipelineRunner);
    assertStatus(pipelineFlow, "joba", Status.QUEUED);
    assertStatus(pipelineFlow, "joba1", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:joba").succeedJob();
    assertStatus(previousFlow, "joba", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobb", Status.SKIPPED);
    assertStatus(previousFlow, "jobb:innerJobA", Status.READY);
    assertStatus(previousFlow, "jobd", Status.RUNNING);
    assertStatus(previousFlow, "jobc", Status.RUNNING);
    assertStatus(previousFlow, "jobd:innerJobA", Status.RUNNING);
    assertStatus(pipelineFlow, "joba", Status.RUNNING);

    assertStatus(previousFlow, "jobb:innerJobA", Status.READY);
    assertStatus(previousFlow, "jobb:innerJobB", Status.READY);
    assertStatus(previousFlow, "jobb:innerJobC", Status.READY);

    InteractiveTestJob.getTestJob("pipe:joba").succeedJob();
    assertStatus(pipelineFlow, "joba", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb", Status.RUNNING);
    assertStatus(pipelineFlow, "jobd", Status.RUNNING);
    assertStatus(pipelineFlow, "jobc", Status.QUEUED);
    assertStatus(pipelineFlow, "jobd:innerJobA", Status.QUEUED);
    assertStatus(pipelineFlow, "jobb:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:jobd:innerJobA").succeedJob();
    assertStatus(previousFlow, "jobd:innerJobA", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobd:innerFlow2", Status.RUNNING);
    assertStatus(pipelineFlow, "jobd:innerJobA", Status.RUNNING);

    // Finish the previous d side
    InteractiveTestJob.getTestJob("prev:jobd:innerFlow2").succeedJob();
    assertStatus(previousFlow, "jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobd", Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("pipe:jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("prev:jobc").succeedJob();
    assertStatus(previousFlow, "jobb:innerJobB", Status.READY);
    assertStatus(previousFlow, "jobb:innerJobC", Status.READY);
    assertStatus(previousFlow, "jobb:innerFlow", Status.READY);
    assertStatus(previousFlow, "jobc", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerJobA", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobc", Status.RUNNING);
    assertStatus(pipelineFlow, "jobb:innerJobB", Status.RUNNING);
    assertStatus(pipelineFlow, "jobb:innerJobC", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobc").succeedJob();
    assertStatus(previousFlow, "jobb:innerFlow", Status.READY);
    assertStatus(previousFlow, "jobb", Status.SKIPPED);
    assertStatus(previousFlow, "jobe", Status.RUNNING);
    assertStatus(pipelineFlow, "jobc", Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("pipe:jobb:innerJobB").succeedJob();
    InteractiveTestJob.getTestJob("pipe:jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("prev:jobe").succeedJob();
    assertStatus(previousFlow, "jobe", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerJobB", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerJobC", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerFlow", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobd:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("pipe:jobb:innerFlow").succeedJob();
    assertStatus(pipelineFlow, "jobb", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobd:innerJobA", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerFlow", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobd:innerFlow2", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobd:innerFlow2").succeedJob();
    InteractiveTestJob.getTestJob("prev:joba1").succeedJob();
    assertStatus(pipelineFlow, "jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobd", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobf", Status.RUNNING);
    assertStatus(previousFlow, "joba1", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "joba1", Status.RUNNING);
    assertStatus(pipelineFlow, "jobe", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobe").succeedJob();
    InteractiveTestJob.getTestJob("prev:jobf").succeedJob();
    assertStatus(pipelineFlow, "jobe", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobf", Status.SUCCEEDED);
    assertFlowStatus(previousFlow, Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("pipe:joba1").succeedJob();
    assertStatus(pipelineFlow, "joba1", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobf", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobf").succeedJob();

    assertThreadShutDown(previousRunner);
    assertThreadShutDown(pipelineRunner);
    assertFlowStatus(pipelineFlow, Status.SUCCEEDED);
  }

  @Test
  public void testBasicPipelineLevel1Run() throws Exception {
    final FlowRunner previousRunner = this.testUtil.createFromFlowMap("jobf", "prev");

    final ExecutionOptions options = new ExecutionOptions();
    options.setPipelineExecutionId(previousRunner.getExecutableFlow()
        .getExecutionId());
    options.setPipelineLevel(1);
    final FlowWatcher watcher = new LocalFlowWatcher(previousRunner);
    final FlowRunner pipelineRunner = this.testUtil.createFromFlowMap("jobf", "pipe", options);
    pipelineRunner.setFlowWatcher(watcher);

    // 1. START FLOW
    final ExecutableFlow pipelineFlow = pipelineRunner.getExecutableFlow();
    final ExecutableFlow previousFlow = previousRunner.getExecutableFlow();

    FlowRunnerTestUtil.startThread(previousRunner);
    assertStatus(previousFlow, "joba", Status.RUNNING);
    assertStatus(previousFlow, "joba", Status.RUNNING);
    assertStatus(previousFlow, "joba1", Status.RUNNING);

    FlowRunnerTestUtil.startThread(pipelineRunner);
    assertStatus(pipelineFlow, "joba", Status.QUEUED);
    assertStatus(pipelineFlow, "joba1", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:joba").succeedJob();
    assertStatus(previousFlow, "joba", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobb", Status.RUNNING);
    assertStatus(previousFlow, "jobb:innerJobA", Status.RUNNING);
    assertStatus(previousFlow, "jobd", Status.RUNNING);
    assertStatus(previousFlow, "jobc", Status.RUNNING);
    assertStatus(previousFlow, "jobd:innerJobA", Status.RUNNING);
    assertStatus(pipelineFlow, "joba", Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:jobb:innerJobA").succeedJob();
    assertStatus(previousFlow, "jobb:innerJobA", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobb:innerJobB", Status.RUNNING);
    assertStatus(previousFlow, "jobb:innerJobC", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:joba").succeedJob();
    assertStatus(pipelineFlow, "joba", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb", Status.RUNNING);
    assertStatus(pipelineFlow, "jobd", Status.RUNNING);
    assertStatus(pipelineFlow, "jobc", Status.QUEUED);
    assertStatus(pipelineFlow, "jobd:innerJobA", Status.QUEUED);
    assertStatus(pipelineFlow, "jobb:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:jobd:innerJobA").succeedJob();
    assertStatus(previousFlow, "jobd:innerJobA", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobd:innerFlow2", Status.RUNNING);
    assertStatus(pipelineFlow, "jobd:innerJobA", Status.RUNNING);

    // Finish the previous d side
    InteractiveTestJob.getTestJob("prev:jobd:innerFlow2").succeedJob();
    assertStatus(previousFlow, "jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobd", Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("prev:jobb:innerJobB").succeedJob();
    InteractiveTestJob.getTestJob("prev:jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("prev:jobc").succeedJob();
    InteractiveTestJob.getTestJob("pipe:jobb:innerJobA").succeedJob();
    assertStatus(previousFlow, "jobb:innerJobB", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobb:innerJobC", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobb:innerFlow", Status.RUNNING);
    assertStatus(previousFlow, "jobc", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerJobA", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobc", Status.RUNNING);
    assertStatus(pipelineFlow, "jobb:innerJobB", Status.RUNNING);
    assertStatus(pipelineFlow, "jobb:innerJobC", Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:jobb:innerFlow").succeedJob();
    InteractiveTestJob.getTestJob("pipe:jobc").succeedJob();
    assertStatus(previousFlow, "jobb:innerFlow", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobb", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobe", Status.RUNNING);
    assertStatus(pipelineFlow, "jobc", Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("pipe:jobb:innerJobB").succeedJob();
    InteractiveTestJob.getTestJob("pipe:jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("prev:jobe").succeedJob();
    assertStatus(previousFlow, "jobe", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerJobB", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerJobC", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerFlow", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobd:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("pipe:jobb:innerFlow").succeedJob();
    assertStatus(pipelineFlow, "jobb", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobd:innerJobA", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobb:innerFlow", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobd:innerFlow2", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobd:innerFlow2").succeedJob();
    InteractiveTestJob.getTestJob("prev:joba1").succeedJob();
    assertStatus(pipelineFlow, "jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobd", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobf", Status.RUNNING);
    assertStatus(previousFlow, "joba1", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "joba1", Status.RUNNING);
    assertStatus(pipelineFlow, "jobe", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobe").succeedJob();
    InteractiveTestJob.getTestJob("prev:jobf").succeedJob();
    assertStatus(pipelineFlow, "jobe", Status.SUCCEEDED);
    assertStatus(previousFlow, "jobf", Status.SUCCEEDED);
    assertFlowStatus(previousFlow, Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("pipe:joba1").succeedJob();
    assertStatus(pipelineFlow, "joba1", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "jobf", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:jobf").succeedJob();

    assertThreadShutDown(previousRunner);
    assertThreadShutDown(pipelineRunner);
    assertFlowStatus(pipelineFlow, Status.SUCCEEDED);
  }

  @Test
  public void testBasicPipelineLevel2Run() throws Exception {
    final FlowRunner previousRunner = this.testUtil.createFromFlowMap("pipelineFlow", "prev");

    final ExecutionOptions options = new ExecutionOptions();
    options.setPipelineExecutionId(previousRunner.getExecutableFlow()
        .getExecutionId());
    options.setPipelineLevel(2);
    final FlowWatcher watcher = new LocalFlowWatcher(previousRunner);
    final FlowRunner pipelineRunner = this.testUtil
        .createFromFlowMap("pipelineFlow", "pipe", options);
    pipelineRunner.setFlowWatcher(watcher);

    // 1. START FLOW
    final ExecutableFlow pipelineFlow = pipelineRunner.getExecutableFlow();
    final ExecutableFlow previousFlow = previousRunner.getExecutableFlow();

    FlowRunnerTestUtil.startThread(previousRunner);
    assertStatus(previousFlow, "pipeline1", Status.RUNNING);

    FlowRunnerTestUtil.startThread(pipelineRunner);
    assertStatus(pipelineFlow, "pipeline1", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipeline1").succeedJob();
    assertStatus(previousFlow, "pipeline1", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline2", Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:pipeline2").succeedJob();
    assertStatus(previousFlow, "pipeline2", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3", Status.RUNNING);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.RUNNING);
    assertStatus(pipelineFlow, "pipeline1", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline1").succeedJob();
    assertStatus(pipelineFlow, "pipeline1", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline2", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipelineEmbeddedFlow3:innerJobA")
        .succeedJob();
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.RUNNING);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.RUNNING);
    assertStatus(pipelineFlow, "pipeline2", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline2").succeedJob();
    assertStatus(pipelineFlow, "pipeline2", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3", Status.RUNNING);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipelineEmbeddedFlow3:innerJobB")
        .succeedJob();
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("prev:pipelineEmbeddedFlow3:innerJobC")
        .succeedJob();
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.RUNNING);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipelineEmbeddedFlow3:innerJobA")
        .succeedJob();
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.QUEUED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.QUEUED);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:pipelineEmbeddedFlow3:innerFlow")
        .succeedJob();
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline4", Status.RUNNING);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.RUNNING);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipelineEmbeddedFlow3:innerJobB")
        .succeedJob();
    InteractiveTestJob.getTestJob("pipe:pipelineEmbeddedFlow3:innerJobC")
        .succeedJob();
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipeline4").succeedJob();
    assertStatus(previousFlow, "pipeline4", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineFlow", Status.RUNNING);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:pipelineFlow").succeedJob();
    assertStatus(previousFlow, "pipelineFlow", Status.SUCCEEDED);
    assertFlowStatus(previousFlow, Status.SUCCEEDED);
    assertThreadShutDown(previousRunner);

    InteractiveTestJob.getTestJob("pipe:pipelineEmbeddedFlow3:innerFlow")
        .succeedJob();
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline4", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline4").succeedJob();
    assertStatus(pipelineFlow, "pipeline4", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineFlow", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipelineFlow").succeedJob();
    assertStatus(pipelineFlow, "pipelineFlow", Status.SUCCEEDED);
    assertFlowStatus(pipelineFlow, Status.SUCCEEDED);
    assertThreadShutDown(pipelineRunner);

  }

  @Test
  public void testBasicPipelineLevel2Run2() throws Exception {
    final FlowRunner previousRunner = this.testUtil.createFromFlowMap("pipeline1_2", "prev");

    final ExecutionOptions options = new ExecutionOptions();
    options.setPipelineExecutionId(previousRunner.getExecutableFlow()
        .getExecutionId());
    options.setPipelineLevel(2);
    final FlowWatcher watcher = new LocalFlowWatcher(previousRunner);
    final FlowRunner pipelineRunner = this.testUtil
        .createFromFlowMap("pipeline1_2", "pipe", options);
    pipelineRunner.setFlowWatcher(watcher);

    // 1. START FLOW
    final ExecutableFlow pipelineFlow = pipelineRunner.getExecutableFlow();
    final ExecutableFlow previousFlow = previousRunner.getExecutableFlow();

    FlowRunnerTestUtil.startThread(previousRunner);
    assertStatus(previousFlow, "pipeline1_1", Status.RUNNING);
    assertStatus(previousFlow, "pipeline1_1:innerJobA", Status.RUNNING);

    FlowRunnerTestUtil.startThread(pipelineRunner);
    assertStatus(pipelineFlow, "pipeline1_1", Status.RUNNING);
    assertStatus(pipelineFlow, "pipeline1_1:innerJobA", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipeline1_1:innerJobA").succeedJob();
    assertStatus(previousFlow, "pipeline1_1:innerJobA", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline1_1:innerFlow2", Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:pipeline1_1:innerFlow2").succeedJob();
    assertStatus(previousFlow, "pipeline1_1", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline1_1:innerFlow2", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline1_2", Status.RUNNING);
    assertStatus(previousFlow, "pipeline1_2:innerJobA", Status.RUNNING);
    assertStatus(pipelineFlow, "pipeline1_1:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline1_1:innerJobA").succeedJob();
    assertStatus(pipelineFlow, "pipeline1_1:innerJobA", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline1_1:innerFlow2", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipeline1_2:innerJobA").succeedJob();
    assertStatus(previousFlow, "pipeline1_2:innerJobA", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline1_2:innerFlow2", Status.RUNNING);
    assertStatus(pipelineFlow, "pipeline1_1:innerFlow2", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline1_1:innerFlow2").succeedJob();
    assertStatus(pipelineFlow, "pipeline1_1", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline1_1:innerFlow2", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline1_2", Status.RUNNING);
    assertStatus(pipelineFlow, "pipeline1_2:innerJobA", Status.QUEUED);

    InteractiveTestJob.getTestJob("pipe:pipeline1_1:innerFlow2").succeedJob();
    assertStatus(pipelineFlow, "pipeline1_1", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline1_1:innerFlow2", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline1_2", Status.RUNNING);
    assertStatus(pipelineFlow, "pipeline1_2:innerJobA", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipeline1_2:innerFlow2").succeedJob();
    assertStatus(previousFlow, "pipeline1_2:innerFlow2", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline1_2", Status.SUCCEEDED);
    assertFlowStatus(previousFlow, Status.SUCCEEDED);
    assertThreadShutDown(previousRunner);
    assertStatus(pipelineFlow, "pipeline1_2:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline1_2:innerJobA").succeedJob();
    assertStatus(pipelineFlow, "pipeline1_2:innerJobA", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline1_2:innerFlow2", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline1_2:innerFlow2").succeedJob();
    assertStatus(pipelineFlow, "pipeline1_2", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline1_2:innerFlow2", Status.SUCCEEDED);
    assertFlowStatus(pipelineFlow, Status.SUCCEEDED);
    assertThreadShutDown(pipelineRunner);
  }

  @Test
  public void testBasicPipelineLevel3Run() throws Exception {
    final FlowRunner previousRunner = this.testUtil.createFromFlowMap("pipelineFlow", "prev");

    final ExecutionOptions options = new ExecutionOptions();
    options.setPipelineExecutionId(previousRunner.getExecutableFlow()
        .getExecutionId());
    options.setPipelineLevel(3);
    final FlowWatcher watcher = new LocalFlowWatcher(previousRunner);
    final FlowRunner pipelineRunner = this.testUtil
        .createFromFlowMap("pipelineFlow", "pipe", options);
    pipelineRunner.setFlowWatcher(watcher);

    // 1. START FLOW
    final ExecutableFlow pipelineFlow = pipelineRunner.getExecutableFlow();
    final ExecutableFlow previousFlow = previousRunner.getExecutableFlow();

    FlowRunnerTestUtil.startThread(previousRunner);
    assertStatus(previousFlow, "pipeline1", Status.RUNNING);

    FlowRunnerTestUtil.startThread(pipelineRunner);
    assertStatus(pipelineFlow, "pipeline1", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipeline1").succeedJob();
    assertStatus(previousFlow, "pipeline1", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline2", Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:pipeline2").succeedJob();
    assertStatus(previousFlow, "pipeline2", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3", Status.RUNNING);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:pipelineEmbeddedFlow3:innerJobA")
        .succeedJob();
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.RUNNING);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("prev:pipelineEmbeddedFlow3:innerJobB")
        .succeedJob();
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("prev:pipelineEmbeddedFlow3:innerJobC")
        .succeedJob();
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.RUNNING);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.RUNNING);
    InteractiveTestJob.getTestJob("prev:pipelineEmbeddedFlow3:innerFlow")
        .succeedJob();
    assertStatus(previousFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineEmbeddedFlow3", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipeline4", Status.RUNNING);
    InteractiveTestJob.getTestJob("prev:pipeline4").succeedJob();
    assertStatus(previousFlow, "pipeline4", Status.SUCCEEDED);
    assertStatus(previousFlow, "pipelineFlow", Status.RUNNING);

    // Should still be queued until the last job in the previous flow has finished.
    assertStatus(pipelineFlow, "pipeline1", Status.QUEUED);

    InteractiveTestJob.getTestJob("prev:pipelineFlow").succeedJob();
    assertStatus(previousFlow, "pipelineFlow", Status.SUCCEEDED);
    assertFlowStatus(previousFlow, Status.SUCCEEDED);
    assertThreadShutDown(previousRunner);

    assertStatus(pipelineFlow, "pipeline1", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline1").succeedJob();
    assertStatus(pipelineFlow, "pipeline1", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline2", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline2").succeedJob();
    assertStatus(pipelineFlow, "pipeline2", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3", Status.RUNNING);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipelineEmbeddedFlow3:innerJobA")
        .succeedJob();
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobA",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.RUNNING);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipelineEmbeddedFlow3:innerJobB")
        .succeedJob();
    InteractiveTestJob.getTestJob("pipe:pipelineEmbeddedFlow3:innerJobC")
        .succeedJob();
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobC",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerJobB",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipelineEmbeddedFlow3:innerFlow")
        .succeedJob();
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3:innerFlow",
        Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineEmbeddedFlow3", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipeline4", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipeline4").succeedJob();
    assertStatus(pipelineFlow, "pipeline4", Status.SUCCEEDED);
    assertStatus(pipelineFlow, "pipelineFlow", Status.RUNNING);

    InteractiveTestJob.getTestJob("pipe:pipelineFlow").succeedJob();
    assertStatus(pipelineFlow, "pipelineFlow", Status.SUCCEEDED);
    assertFlowStatus(pipelineFlow, Status.SUCCEEDED);
    assertThreadShutDown(pipelineRunner);
  }
}
