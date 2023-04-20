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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_WEBSERVER_URL;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.Status;
import azkaban.flow.CommonJobProperties;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the flow run, especially with embedded flows.
 *
 * This test uses executions/embedded2. It also mainly uses the flow named jobf. The test is
 * designed to control success/failures explicitly so we don't have to time the flow exactly.
 *
 * Flow jobf looks like the following:
 *
 * <pre>
 *       joba       joba1
 *      /  |  \      |
 *     /   |   \     |
 *  jobb  jobd jobc  |
 *     \   |   /    /
 *      \  |  /    /
 *        jobe    /
 *         |     /
 *         |    /
 *        jobf
 * </pre>
 *
 * The job 'jobb' is an embedded flow:
 *
 * jobb:innerFlow
 *
 * <pre>
 *        innerJobA
 *        /       \
 *   innerJobB   innerJobC
 *        \       /
 *        innerFlow
 *
 * </pre>
 *
 * The job 'jobd' is a simple embedded flow:
 *
 * jobd:innerFlow2
 *
 * <pre>
 *       innerJobA
 *           |
 *       innerFlow2
 * </pre>
 *
 * The following tests checks each stage of the flow run by forcing jobs to succeed or fail.
 */
public class FlowRunnerTest2 extends FlowRunnerTestBase {

  private FlowRunnerTestUtil testUtil;

  @Before
  public void setUp() throws Exception {
    this.testUtil = new FlowRunnerTestUtil("embedded2", this.temporaryFolder);
  }

  /**
   * Tests the basic successful flow run, and also tests all output variables from each job.
   */
  @Test
  public void testBasicRun() throws Exception {
    final Map<String, String> flowParams = new HashMap<>();
    flowParams.put("param4", "override.4");
    flowParams.put("param10", "override.10");
    flowParams.put("param11", "override.11");

    final ExecutionOptions options = new ExecutionOptions();
    options.setFailureAction(FailureAction.FINISH_CURRENTLY_RUNNING);

    Props props = new Props();
    props.put(AZKABAN_WEBSERVER_URL, "http://localhost:8443");

    this.runner = this.testUtil.createFromFlowMap("jobf", options, flowParams, props);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    final Props joba = this.runner.getExecutableFlow().getExecutableNodePath("joba")
        .getInputProps();
    assertEquals("joba.1", joba.get("param1"));
    assertEquals("test1.2", joba.get("param2"));
    assertEquals("test1.3", joba.get("param3"));
    assertEquals("override.4", joba.get("param4"));
    assertEquals("test2.5", joba.get("param5"));
    assertEquals("test2.6", joba.get("param6"));
    assertEquals("test2.7", joba.get("param7"));
    assertEquals("test2.8", joba.get("param8"));
    assertThat(joba.get(CommonJobProperties.JOB_ID)).isEqualTo("joba");
    assertThat(joba.get(CommonJobProperties.JOB_ATTEMPT)).isEqualTo("0");
    assertThat(joba.get(CommonJobProperties.EXECUTION_LINK))
        .matches("http://localhost:8443/executor\\?execid=\\d\\d\\d");
    assertThat(joba.get(CommonJobProperties.WORKFLOW_LINK))
        .matches("http://localhost:8443/manager\\?project=testProject&flow=jobf");
    assertThat(joba.get(CommonJobProperties.JOBEXEC_LINK))
        .matches("http://localhost:8443/executor\\?execid=\\d\\d\\d&job=joba");
    assertThat(joba.get(CommonJobProperties.JOB_LINK))
        .matches("http://localhost:8443/manager\\?project=testProject&flow=jobf&job=joba");
    assertThat(joba.get(CommonJobProperties.ATTEMPT_LINK))
        .matches("http://localhost:8443/executor\\?execid=\\d\\d\\d&job=joba&attempt=0");

    final Props joba1 = this.runner.getExecutableFlow().getExecutableNodePath("joba1")
        .getInputProps();
    assertEquals("test1.1", joba1.get("param1"));
    assertEquals("test1.2", joba1.get("param2"));
    assertEquals("test1.3", joba1.get("param3"));
    assertEquals("override.4", joba1.get("param4"));
    assertEquals("test2.5", joba1.get("param5"));
    assertEquals("test2.6", joba1.get("param6"));
    assertEquals("test2.7", joba1.get("param7"));
    assertEquals("test2.8", joba1.get("param8"));

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob(
        Props.of("output.joba", "joba", "output.override", "joba"));
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);

    final ExecutableNode node = this.runner.getExecutableFlow().getExecutableNodePath("jobb");
    assertEquals(Status.RUNNING, node.getStatus());
    final Props jobb = node.getInputProps();
    assertEquals("override.4", jobb.get("param4"));
    // Test that jobb properties overwrites the output properties
    assertEquals("moo", jobb.get("testprops"));
    assertEquals("jobb", jobb.get("output.override"));
    assertEquals("joba", jobb.get("output.joba"));

    final Props jobbInnerJobA = this.runner.getExecutableFlow()
        .getExecutableNodePath("jobb:innerJobA")
        .getInputProps();
    assertEquals("test1.1", jobbInnerJobA.get("param1"));
    assertEquals("test1.2", jobbInnerJobA.get("param2"));
    assertEquals("test1.3", jobbInnerJobA.get("param3"));
    assertEquals("override.4", jobbInnerJobA.get("param4"));
    assertEquals("test2.5", jobbInnerJobA.get("param5"));
    assertEquals("test2.6", jobbInnerJobA.get("param6"));
    assertEquals("test2.7", jobbInnerJobA.get("param7"));
    assertEquals("test2.8", jobbInnerJobA.get("param8"));
    assertEquals("joba", jobbInnerJobA.get("output.joba"));
    assertThat(jobbInnerJobA.get(CommonJobProperties.JOB_ID)).isEqualTo("innerJobA");
    assertThat(jobbInnerJobA.get(CommonJobProperties.JOB_ATTEMPT)).isEqualTo("0");
    assertThat(jobbInnerJobA.get(CommonJobProperties.EXECUTION_LINK))
        .matches("http://localhost:8443/executor\\?execid=\\d\\d\\d");
    assertThat(jobbInnerJobA.get(CommonJobProperties.WORKFLOW_LINK))
        .matches("http://localhost:8443/manager\\?project=testProject&flow=innerFlow");
    assertThat(jobbInnerJobA.get(CommonJobProperties.JOBEXEC_LINK))
        .matches("http://localhost:8443/executor\\?execid=\\d\\d\\d&job=jobb:innerJobA");
    assertThat(jobbInnerJobA.get(CommonJobProperties.JOB_LINK))
        .matches("http://localhost:8443/manager\\?project=testProject&flow=innerFlow&job=innerJobA");
    assertThat(jobbInnerJobA.get(CommonJobProperties.ATTEMPT_LINK))
        .matches("http://localhost:8443/executor\\?execid=\\d\\d\\d&job=jobb:innerJobA&attempt=0");


    // 3. jobb:Inner completes
    /// innerJobA completes
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob(
        Props.of("output.jobb.innerJobA", "jobb.innerJobA"));
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);
    final Props jobbInnerJobB = this.runner.getExecutableFlow()
        .getExecutableNodePath("jobb:innerJobB")
        .getInputProps();
    assertEquals("test1.1", jobbInnerJobB.get("param1"));
    assertEquals("override.4", jobbInnerJobB.get("param4"));
    assertEquals("jobb.innerJobA",
        jobbInnerJobB.get("output.jobb.innerJobA"));
    assertEquals("moo", jobbInnerJobB.get("testprops"));
    /// innerJobB, C completes
    InteractiveTestJob.getTestJob("jobb:innerJobB").succeedJob(
        Props.of("output.jobb.innerJobB", "jobb.innerJobB"));
    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob(
        Props.of("output.jobb.innerJobC", "jobb.innerJobC"));
    assertStatus("jobb:innerJobB", Status.SUCCEEDED);
    assertStatus("jobb:innerJobC", Status.SUCCEEDED);
    assertStatus("jobb:innerFlow", Status.RUNNING);

    final Props jobbInnerJobD = this.runner.getExecutableFlow()
        .getExecutableNodePath("jobb:innerFlow")
        .getInputProps();
    assertEquals("test1.1", jobbInnerJobD.get("param1"));
    assertEquals("override.4", jobbInnerJobD.get("param4"));
    assertEquals("jobb.innerJobB",
        jobbInnerJobD.get("output.jobb.innerJobB"));
    assertEquals("jobb.innerJobC",
        jobbInnerJobD.get("output.jobb.innerJobC"));

    // 4. Finish up on inner flow for jobb
    InteractiveTestJob.getTestJob("jobb:innerFlow").succeedJob(
        Props.of("output1.jobb", "test1", "output2.jobb", "test2"));
    assertStatus("jobb:innerFlow", Status.SUCCEEDED);
    assertStatus("jobb", Status.SUCCEEDED);
    final Props jobbOutput = this.runner.getExecutableFlow().getExecutableNodePath("jobb")
        .getOutputProps();
    assertEquals("test1", jobbOutput.get("output1.jobb"));
    assertEquals("test2", jobbOutput.get("output2.jobb"));

    // 5. Finish jobc, jobd
    InteractiveTestJob.getTestJob("jobc").succeedJob(
        Props.of("output.jobc", "jobc"));
    assertStatus("jobc", Status.SUCCEEDED);
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    assertStatus("jobd:innerJobA", Status.SUCCEEDED);
    assertStatus("jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus("jobd", Status.SUCCEEDED);
    assertStatus("jobe", Status.RUNNING);

    final Props jobd = this.runner.getExecutableFlow().getExecutableNodePath("jobe")
        .getInputProps();
    assertEquals("test1", jobd.get("output1.jobb"));
    assertEquals("jobc", jobd.get("output.jobc"));

    // 6. Finish off flow
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobe").succeedJob();
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobe", Status.SUCCEEDED);
    assertStatus("jobf", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobf").succeedJob();
    assertStatus("jobf", Status.SUCCEEDED);
    waitForAndAssertFlowStatus(Status.SUCCEEDED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests a flow with Disabled jobs and flows. They should properly SKIP executions
   */
  @Test
  public void testDisabledNormal() throws Exception {
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    flow.getExecutableNode("jobb").setStatus(Status.DISABLED);
    ((ExecutableFlowBase) flow.getExecutableNode("jobd")).getExecutableNode(
        "innerJobA").setStatus(Status.DISABLED);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB A COMPLETES SUCCESSFULLY, others should be skipped
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.SKIPPED);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.SKIPPED);
    assertStatus("jobd:innerFlow2", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.READY);
    assertStatus("jobb:innerJobB", Status.READY);
    assertStatus("jobb:innerJobC", Status.READY);
    assertStatus("jobb:innerFlow", Status.READY);

    // 3. jobb:Inner completes
    /// innerJobA completes
    InteractiveTestJob.getTestJob("jobc").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    assertStatus("jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus("jobd", Status.SUCCEEDED);
    assertStatus("jobc", Status.SUCCEEDED);
    assertStatus("jobe", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobe").succeedJob();
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    assertStatus("jobe", Status.SUCCEEDED);
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobf", Status.RUNNING);

    // 4. Finish up on inner flow for jobb
    InteractiveTestJob.getTestJob("jobf").succeedJob();
    assertStatus("jobf", Status.SUCCEEDED);

    waitForAndAssertFlowStatus(Status.SUCCEEDED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests a failure with the default FINISH_CURRENTLY_RUNNING. After the first failure, every job
   * that started should complete, and the rest of the jobs should be skipped.
   */
  @Test
  public void testNormalFailure1() throws Exception {
    // Test propagation of KILLED status to embedded flows.
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB A COMPLETES SUCCESSFULLY, others should be skipped
    InteractiveTestJob.getTestJob("joba").failJob();
    waitForAndAssertFlowStatus(Status.FAILED_FINISHING);
    assertStatus("joba", Status.FAILED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.CANCELLED);
    assertStatus("jobc", Status.CANCELLED);
    assertStatus("jobd", Status.CANCELLED);
    assertStatus("jobd:innerJobA", Status.READY);
    assertStatus("jobd:innerFlow2", Status.READY);
    assertStatus("jobb:innerJobA", Status.READY);
    assertStatus("jobb:innerFlow", Status.READY);
    assertStatus("jobe", Status.CANCELLED);

    // 3. jobb:Inner completes
    /// innerJobA completes
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    assertStatus("jobf", Status.CANCELLED);
    waitForAndAssertFlowStatus(Status.FAILED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Test #2 on the default failure case.
   */
  @Test
  public void testNormalFailure2() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB A COMPLETES SUCCESSFULLY, others should be skipped
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("joba1").failJob();
    assertStatus("joba1", Status.FAILED);
    waitForAndAssertFlowStatus(Status.FAILED_FINISHING);

    // 3. joba completes, everything is killed
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("jobd:innerJobA", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.CANCELLED);
    assertStatus("jobb:innerJobC", Status.CANCELLED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobb", Status.KILLED);
    assertStatus("jobd", Status.KILLED);
    waitForAndAssertFlowStatus(Status.FAILED_FINISHING);

    InteractiveTestJob.getTestJob("jobc").succeedJob();
    assertStatus("jobc", Status.SUCCEEDED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);
    waitForAndAssertFlowStatus(Status.FAILED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  @Test
  public void testNormalFailure3() throws Exception {
    // Test propagation of CANCELLED status to embedded flows different branch
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    assertStatus("jobb", Status.FAILED_FINISHING);
    assertStatus("jobb:innerJobB", Status.FAILED);
    waitForAndAssertFlowStatus(Status.FAILED_FINISHING);

    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    assertStatus("jobd:innerJobA", Status.SUCCEEDED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobd", Status.KILLED);
    assertStatus("jobb:innerJobC", Status.SUCCEEDED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);
    assertStatus("jobb", Status.FAILED);

    // 3. jobc completes, everything is killed
    InteractiveTestJob.getTestJob("jobc").succeedJob();
    assertStatus("jobc", Status.SUCCEEDED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);
    waitForAndAssertFlowStatus(Status.FAILED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests failures when the fail behaviour is FINISH_ALL_POSSIBLE. In this case, all jobs which
   * have had its pre-requisite met can continue to run. Finishes when the failure is propagated to
   * the last node of the flow.
   */
  @Test
  public void testFailedFinishingFailure3() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_ALL_POSSIBLE);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    assertStatus("jobb", Status.FAILED_FINISHING);
    assertStatus("jobb:innerJobB", Status.FAILED);
    waitForAndAssertFlowStatus(Status.FAILED_FINISHING);

    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    assertStatus("jobb", Status.FAILED);
    assertStatus("jobd:innerJobA", Status.SUCCEEDED);
    assertStatus("jobd:innerFlow2", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.SUCCEEDED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);

    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    assertStatus("jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus("jobd", Status.SUCCEEDED);

    // 3. jobc completes, everything is killed
    InteractiveTestJob.getTestJob("jobc").succeedJob();
    assertStatus("jobc", Status.SUCCEEDED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);
    waitForAndAssertFlowStatus(Status.FAILED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests the failure condition when a failure invokes a cancel (or killed) on the flow.
   *
   * Any jobs that are running will be assigned a KILLED state, and any nodes which were skipped due
   * to prior errors will be given a CANCELLED state.
   */
  @Test
  public void testCancelOnFailure() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.CANCEL_ALL);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    assertStatus("jobb", Status.FAILED);
    assertStatus("jobb:innerJobB", Status.FAILED);
    assertStatus("jobb:innerJobC", Status.KILLED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);
    assertStatus("jobc", Status.KILLED);
    assertStatus("jobd", Status.KILLED);
    assertStatus("jobd:innerJobA", Status.KILLED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);

    waitForAndAssertFlowStatus(Status.KILLED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests retries after a failure
   */
  @Test
  public void testRetryOnFailure() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);
    final ExecutableFlow flow = this.runner.getExecutableFlow();
    flow.getExecutableNode("joba").setStatus(Status.DISABLED);
    ((ExecutableFlowBase) flow.getExecutableNode("jobb")).getExecutableNode(
        "innerFlow").setStatus(Status.DISABLED);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    assertStatus("joba", Status.SKIPPED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    InteractiveTestJob.getTestJob("jobb:innerJobC").failJob();
    assertStatus("jobb:innerJobB", Status.FAILED);
    assertStatus("jobb:innerJobC", Status.FAILED);
    assertStatus("jobb", Status.FAILED);
    assertStatus("jobb:innerFlow", Status.SKIPPED);
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    assertStatus("jobd:innerJobA", Status.SUCCEEDED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobd", Status.KILLED);
    waitForAndAssertFlowStatus(Status.FAILED_FINISHING);

    final ExecutableNode node = this.runner.getExecutableFlow()
        .getExecutableNodePath("jobd:innerFlow2");
    final ExecutableFlowBase base = node.getParentFlow();
    for (final String nodeId : node.getInNodes()) {
      final ExecutableNode inNode = base.getExecutableNode(nodeId);
      System.out.println(inNode.getId() + " > " + inNode.getStatus());
    }

    assertStatus("jobb:innerFlow", Status.SKIPPED);
    InteractiveTestJob.clearTestJobs("jobb:innerJobB", "jobb:innerJobC");
    this.runner.retryFailures("me");
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobb:innerFlow", Status.DISABLED);
    assertStatus("jobd:innerFlow2", Status.RUNNING);
    waitForAndAssertFlowStatus(Status.RUNNING);
    assertThreadRunning();

    InteractiveTestJob.getTestJob("jobb:innerJobB").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    InteractiveTestJob.getTestJob("jobc").succeedJob();
    assertStatus("jobb:innerFlow", Status.SKIPPED);
    assertStatus("jobb", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.SUCCEEDED);
    assertStatus("jobb:innerJobC", Status.SUCCEEDED);
    assertStatus("jobc", Status.SUCCEEDED);
    assertStatus("jobd", Status.SUCCEEDED);
    assertStatus("jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus("jobe", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobe").succeedJob();
    assertStatus("jobe", Status.SUCCEEDED);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobf", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobf").succeedJob();
    assertStatus("jobf", Status.SUCCEEDED);
    waitForAndAssertFlowStatus(Status.SUCCEEDED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests the manual Killing of a flow. In this case, the flow is just fine before the cancel is
   * called.
   */
  @Test
  public void testCancel() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.CANCEL_ALL);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);

    this.runner.kill("me");

    assertStatus("jobb", Status.KILLED);
    assertStatus("jobb:innerJobB", Status.KILLED);
    assertStatus("jobb:innerJobC", Status.KILLED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);
    assertStatus("jobc", Status.KILLED);
    assertStatus("jobd", Status.KILLED);
    assertStatus("jobd:innerJobA", Status.KILLED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);

    waitForAndAssertFlowStatus(Status.KILLED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests the manual invocation of cancel on a flow that is FAILED_FINISHING
   */
  @Test
  public void testManualCancelOnFailure() throws Exception {
    // Test propagation of KILLED status to embedded flows different branch
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB in subflow FAILS
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);

    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobb:innerJobB").failJob();
    assertStatus("jobb:innerJobB", Status.FAILED);
    assertStatus("jobb", Status.FAILED_FINISHING);
    waitForAndAssertFlowStatus(Status.FAILED_FINISHING);

    this.runner.kill("me");

    assertStatus("jobb", Status.FAILED);
    assertStatus("jobb:innerJobC", Status.KILLED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);
    assertStatus("jobc", Status.KILLED);
    assertStatus("jobd", Status.KILLED);
    assertStatus("jobd:innerJobA", Status.KILLED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);

    waitForAndAssertFlowStatus(Status.KILLED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests that pause and resume work
   */
  @Test
  public void testPause() throws Exception {
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    this.runner.pause("test");
    InteractiveTestJob.getTestJob("joba").succeedJob();
    // 2.1 JOB A COMPLETES SUCCESSFULLY AFTER PAUSE
    assertStatus("joba", Status.SUCCEEDED);
    waitForAndAssertFlowStatus(Status.PAUSED);

    // 2.2 Flow is unpaused
    this.runner.resume("test");
    waitForAndAssertFlowStatus(Status.RUNNING);
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);

    // 3. jobb:Inner completes
    this.runner.pause("test");

    /// innerJobA completes, but paused
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob(
        Props.of("output.jobb.innerJobA", "jobb.innerJobA"));
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);

    this.runner.resume("test");
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);

    /// innerJobB, C completes
    InteractiveTestJob.getTestJob("jobb:innerJobB").succeedJob(
        Props.of("output.jobb.innerJobB", "jobb.innerJobB"));
    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob(
        Props.of("output.jobb.innerJobC", "jobb.innerJobC"));
    assertStatus("jobb:innerJobB", Status.SUCCEEDED);
    assertStatus("jobb:innerJobC", Status.SUCCEEDED);
    assertStatus("jobb:innerFlow", Status.RUNNING);

    // 4. Finish up on inner flow for jobb
    InteractiveTestJob.getTestJob("jobb:innerFlow").succeedJob(
        Props.of("output1.jobb", "test1", "output2.jobb", "test2"));
    assertStatus("jobb:innerFlow", Status.SUCCEEDED);
    assertStatus("jobb", Status.SUCCEEDED);

    // 5. Finish jobc, jobd
    InteractiveTestJob.getTestJob("jobc").succeedJob(
        Props.of("output.jobc", "jobc"));
    assertStatus("jobc", Status.SUCCEEDED);
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerFlow2").succeedJob();
    assertStatus("jobd:innerJobA", Status.SUCCEEDED);
    assertStatus("jobd:innerFlow2", Status.SUCCEEDED);
    assertStatus("jobd", Status.SUCCEEDED);
    assertStatus("jobe", Status.RUNNING);

    // 6. Finish off flow
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobe").succeedJob();
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobe", Status.SUCCEEDED);
    assertStatus("jobf", Status.RUNNING);

    InteractiveTestJob.getTestJob("jobf").succeedJob();
    assertStatus("jobf", Status.SUCCEEDED);
    waitForAndAssertFlowStatus(Status.SUCCEEDED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Test the condition for a manual invocation of a KILL (cancel) on a flow that has been paused.
   * The flow should unpause and be killed immediately.
   */
  @Test
  public void testPauseKill() throws Exception {
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);

    this.runner.pause("me");
    waitForAndAssertFlowStatus(Status.PAUSED);
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").succeedJob();
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    assertStatus("jobd:innerJobA", Status.SUCCEEDED);

    this.runner.kill("me");
    assertStatus("joba1", Status.KILLED);
    assertStatus("jobb:innerJobB", Status.CANCELLED);
    assertStatus("jobb:innerJobC", Status.CANCELLED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);
    assertStatus("jobb", Status.KILLED);
    assertStatus("jobc", Status.KILLED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobd", Status.KILLED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);

    waitForAndAssertFlowStatus(Status.KILLED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests the case where a failure occurs on a Paused flow. In this case, the flow should stay
   * paused.
   */
  @Test
  public void testPauseFail() throws Exception {
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_CURRENTLY_RUNNING);
    final EventCollectorListener eventCollector = new EventCollectorListener();
    this.runner.addListener(eventCollector);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);

    this.runner.pause("me");
    waitForAndAssertFlowStatus(Status.PAUSED);
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").failJob();
    assertStatus("jobd:innerJobA", Status.FAILED);
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);
    // When flow is paused, no new jobs are started. So these two jobs that were already running
    // are allowed to finish, but their dependencies aren't started.
    // Now, ensure that jobd:innerJobA has completely finished as failed before resuming.
    // If we would resume before the job failure has been completely processed, FlowRunner would be
    // able to start some new jobs instead of cancelling everything.
    FlowRunnerTestUtil.waitEventFired(eventCollector, "jobd:innerJobA", Status.FAILED);
    waitForAndAssertFlowStatus(Status.PAUSED);

    this.runner.resume("me");
    assertStatus("jobb:innerJobB", Status.CANCELLED);
    assertStatus("jobb:innerJobC", Status.CANCELLED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);
    assertStatus("jobb", Status.KILLED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobd", Status.FAILED);

    InteractiveTestJob.getTestJob("jobc").succeedJob();
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    assertStatus("jobc", Status.SUCCEEDED);
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobf", Status.CANCELLED);
    assertStatus("jobe", Status.CANCELLED);

    waitForAndAssertFlowStatus(Status.FAILED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Test the condition when a Finish all possible is called during a pause. The Failure is not
   * acted upon until the flow is resumed.
   */
  @Test
  public void testPauseFailFinishAll() throws Exception {
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_ALL_POSSIBLE);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);

    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob();

    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);

    this.runner.pause("me");
    waitForAndAssertFlowStatus(Status.PAUSED);
    InteractiveTestJob.getTestJob("jobb:innerJobA").succeedJob();
    InteractiveTestJob.getTestJob("jobd:innerJobA").failJob();
    assertStatus("jobd:innerJobA", Status.FAILED);
    assertStatus("jobb:innerJobA", Status.SUCCEEDED);

    this.runner.resume("me");
    assertStatus("jobb:innerJobB", Status.RUNNING);
    assertStatus("jobb:innerJobC", Status.RUNNING);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobd", Status.FAILED);

    InteractiveTestJob.getTestJob("jobc").succeedJob();
    InteractiveTestJob.getTestJob("joba1").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobB").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerJobC").succeedJob();
    InteractiveTestJob.getTestJob("jobb:innerFlow").succeedJob();
    assertStatus("jobc", Status.SUCCEEDED);
    assertStatus("joba1", Status.SUCCEEDED);
    assertStatus("jobb:innerJobB", Status.SUCCEEDED);
    assertStatus("jobb:innerJobC", Status.SUCCEEDED);
    assertStatus("jobb:innerFlow", Status.SUCCEEDED);
    assertStatus("jobb", Status.SUCCEEDED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);

    waitForAndAssertFlowStatus(Status.FAILED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests the case when a job is killed by SLA causing a flow to fail. The flow should be in
   * "killed" status.
   */
  @Test
  public void testFlowKilledByJobLevelSLA() throws Exception {
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.CANCEL_ALL);

    FlowRunnerTestUtil.startThread(this.runner);
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    for (final JobRunner jobRunner : this.runner.getActiveJobRunners()) {
      if (jobRunner.getJobId().equals("joba")) {
        jobRunner.killBySLA();
        break;
      }
    }

    waitForAndAssertFlowStatus(Status.KILLED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests the case when a flow is paused and a failure causes a kill. The flow should die
   * immediately regardless of the 'paused' status.
   */
  @Test
  public void testPauseFailKill() throws Exception {
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.CANCEL_ALL);

    // 1. START FLOW
    FlowRunnerTestUtil.startThread(this.runner);
    // After it starts up, only joba should be running
    assertStatus("joba", Status.RUNNING);
    assertStatus("joba1", Status.RUNNING);

    // 2. JOB A COMPLETES SUCCESSFULLY
    InteractiveTestJob.getTestJob("joba").succeedJob();
    assertStatus("joba", Status.SUCCEEDED);
    assertStatus("joba1", Status.RUNNING);
    assertStatus("jobb", Status.RUNNING);
    assertStatus("jobc", Status.RUNNING);
    assertStatus("jobd", Status.RUNNING);
    assertStatus("jobd:innerJobA", Status.RUNNING);
    assertStatus("jobb:innerJobA", Status.RUNNING);

    this.runner.pause("me");
    waitForAndAssertFlowStatus(Status.PAUSED);
    InteractiveTestJob.getTestJob("jobd:innerJobA").failJob();
    assertStatus("jobd:innerJobA", Status.FAILED);
    assertStatus("jobd:innerFlow2", Status.CANCELLED);
    assertStatus("jobd", Status.FAILED);
    assertStatus("jobb:innerJobA", Status.KILLED);
    assertStatus("jobb:innerJobB", Status.CANCELLED);
    assertStatus("jobb:innerJobC", Status.CANCELLED);
    assertStatus("jobb:innerFlow", Status.CANCELLED);
    assertStatus("jobb", Status.KILLED);
    assertStatus("jobc", Status.KILLED);
    assertStatus("jobe", Status.CANCELLED);
    assertStatus("jobf", Status.CANCELLED);
    assertStatus("joba1", Status.KILLED);

    waitForAndAssertFlowStatus(Status.KILLED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

  /**
   * Tests the case when an execution is killed before it has started. The final execution
   * status should "KILLED".
   */
  @Test
  public void testKillBeforeStart() throws Exception {
    this.runner = this.testUtil.createFromFlowMap("jobf", FailureAction.FINISH_ALL_POSSIBLE);
    this.runner.addListener((event) -> {
      if (event.getType().equals(EventType.FLOW_STARTED)) {
        // kill interrupts the current thread which would cause an exception if called directly,
        // so do it from another thread.
        Thread aThread = new Thread( () -> this.runner.kill());
        aThread.start();
        try {
          // give the thread a chance to kill the execution
          aThread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    FlowRunnerTestUtil.startThread(this.runner).join();
    // children jobs shouldn't start
    assertStatus("joba", Status.READY);
    assertStatus("joba1", Status.READY);
    waitForAndAssertFlowStatus(Status.KILLED);
    assertThreadShutDown();
    Assert.assertFalse(this.runner.getLogger().getAllAppenders().hasMoreElements());
  }

}
