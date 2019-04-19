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

package azkaban.executor;

import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.flow.Flow;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.sla.SlaOption;
import azkaban.sla.SlaOption.SlaOptionBuilder;
import azkaban.sla.SlaType;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExecutableFlowTest {

  private Project project;

  private static void testEquals(final ExecutableNode a, final ExecutableNode b) {
    if (a instanceof ExecutableFlow) {
      if (b instanceof ExecutableFlow) {
        final ExecutableFlow exA = (ExecutableFlow) a;
        final ExecutableFlow exB = (ExecutableFlow) b;

        Assert.assertEquals(exA.getScheduleId(), exB.getScheduleId());
        Assert.assertEquals(exA.getProjectId(), exB.getProjectId());
        Assert.assertEquals(exA.getVersion(), exB.getVersion());
        Assert.assertEquals(exA.getSubmitTime(), exB.getSubmitTime());
        Assert.assertEquals(exA.getSubmitUser(), exB.getSubmitUser());
        Assert.assertEquals(exA.getExecutionPath(), exB.getExecutionPath());

        testEquals(exA.getExecutionOptions(), exB.getExecutionOptions());
      } else {
        Assert.fail("A is ExecutableFlow, but B is not");
      }
    }

    if (a instanceof ExecutableFlowBase) {
      if (b instanceof ExecutableFlowBase) {
        final ExecutableFlowBase exA = (ExecutableFlowBase) a;
        final ExecutableFlowBase exB = (ExecutableFlowBase) b;

        Assert.assertEquals(exA.getFlowId(), exB.getFlowId());
        Assert.assertEquals(exA.getExecutableNodes().size(), exB
            .getExecutableNodes().size());

        for (final ExecutableNode nodeA : exA.getExecutableNodes()) {
          final ExecutableNode nodeB = exB.getExecutableNode(nodeA.getId());
          Assert.assertNotNull(nodeB);
          Assert.assertEquals(a, nodeA.getParentFlow());
          Assert.assertEquals(b, nodeB.getParentFlow());

          testEquals(nodeA, nodeB);
        }
      } else {
        Assert.fail("A is ExecutableFlowBase, but B is not");
      }
    }

    Assert.assertEquals(a.getId(), b.getId());
    Assert.assertEquals(a.getStatus(), b.getStatus());
    Assert.assertEquals(a.getStartTime(), b.getStartTime());
    Assert.assertEquals(a.getEndTime(), b.getEndTime());
    Assert.assertEquals(a.getUpdateTime(), b.getUpdateTime());
    Assert.assertEquals(a.getAttempt(), b.getAttempt());

    Assert.assertEquals(a.getJobSource(), b.getJobSource());
    Assert.assertEquals(a.getPropsSource(), b.getPropsSource());
    Assert.assertEquals(a.getInNodes(), a.getInNodes());
    Assert.assertEquals(a.getOutNodes(), a.getOutNodes());
  }

  private static void testEquals(final ExecutionOptions optionsA,
      final ExecutionOptions optionsB) {
    Assert.assertEquals(optionsA.getConcurrentOption(),
        optionsB.getConcurrentOption());
    Assert.assertEquals(optionsA.getNotifyOnFirstFailure(),
        optionsB.getNotifyOnFirstFailure());
    Assert.assertEquals(optionsA.getNotifyOnLastFailure(),
        optionsB.getNotifyOnLastFailure());
    Assert.assertEquals(optionsA.getFailureAction(),
        optionsB.getFailureAction());
    Assert.assertEquals(optionsA.getPipelineExecutionId(),
        optionsB.getPipelineExecutionId());
    Assert.assertEquals(optionsA.getPipelineLevel(),
        optionsB.getPipelineLevel());
    Assert.assertEquals(optionsA.isFailureEmailsOverridden(),
        optionsB.isFailureEmailsOverridden());
    Assert.assertEquals(optionsA.isSuccessEmailsOverridden(),
        optionsB.isSuccessEmailsOverridden());

    testDisabledEquals(optionsA.getDisabledJobs(), optionsB.getDisabledJobs());
    testEquals(optionsA.getSuccessEmails(), optionsB.getSuccessEmails());
    testEquals(optionsA.getFailureEmails(), optionsB.getFailureEmails());
    testEquals(optionsA.getFlowParameters(), optionsB.getFlowParameters());
    testSlaEquals(optionsA.getSlaOptions(), optionsB.getSlaOptions());
  }

  private static void testEquals(final List<String> a, final List<String> b) {
    if (a == b) {
      return;
    }

    if (a == null || b == null) {
      Assert.fail();
    }

    Assert.assertEquals(a.size(), b.size());

    final Iterator<String> iterA = a.iterator();
    final Iterator<String> iterB = b.iterator();

    while (iterA.hasNext()) {
      final String aStr = iterA.next();
      final String bStr = iterB.next();
      Assert.assertEquals(aStr, bStr);
    }
  }

  private static void testDisabledEquals(final List<DisabledJob> a, final List<DisabledJob> b) {
    if (a == b) {
      return;
    }

    if (a == null || b == null) {
      Assert.fail();
    }

    Assert.assertEquals(a.size(), b.size());

    final Iterator<DisabledJob> iterA = a.iterator();
    final Iterator<DisabledJob> iterB = b.iterator();

    while (iterA.hasNext()) {
      final DisabledJob aElem = iterA.next();
      final DisabledJob bElem = iterB.next();

      if (aElem == null) {
        Assert.assertNull(bElem);
        continue;
      }
      Assert.assertEquals(aElem.getName(), bElem.getName());
      testDisabledEquals(aElem.getChildren(), bElem.getChildren());
    }
  }

  private static void testSlaEquals(List<SlaOption> a, List<SlaOption> b) {
    if (a == b) {
      return;
    }

    if (a == null || b == null) {
      Assert.fail();
    }

    Assert.assertEquals(a.size(), b.size());

    final Iterator<SlaOption> iterA = a.iterator();
    final Iterator<SlaOption> iterB = b.iterator();

    while (iterA.hasNext()) {
      final SlaOption aElem = iterA.next();
      final SlaOption bElem = iterB.next();

      Assert.assertEquals(aElem.getFlowName(), bElem.getFlowName());
      Assert.assertEquals(aElem.getJobName(), bElem.getJobName());
      Assert.assertEquals(aElem.getDuration(), bElem.getDuration());
      Assert.assertEquals(aElem.hasAlert(), bElem.hasAlert());
      Assert.assertEquals(aElem.hasKill(), bElem.hasKill());
      Assert.assertEquals(aElem.getType(), bElem.getType());
      Assert.assertEquals(aElem.getEmails(), bElem.getEmails());
    }
  }

  private static void testEquals(final Map<String, String> a, final Map<String, String> b) {
    if (a == b) {
      return;
    }

    if (a == null || b == null) {
      Assert.fail();
    }

    Assert.assertEquals(a.size(), b.size());

    for (final String key : a.keySet()) {
      Assert.assertEquals(a.get(key), b.get(key));
    }
  }

  @Before
  public void setUp() throws Exception {
    this.project = new Project(11, "myTestProject");

    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"));
    Assert.assertEquals(0, loader.getErrors().size());
    this.project.setFlows(loader.getFlowMap());
    this.project.setVersion(123);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testExecutorFlowCreation() throws Exception {
    final Flow flow = this.project.getFlow("jobe");
    Assert.assertNotNull(flow);

    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    Assert.assertNotNull(exFlow.getExecutableNode("joba"));
    Assert.assertNotNull(exFlow.getExecutableNode("jobb"));
    Assert.assertNotNull(exFlow.getExecutableNode("jobc"));
    Assert.assertNotNull(exFlow.getExecutableNode("jobd"));
    Assert.assertNotNull(exFlow.getExecutableNode("jobe"));

    Assert.assertFalse(exFlow.getExecutableNode("joba") instanceof ExecutableFlowBase);
    Assert.assertTrue(exFlow.getExecutableNode("jobb") instanceof ExecutableFlowBase);
    Assert.assertTrue(exFlow.getExecutableNode("jobc") instanceof ExecutableFlowBase);
    Assert.assertTrue(exFlow.getExecutableNode("jobd") instanceof ExecutableFlowBase);
    Assert.assertFalse(exFlow.getExecutableNode("jobe") instanceof ExecutableFlowBase);

    final ExecutableFlowBase jobbFlow =
        (ExecutableFlowBase) exFlow.getExecutableNode("jobb");
    final ExecutableFlowBase jobcFlow =
        (ExecutableFlowBase) exFlow.getExecutableNode("jobc");
    final ExecutableFlowBase jobdFlow =
        (ExecutableFlowBase) exFlow.getExecutableNode("jobd");

    Assert.assertEquals("innerFlow", jobbFlow.getFlowId());
    Assert.assertEquals("jobb", jobbFlow.getId());
    Assert.assertEquals(4, jobbFlow.getExecutableNodes().size());

    Assert.assertEquals("innerFlow", jobcFlow.getFlowId());
    Assert.assertEquals("jobc", jobcFlow.getId());
    Assert.assertEquals(4, jobcFlow.getExecutableNodes().size());

    Assert.assertEquals("innerFlow", jobdFlow.getFlowId());
    Assert.assertEquals("jobd", jobdFlow.getId());
    Assert.assertEquals(4, jobdFlow.getExecutableNodes().size());

    Assert.assertEquals(false, exFlow.isLocked());
    exFlow.setLocked(true);
    Assert.assertEquals(true, exFlow.isLocked());
    exFlow.setLocked(false);
    Assert.assertEquals(false, exFlow.isLocked());
  }

  @Test
  public void testExecutorFlowJson() throws Exception {
    final Flow flow = this.project.getFlow("jobe");
    Assert.assertNotNull(flow);

    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);

    final Object obj = exFlow.toObject();
    final String exFlowJSON = JSONUtils.toJSON(obj);
    final Map<String, Object> flowObjMap =
        (Map<String, Object>) JSONUtils.parseJSONFromString(exFlowJSON);

    final ExecutableFlow parsedExFlow =
        ExecutableFlow.createExecutableFlowFromObject(flowObjMap);
    testEquals(exFlow, parsedExFlow);
  }

  @Test
  public void testExecutorFlowJson2() throws Exception {
    final Flow flow = this.project.getFlow("jobe");
    Assert.assertNotNull(flow);

    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    exFlow.setExecutionId(101);
    // reset twice so that attempt = 2
    exFlow.resetForRetry();
    exFlow.resetForRetry();
    exFlow.setDelayedExecution(1000);
    exFlow.setLocked(true);

    final ExecutionOptions options = new ExecutionOptions();
    options.setConcurrentOption("blah");
    options.setDisabledJobs(Arrays.asList(new DisabledJob[]{new DisabledJob("bee"), null,
        new DisabledJob("boo"), new DisabledJob("ce",
        Arrays.asList(new DisabledJob[]{new DisabledJob("ca"),
            new DisabledJob("cu")}))}));
    options.setFailureAction(FailureAction.CANCEL_ALL);
    options
        .setFailureEmails(Arrays.asList(new String[]{"doo", null, "daa"}));
    options
        .setSuccessEmails(Arrays.asList(new String[]{"dee", null, "dae"}));
    options.setPipelineLevel(2);
    options.setPipelineExecutionId(3);
    options.setNotifyOnFirstFailure(true);
    options.setNotifyOnLastFailure(true);
    options.setSlaOptions(Arrays.asList(new SlaOptionBuilder(SlaType.FLOW_FINISH, "flowTest",
            Duration.ofMinutes(1130)).setAlert()
            .setEmails(Arrays.asList("fe@company.com", "fi@company.com")).createSlaOption(),
        new SlaOptionBuilder(SlaType.JOB_SUCCEED, "flowTest", Duration.ofMinutes(130))
            .setJobName("fo").setKill()
            .setEmails( Arrays.asList("fe@company.com", "fi@company.com")).createSlaOption()));

    final HashMap<String, String> flowProps = new HashMap<>();
    flowProps.put("la", "fa");
    options.addAllFlowParameters(flowProps);
    exFlow.setExecutionOptions(options);

    final Object obj = exFlow.toObject();
    final String exFlowJSON = JSONUtils.toJSON(obj);
    final Map<String, Object> flowObjMap =
        (Map<String, Object>) JSONUtils.parseJSONFromString(exFlowJSON);
    final ExecutableFlow parsedExFlow =
        ExecutableFlow.createExecutableFlowFromObject(flowObjMap);
    testEquals(exFlow, parsedExFlow);

    // test backward compatibility: reading in the original JSON format should
    // generate the same object
    Map<String, Object>origObjMap =
        (Map<String, Object>) JSONUtils.parseJSONFromFile(new File
        ("src/test/resources/json/embedded_flow.json"));
    final ExecutableFlow origExFlow =
        ExecutableFlow.createExecutableFlowFromObject(origObjMap);
    origExFlow.setUpdateTime(exFlow.getUpdateTime()); // update time changes
    testEquals(exFlow, origExFlow);
  }

  @Test
  public void testExecutorFlowUpdates() throws Exception {
    final Flow flow = this.project.getFlow("jobe");
    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    exFlow.setExecutionId(101);

    // Create copy of flow
    final Object obj = exFlow.toObject();
    final String exFlowJSON = JSONUtils.toJSON(obj);
    final Map<String, Object> flowObjMap =
        (Map<String, Object>) JSONUtils.parseJSONFromString(exFlowJSON);
    final ExecutableFlow copyFlow =
        ExecutableFlow.createExecutableFlowFromObject(flowObjMap);

    testEquals(exFlow, copyFlow);

    final ExecutableNode joba = exFlow.getExecutableNode("joba");
    final ExecutableFlowBase jobb =
        (ExecutableFlowBase) (exFlow.getExecutableNode("jobb"));
    final ExecutableFlowBase jobc =
        (ExecutableFlowBase) (exFlow.getExecutableNode("jobc"));
    final ExecutableFlowBase jobd =
        (ExecutableFlowBase) (exFlow.getExecutableNode("jobd"));
    final ExecutableNode jobe = exFlow.getExecutableNode("jobe");
    assertNotNull(joba, jobb, jobc, jobd, jobe);

    final ExecutableNode jobbInnerFlowA = jobb.getExecutableNode("innerJobA");
    final ExecutableNode jobbInnerFlowB = jobb.getExecutableNode("innerJobB");
    final ExecutableNode jobbInnerFlowC = jobb.getExecutableNode("innerJobC");
    final ExecutableNode jobbInnerFlow = jobb.getExecutableNode("innerFlow");
    assertNotNull(jobbInnerFlowA, jobbInnerFlowB, jobbInnerFlowC, jobbInnerFlow);

    final ExecutableNode jobcInnerFlowA = jobc.getExecutableNode("innerJobA");
    final ExecutableNode jobcInnerFlowB = jobc.getExecutableNode("innerJobB");
    final ExecutableNode jobcInnerFlowC = jobc.getExecutableNode("innerJobC");
    final ExecutableNode jobcInnerFlow = jobc.getExecutableNode("innerFlow");
    assertNotNull(jobcInnerFlowA, jobcInnerFlowB, jobcInnerFlowC, jobcInnerFlow);

    final ExecutableNode jobdInnerFlowA = jobd.getExecutableNode("innerJobA");
    final ExecutableNode jobdInnerFlowB = jobd.getExecutableNode("innerJobB");
    final ExecutableNode jobdInnerFlowC = jobd.getExecutableNode("innerJobC");
    final ExecutableNode jobdInnerFlow = jobd.getExecutableNode("innerFlow");
    assertNotNull(jobdInnerFlowA, jobdInnerFlowB, jobdInnerFlowC, jobdInnerFlow);

    exFlow.setEndTime(1000);
    exFlow.setStartTime(500);
    exFlow.setStatus(Status.RUNNING);
    exFlow.setUpdateTime(133);

    // Change one job and see if it updates
    final long time = System.currentTimeMillis();
    jobe.setEndTime(time);
    jobe.setUpdateTime(time);
    jobe.setStatus(Status.DISABLED);
    jobe.setStartTime(time - 1);
    // Should be one node that was changed
    Map<String, Object> updateObject = exFlow.toUpdateObject(0);
    Assert.assertEquals(1, ((List) (updateObject.get("nodes"))).size());
    // Reapplying should give equal results.
    copyFlow.applyUpdateObject(updateObject);
    testEquals(exFlow, copyFlow);

    // This update shouldn't provide any results
    updateObject = exFlow.toUpdateObject(System.currentTimeMillis());
    Assert.assertNull(updateObject.get("nodes"));

    // Change inner flow
    final long currentTime = time + 1;
    jobbInnerFlowA.setEndTime(currentTime);
    jobbInnerFlowA.setUpdateTime(currentTime);
    jobbInnerFlowA.setStatus(Status.DISABLED);
    jobbInnerFlowA.setStartTime(currentTime - 100);
    // We should get 2 updates if we do a toUpdateObject using 0 as the start
    // time
    updateObject = exFlow.toUpdateObject(0);
    Assert.assertEquals(2, ((List) (updateObject.get("nodes"))).size());

    // This should provide 1 update. That we can apply
    updateObject = exFlow.toUpdateObject(jobe.getUpdateTime());
    Assert.assertNotNull(updateObject.get("nodes"));
    Assert.assertEquals(1, ((List) (updateObject.get("nodes"))).size());
    copyFlow.applyUpdateObject(updateObject);
    testEquals(exFlow, copyFlow);

    // This shouldn't give any results anymore
    updateObject = exFlow.toUpdateObject(jobbInnerFlowA.getUpdateTime());
    Assert.assertNull(updateObject.get("nodes"));
  }

  private void assertNotNull(final ExecutableNode... nodes) {
    for (final ExecutableNode node : nodes) {
      Assert.assertNotNull(node);
    }
  }
}
