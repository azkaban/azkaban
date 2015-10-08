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

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.flow.Flow;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.test.executions.TestExecutions;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class ExecutableFlowTest {
  private Project project;

  @Before
  public void setUp() throws Exception {
    project = new Project(11, "myTestProject");

    Logger logger = Logger.getLogger(this.getClass());
    DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);

    loader.loadProjectFlow(project, TestExecutions.getFlowDir("embedded"));
    Assert.assertEquals(0, loader.getErrors().size());

    project.setFlows(loader.getFlowMap());
    project.setVersion(123);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testExecutorFlowCreation() throws Exception {
    Flow flow = project.getFlow("jobe");
    Assert.assertNotNull(flow);

    ExecutableFlow exFlow = new ExecutableFlow(project, flow);
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

    ExecutableFlowBase jobbFlow =
        (ExecutableFlowBase) exFlow.getExecutableNode("jobb");
    ExecutableFlowBase jobcFlow =
        (ExecutableFlowBase) exFlow.getExecutableNode("jobc");
    ExecutableFlowBase jobdFlow =
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
  }

  @Test
  public void testExecutorFlowJson() throws Exception {
    Flow flow = project.getFlow("jobe");
    Assert.assertNotNull(flow);

    ExecutableFlow exFlow = new ExecutableFlow(project, flow);

    Object obj = exFlow.toObject();
    String exFlowJSON = JSONUtils.toJSON(obj);
    @SuppressWarnings("unchecked")
    Map<String, Object> flowObjMap =
        (Map<String, Object>) JSONUtils.parseJSONFromString(exFlowJSON);

    ExecutableFlow parsedExFlow =
        ExecutableFlow.createExecutableFlowFromObject(flowObjMap);
    testEquals(exFlow, parsedExFlow);
  }

  @Test
  public void testExecutorFlowJson2() throws Exception {
    Flow flow = project.getFlow("jobe");
    Assert.assertNotNull(flow);

    ExecutableFlow exFlow = new ExecutableFlow(project, flow);
    exFlow.setExecutionId(101);
    exFlow.setAttempt(2);
    exFlow.setDelayedExecution(1000);

    ExecutionOptions options = new ExecutionOptions();
    options.setConcurrentOption("blah");
    options.setDisabledJobs(Arrays.asList(new Object[] { "bee", null, "boo" }));
    options.setFailureAction(FailureAction.CANCEL_ALL);
    options
        .setFailureEmails(Arrays.asList(new String[] { "doo", null, "daa" }));
    options
        .setSuccessEmails(Arrays.asList(new String[] { "dee", null, "dae" }));
    options.setPipelineLevel(2);
    options.setPipelineExecutionId(3);
    options.setNotifyOnFirstFailure(true);
    options.setNotifyOnLastFailure(true);

    HashMap<String, String> flowProps = new HashMap<String, String>();
    flowProps.put("la", "fa");
    options.addAllFlowParameters(flowProps);
    exFlow.setExecutionOptions(options);

    Object obj = exFlow.toObject();
    String exFlowJSON = JSONUtils.toJSON(obj);
    @SuppressWarnings("unchecked")
    Map<String, Object> flowObjMap =
        (Map<String, Object>) JSONUtils.parseJSONFromString(exFlowJSON);

    ExecutableFlow parsedExFlow =
        ExecutableFlow.createExecutableFlowFromObject(flowObjMap);
    testEquals(exFlow, parsedExFlow);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testExecutorFlowUpdates() throws Exception {
    Flow flow = project.getFlow("jobe");
    ExecutableFlow exFlow = new ExecutableFlow(project, flow);
    exFlow.setExecutionId(101);

    // Create copy of flow
    Object obj = exFlow.toObject();
    String exFlowJSON = JSONUtils.toJSON(obj);
    @SuppressWarnings("unchecked")
    Map<String, Object> flowObjMap =
        (Map<String, Object>) JSONUtils.parseJSONFromString(exFlowJSON);
    ExecutableFlow copyFlow =
        ExecutableFlow.createExecutableFlowFromObject(flowObjMap);

    testEquals(exFlow, copyFlow);

    ExecutableNode joba = exFlow.getExecutableNode("joba");
    ExecutableFlowBase jobb =
        (ExecutableFlowBase) (exFlow.getExecutableNode("jobb"));
    ExecutableFlowBase jobc =
        (ExecutableFlowBase) (exFlow.getExecutableNode("jobc"));
    ExecutableFlowBase jobd =
        (ExecutableFlowBase) (exFlow.getExecutableNode("jobd"));
    ExecutableNode jobe = exFlow.getExecutableNode("jobe");
    assertNotNull(joba, jobb, jobc, jobd, jobe);

    ExecutableNode jobbInnerFlowA = jobb.getExecutableNode("innerJobA");
    ExecutableNode jobbInnerFlowB = jobb.getExecutableNode("innerJobB");
    ExecutableNode jobbInnerFlowC = jobb.getExecutableNode("innerJobC");
    ExecutableNode jobbInnerFlow = jobb.getExecutableNode("innerFlow");
    assertNotNull(jobbInnerFlowA, jobbInnerFlowB, jobbInnerFlowC, jobbInnerFlow);

    ExecutableNode jobcInnerFlowA = jobc.getExecutableNode("innerJobA");
    ExecutableNode jobcInnerFlowB = jobc.getExecutableNode("innerJobB");
    ExecutableNode jobcInnerFlowC = jobc.getExecutableNode("innerJobC");
    ExecutableNode jobcInnerFlow = jobc.getExecutableNode("innerFlow");
    assertNotNull(jobcInnerFlowA, jobcInnerFlowB, jobcInnerFlowC, jobcInnerFlow);

    ExecutableNode jobdInnerFlowA = jobd.getExecutableNode("innerJobA");
    ExecutableNode jobdInnerFlowB = jobd.getExecutableNode("innerJobB");
    ExecutableNode jobdInnerFlowC = jobd.getExecutableNode("innerJobC");
    ExecutableNode jobdInnerFlow = jobd.getExecutableNode("innerFlow");
    assertNotNull(jobdInnerFlowA, jobdInnerFlowB, jobdInnerFlowC, jobdInnerFlow);

    exFlow.setEndTime(1000);
    exFlow.setStartTime(500);
    exFlow.setStatus(Status.RUNNING);
    exFlow.setUpdateTime(133);

    // Change one job and see if it updates
    long time = System.currentTimeMillis();
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
    long currentTime = time + 1;
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

  private void assertNotNull(ExecutableNode... nodes) {
    for (ExecutableNode node : nodes) {
      Assert.assertNotNull(node);
    }
  }

  private static void testEquals(ExecutableNode a, ExecutableNode b) {
    if (a instanceof ExecutableFlow) {
      if (b instanceof ExecutableFlow) {
        ExecutableFlow exA = (ExecutableFlow) a;
        ExecutableFlow exB = (ExecutableFlow) b;

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
        ExecutableFlowBase exA = (ExecutableFlowBase) a;
        ExecutableFlowBase exB = (ExecutableFlowBase) b;

        Assert.assertEquals(exA.getFlowId(), exB.getFlowId());
        Assert.assertEquals(exA.getExecutableNodes().size(), exB
            .getExecutableNodes().size());

        for (ExecutableNode nodeA : exA.getExecutableNodes()) {
          ExecutableNode nodeB = exB.getExecutableNode(nodeA.getId());
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

  private static void testEquals(ExecutionOptions optionsA,
      ExecutionOptions optionsB) {
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
  }

  private static void testEquals(Set<String> a, Set<String> b) {
    if (a == b) {
      return;
    }

    if (a == null || b == null) {
      Assert.fail();
    }

    Assert.assertEquals(a.size(), b.size());

    Iterator<String> iterA = a.iterator();

    while (iterA.hasNext()) {
      String aStr = iterA.next();
      Assert.assertTrue(b.contains(aStr));
    }
  }

  private static void testEquals(List<String> a, List<String> b) {
    if (a == b) {
      return;
    }

    if (a == null || b == null) {
      Assert.fail();
    }

    Assert.assertEquals(a.size(), b.size());

    Iterator<String> iterA = a.iterator();
    Iterator<String> iterB = b.iterator();

    while (iterA.hasNext()) {
      String aStr = iterA.next();
      String bStr = iterB.next();
      Assert.assertEquals(aStr, bStr);
    }
  }

  @SuppressWarnings("unchecked")
  private static void testDisabledEquals(List<Object> a, List<Object> b) {
    if (a == b) {
      return;
    }

    if (a == null || b == null) {
      Assert.fail();
    }

    Assert.assertEquals(a.size(), b.size());

    Iterator<Object> iterA = a.iterator();
    Iterator<Object> iterB = b.iterator();

    while (iterA.hasNext()) {
      Object aStr = iterA.next();
      Object bStr = iterB.next();

      if (aStr instanceof Map && bStr instanceof Map) {
        Map<String, Object> aMap = (Map<String, Object>) aStr;
        Map<String, Object> bMap = (Map<String, Object>) bStr;

        Assert.assertEquals((String) aMap.get("id"), (String) bMap.get("id"));
        testDisabledEquals((List<Object>) aMap.get("children"),
            (List<Object>) bMap.get("children"));
      } else {
        Assert.assertEquals(aStr, bStr);
      }
    }
  }

  private static void testEquals(Map<String, String> a, Map<String, String> b) {
    if (a == b) {
      return;
    }

    if (a == null || b == null) {
      Assert.fail();
    }

    Assert.assertEquals(a.size(), b.size());

    for (String key : a.keySet()) {
      Assert.assertEquals(a.get(key), b.get(key));
    }
  }
}
