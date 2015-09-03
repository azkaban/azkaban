/*
 * Copyright 2015 LinkedIn Corp.
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
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Assert;
import org.junit.Test;

import azkaban.alert.Alerter;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.user.User;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;

/**
 * Test class for ExecutableFlowPriorityComparator
 * */

public class ExecutableFlowPriorityComparatorTest {
  /* Directory with serialized description of test flows */
  private static final String UNIT_BASE_DIR =
    "../azkaban-test/src/test/resources/executions/exectest1/";

  private File getFlowDir(String flow) {
    return new File(UNIT_BASE_DIR + flow + ".flow");
  }

  /* Helper method to create an ExecutableFlow from serialized description */
  private ExecutableFlow createExecutableFlow(String flowName, int execId,
    int priority) throws IOException {
    File jsonFlowFile = getFlowDir(flowName);
    @SuppressWarnings("unchecked")
    HashMap<String, Object> flowObj =
      (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);

    Flow flow = Flow.flowFromObject(flowObj);
    Project project = new Project(1, "flow");
    HashMap<String, Flow> flowMap = new HashMap<String, Flow>();
    flowMap.put(flow.getId(), flow);
    project.setFlows(flowMap);
    ExecutableFlow execFlow = new ExecutableFlow(project, flow);

    execFlow.setExecutionId(execId);
    if (priority > 0) {
      execFlow.getExecutionOptions().getFlowParameters()
        .put(ExecutionOptions.FLOW_PRIORITY, String.valueOf(priority));
    }
    return execFlow;
  }

  /* priority queue order when all priorities are explicitly specified */
  @Test
  public void testExplicitlySpecifiedPriorities() throws IOException,
    InterruptedException {
    ExecutableFlow flow1 = createExecutableFlow("exec1", 1, 5);
    ExecutableFlow flow2 = createExecutableFlow("exec2", 2, 6);
    ExecutableFlow flow3 = createExecutableFlow("exec3", 3, 2);
    ExecutionReference dummyRef = new ExecutionReference(0);

    BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queue =
      new PriorityBlockingQueue<Pair<ExecutionReference, ExecutableFlow>>(10,
        new ExecutableFlowPriorityComparator());
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow1));
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow2));
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow3));

    Assert.assertEquals(flow2, queue.take().getSecond());
    Assert.assertEquals(flow1, queue.take().getSecond());
    Assert.assertEquals(flow3, queue.take().getSecond());
  }

  /* priority queue order when some priorities are implicitly specified */
  @Test
  public void testMixedSpecifiedPriorities() throws IOException,
    InterruptedException {
    ExecutableFlow flow1 = createExecutableFlow("exec1", 1, 3);
    ExecutableFlow flow2 = createExecutableFlow("exec2", 2, 2);
    ExecutableFlow flow3 = createExecutableFlow("exec3", 3, -2);
    ExecutionReference dummyRef = new ExecutionReference(0);

    BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queue =
      new PriorityBlockingQueue<Pair<ExecutionReference, ExecutableFlow>>(10,
        new ExecutableFlowPriorityComparator());
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow1));
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow2));
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow3));

    Assert.assertEquals(flow3, queue.take().getSecond());
    Assert.assertEquals(flow1, queue.take().getSecond());
    Assert.assertEquals(flow2, queue.take().getSecond());
  }

  /*
   * priority queue order when some priorities are equal, execution Id is used
   * in this case
   */
  @Test
  public void testEqualPriorities() throws IOException, InterruptedException {
    ExecutableFlow flow1 = createExecutableFlow("exec1", 1, 3);
    ExecutableFlow flow2 = createExecutableFlow("exec2", 2, 2);
    ExecutableFlow flow3 = createExecutableFlow("exec3", 3, -2);
    ExecutableFlow flow4 = createExecutableFlow("exec3", 4, 3);
    ExecutionReference dummyRef = new ExecutionReference(0);

    BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queue =
      new PriorityBlockingQueue<Pair<ExecutionReference, ExecutableFlow>>(10,
        new ExecutableFlowPriorityComparator());

    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow4));
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow1));
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow2));
    queue.put(new Pair<ExecutionReference, ExecutableFlow>(dummyRef, flow3));

    Assert.assertEquals(flow3, queue.take().getSecond());
    Assert.assertEquals(flow1, queue.take().getSecond());
    Assert.assertEquals(flow4, queue.take().getSecond());
    Assert.assertEquals(flow2, queue.take().getSecond());
  }
}
