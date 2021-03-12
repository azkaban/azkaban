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

import azkaban.DispatchMethod;
import azkaban.utils.Pair;
import azkaban.utils.TestUtils;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for ExecutableFlowPriorityComparator
 */

public class ExecutableFlowPriorityComparatorTest {

  /* Helper method to create an ExecutableFlow from serialized description */
  private ExecutableFlow createExecutableFlow(final String flowName, final int priority,
      final long updateTime, final int executionId) throws IOException {
    final ExecutableFlow execFlow =
        TestUtils.createTestExecutableFlow("exectest1", flowName, DispatchMethod.POLL);

    execFlow.setUpdateTime(updateTime);
    execFlow.setExecutionId(executionId);
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
    final ExecutableFlow flow1 = createExecutableFlow("exec1", 5, 3, 1);
    final ExecutableFlow flow2 = createExecutableFlow("exec2", 6, 3, 2);
    final ExecutableFlow flow3 = createExecutableFlow("exec3", 2, 3, 3);
    final ExecutionReference dummyRef = new ExecutionReference(0, DispatchMethod.PUSH);

    final BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queue =
        new PriorityBlockingQueue<>(10,
            new ExecutableFlowPriorityComparator());
    queue.put(new Pair<>(dummyRef, flow1));
    queue.put(new Pair<>(dummyRef, flow2));
    queue.put(new Pair<>(dummyRef, flow3));

    Assert.assertEquals(flow2, queue.take().getSecond());
    Assert.assertEquals(flow1, queue.take().getSecond());
    Assert.assertEquals(flow3, queue.take().getSecond());
  }

  /* priority queue order when some priorities are implicitly specified */
  @Test
  public void testMixedSpecifiedPriorities() throws IOException,
      InterruptedException {
    final ExecutableFlow flow1 = createExecutableFlow("exec1", 3, 3, 1);
    final ExecutableFlow flow2 = createExecutableFlow("exec2", 2, 3, 2);
    final ExecutableFlow flow3 = createExecutableFlow("exec3", -2, 3, 3);
    final ExecutionReference dummyRef = new ExecutionReference(0, DispatchMethod.PUSH);

    final BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queue =
        new PriorityBlockingQueue<>(10,
            new ExecutableFlowPriorityComparator());
    queue.put(new Pair<>(dummyRef, flow1));
    queue.put(new Pair<>(dummyRef, flow2));
    queue.put(new Pair<>(dummyRef, flow3));

    Assert.assertEquals(flow3, queue.take().getSecond());
    Assert.assertEquals(flow1, queue.take().getSecond());
    Assert.assertEquals(flow2, queue.take().getSecond());
  }

  /*
   * priority queue order when some priorities are equal, updatetime is used in
   * this case
   */
  @Test
  public void testEqualPriorities() throws IOException, InterruptedException {
    final ExecutableFlow flow1 = createExecutableFlow("exec1", 3, 1, 1);
    final ExecutableFlow flow2 = createExecutableFlow("exec2", 2, 2, 2);
    final ExecutableFlow flow3 = createExecutableFlow("exec3", -2, 3, 3);
    final ExecutableFlow flow4 = createExecutableFlow("exec3", 3, 4, 4);
    final ExecutionReference dummyRef = new ExecutionReference(0, DispatchMethod.PUSH);

    final BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queue =
        new PriorityBlockingQueue<>(10,
            new ExecutableFlowPriorityComparator());

    queue.put(new Pair<>(dummyRef, flow4));
    queue.put(new Pair<>(dummyRef, flow1));
    queue.put(new Pair<>(dummyRef, flow2));
    queue.put(new Pair<>(dummyRef, flow3));

    Assert.assertEquals(flow3, queue.take().getSecond());
    Assert.assertEquals(flow1, queue.take().getSecond());
    Assert.assertEquals(flow4, queue.take().getSecond());
    Assert.assertEquals(flow2, queue.take().getSecond());
  }

  /*
   * priority queue order when some priorities and updatetime are equal,
   * execution Id is used in this case
   */
  @Test
  public void testEqualUpdateTimeAndPriority() throws IOException,
      InterruptedException {
    final ExecutableFlow flow1 = createExecutableFlow("exec1", 3, 1, 1);
    final ExecutableFlow flow2 = createExecutableFlow("exec2", 2, 2, 2);
    final ExecutableFlow flow3 = createExecutableFlow("exec3", -2, 2, 3);
    final ExecutableFlow flow4 = createExecutableFlow("exec3", 3, 4, 4);
    final ExecutionReference dummyRef = new ExecutionReference(0, DispatchMethod.PUSH);

    final BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queue =
        new PriorityBlockingQueue<>(10,
            new ExecutableFlowPriorityComparator());

    queue.put(new Pair<>(dummyRef, flow4));
    queue.put(new Pair<>(dummyRef, flow1));
    queue.put(new Pair<>(dummyRef, flow2));
    queue.put(new Pair<>(dummyRef, flow3));

    Assert.assertEquals(flow3, queue.take().getSecond());
    Assert.assertEquals(flow1, queue.take().getSecond());
    Assert.assertEquals(flow4, queue.take().getSecond());
    Assert.assertEquals(flow2, queue.take().getSecond());
  }
}
