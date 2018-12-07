/*
* Copyright 2018 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.utils.Pair;
import azkaban.utils.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExecutionControllerTest {

  private Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new HashMap<>();
  private Map<Integer, Pair<ExecutionReference, ExecutableFlow>> unfinishedFlows = new
      HashMap<>();
  private List<Executor> activeExecutors = new ArrayList<>();
  private List<Executor> allExecutors = new ArrayList<>();
  private ExecutionController controller;
  private ExecutorLoader loader;
  private ExecutorApiGateway apiGateway;

  @Before
  public void setup() throws Exception {
    this.loader = mock(ExecutorLoader.class);
    this.apiGateway = mock(ExecutorApiGateway.class);
    this.controller = new ExecutionController(this.loader, this.apiGateway);

    final Executor executor1 = new Executor(1, "localhost", 12345, true);
    final Executor executor2 = new Executor(2, "localhost", 12346, true);
    final Executor executor3 = new Executor(3, "localhost", 12347, false);
    this.activeExecutors = ImmutableList.of(executor1, executor2);
    this.allExecutors = ImmutableList.of(executor1, executor2, executor3);
    when(this.loader.fetchActiveExecutors()).thenReturn(this.activeExecutors);

    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    final ExecutableFlow flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    final ExecutableFlow flow3 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    flow1.setExecutionId(1);
    flow2.setExecutionId(2);
    flow3.setExecutionId(3);
    final ExecutionReference ref1 =
        new ExecutionReference(flow1.getExecutionId(), null);
    final ExecutionReference ref2 =
        new ExecutionReference(flow2.getExecutionId(), executor2);
    final ExecutionReference ref3 =
        new ExecutionReference(flow3.getExecutionId(), executor3);

    this.activeFlows = ImmutableMap
        .of(flow2.getExecutionId(), new Pair<>(ref2, flow2), flow3.getExecutionId(),
            new Pair<>(ref3, flow3));
    when(this.loader.fetchActiveFlows()).thenReturn(this.activeFlows);

    this.unfinishedFlows = ImmutableMap.of(flow1.getExecutionId(), new Pair<>(ref1, flow1),
        flow2.getExecutionId(), new Pair<>(ref2, flow2), flow3.getExecutionId(), new Pair<>(ref3,
            flow3));
    when(this.loader.fetchUnfinishedFlows()).thenReturn(this.unfinishedFlows);
  }

  @After
  public void tearDown() {
    if (this.controller != null) {
      this.controller.shutdown();
    }
  }

  @Test
  public void testFetchAllActiveFlows() throws Exception {
    final List<ExecutableFlow> flows = this.controller.getRunningFlows();
    this.unfinishedFlows.values()
        .forEach(pair -> assertThat(flows.contains(pair.getSecond())).isTrue());
  }

  @Test
  public void testFetchActiveFlowByProject() throws Exception {
    final ExecutableFlow flow2 = this.unfinishedFlows.get(2).getSecond();
    final ExecutableFlow flow3 = this.unfinishedFlows.get(3).getSecond();
    final List<Integer> executions = this.controller.getRunningFlows(flow2.getProjectId(), flow2
        .getFlowId());
    assertThat(executions.contains(flow2.getExecutionId())).isTrue();
    assertThat(executions.contains(flow3.getExecutionId())).isTrue();
    assertThat(this.controller.isFlowRunning(flow2.getProjectId(), flow2.getFlowId())).isTrue();
    assertThat(this.controller.isFlowRunning(flow3.getProjectId(), flow3.getFlowId())).isTrue();
  }

  @Test
  public void testFetchActiveFlowWithExecutor() throws Exception {
    final List<Pair<ExecutableFlow, Optional<Executor>>> activeFlowsWithExecutor =
        this.controller.getActiveFlowsWithExecutor();
    this.unfinishedFlows.values().forEach(pair -> assertThat(activeFlowsWithExecutor
        .contains(new Pair<>(pair.getSecond(), pair.getFirst().getExecutor()))).isTrue());
  }

  @Test
  public void testFetchAllActiveExecutorServerHosts() throws Exception {
    final Set<String> activeExecutorServerHosts = this.controller.getAllActiveExecutorServerHosts();
    assertThat(activeExecutorServerHosts.size()).isEqualTo(3);
    this.allExecutors.forEach(executor -> assertThat(
        activeExecutorServerHosts.contains(executor.getHost() + ":" + executor.getPort()))
        .isTrue());
  }
}
