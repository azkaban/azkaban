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

package azkaban.trigger;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.intThat;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

public class ExecuteFlowActionTest {

  @Ignore
  @Test
  public void jsonConversionTest() throws Exception {
    final ActionTypeLoader loader = new ActionTypeLoader();
    loader.init(new Props());

    final ExecutionOptions options = new ExecutionOptions();
    final List<Object> disabledJobs = new ArrayList<>();
    options.setDisabledJobs(disabledJobs);

    final ExecuteFlowAction executeFlowAction =
        new ExecuteFlowAction("ExecuteFlowAction", 1, "testproject",
            "testflow", "azkaban", options, null);

    final Object obj = executeFlowAction.toJson();

    final ExecuteFlowAction action =
        (ExecuteFlowAction) loader.createActionFromJson(ExecuteFlowAction.type,
            obj);
    assertTrue(executeFlowAction.getProjectId() == action.getProjectId());
    assertTrue(executeFlowAction.getFlowName().equals(action.getFlowName()));
    assertTrue(executeFlowAction.getSubmitUser().equals(action.getSubmitUser()));
  }

  @Test
  public void doActionWithNoPreviousExecutionDefaultOptionTest() throws Exception {
    final Project project = new Project(1, "project");
    project.setFlows(ImmutableMap.of("flow", new Flow("flow")));

    final List<ExecutableFlow> executableFlows = new ArrayList<>();

    final ExecutorManagerAdapter executorManager = getMockedExecutorManagerAdapter(project,
        executableFlows);

    final ExecutionOptions executionOptions = new ExecutionOptions();
    final ExecuteFlowAction executeFlowAction = new ExecuteFlowAction("action", 1, "project",
        "flow", "user", executionOptions, null);
    executeFlowAction.doAction();

    //noinspection unchecked
    verify(executorManager, times(0)).getExecutableFlows(anyInt(), anyString(), anyInt(), anyInt(),
        (List<ExecutableFlow>) any());
    verify(executorManager).submitExecutableFlow(notNull(), eq("user"));
  }

  @Test
  public void doActionWithPreviousFailedExecutionDefaultOptionTest() throws Exception {
    final Project project = new Project(1, "project");
    final Flow flow = new Flow("flow");
    project.setFlows(ImmutableMap.of("flow", flow));

    final List<ExecutableFlow> executableFlows = new ArrayList<>();
    final ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    executableFlow.setStatus(Status.FAILED);
    executableFlows.add(executableFlow);

    final ExecutorManagerAdapter executorManager = getMockedExecutorManagerAdapter(project,
        executableFlows);

    final ExecutionOptions executionOptions = new ExecutionOptions();
    final ExecuteFlowAction executeFlowAction = new ExecuteFlowAction("action", 1, "project",
        "flow", "user", executionOptions, null);
    executeFlowAction.doAction();

    //noinspection unchecked
    verify(executorManager, times(0)).getExecutableFlows(anyInt(), anyString(), anyInt(), anyInt(),
        (List<ExecutableFlow>) any());
    verify(executorManager).submitExecutableFlow(notNull(), eq("user"));
  }

  @Test
  public void doActionWithPreviousSuccessfulExecutionDefaultOptionTest() throws Exception {
    final Project project = new Project(1, "project");
    final Flow flow = new Flow("flow");
    project.setFlows(ImmutableMap.of("flow", flow));

    final List<ExecutableFlow> executableFlows = new ArrayList<>();
    final ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    executableFlow.setStatus(Status.SUCCEEDED);
    executableFlows.add(executableFlow);

    final ExecutorManagerAdapter executorManager = getMockedExecutorManagerAdapter(project,
        executableFlows);

    final ExecutionOptions executionOptions = new ExecutionOptions();
    final ExecuteFlowAction executeFlowAction = new ExecuteFlowAction("action", 1, "project",
        "flow", "user", executionOptions, null);
    executeFlowAction.doAction();

    //noinspection unchecked
    verify(executorManager, times(0)).getExecutableFlows(anyInt(), anyString(), anyInt(), anyInt(),
        (List<ExecutableFlow>) any());
    verify(executorManager).submitExecutableFlow(notNull(), eq("user"));
  }

  @Test
  public void doActionWithNoPreviousExecutionSkipOptionTest() throws Exception {
    final Project project = new Project(1, "project");
    project.setFlows(ImmutableMap.of("flow", new Flow("flow")));

    final List<ExecutableFlow> executableFlows = new ArrayList<>();

    final ExecutorManagerAdapter executorManager = getMockedExecutorManagerAdapter(project,
        executableFlows);

    final ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setSkipIfPreviousExecutionFailed(true);
    final ExecuteFlowAction executeFlowAction = new ExecuteFlowAction("action", 1, "project",
        "flow", "user", executionOptions, null);
    executeFlowAction.doAction();

    //noinspection unchecked
    verify(executorManager).getExecutableFlows(eq(1), eq("flow"), eq(0), intThat(i -> i >= 1),
        (List<ExecutableFlow>) notNull());
    verify(executorManager).submitExecutableFlow(notNull(), eq("user"));
  }

  @Test
  public void doActionWithPreviousFailedExecutionSkipOptionTest() throws Exception {
    final Project project = new Project(1, "project");
    final Flow flow = new Flow("flow");
    project.setFlows(ImmutableMap.of("flow", flow));

    final List<ExecutableFlow> executableFlows = new ArrayList<>();
    final ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    executableFlow.setStatus(Status.FAILED);
    executableFlows.add(executableFlow);

    final ExecutorManagerAdapter executorManager = getMockedExecutorManagerAdapter(project,
        executableFlows);

    final ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setSkipIfPreviousExecutionFailed(true);
    final ExecuteFlowAction executeFlowAction = new ExecuteFlowAction("action", 1, "project",
        "flow", "user", executionOptions, null);
    executeFlowAction.doAction();

    verify(executorManager, times(0)).submitExecutableFlow(any(), any());
  }

  @Test
  public void doActionWithPreviousSuccessfulExecutionSkipOptionTest() throws Exception {
    final Project project = new Project(1, "project");
    final Flow flow = new Flow("flow");
    project.setFlows(ImmutableMap.of("flow", flow));

    final List<ExecutableFlow> executableFlows = new ArrayList<>();
    final ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    executableFlow.setStatus(Status.SUCCEEDED);
    executableFlows.add(executableFlow);

    final ExecutorManagerAdapter executorManager = getMockedExecutorManagerAdapter(project,
        executableFlows);

    final ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setSkipIfPreviousExecutionFailed(true);
    final ExecuteFlowAction executeFlowAction = new ExecuteFlowAction("action", 1, "project",
        "flow", "user", executionOptions, null);
    executeFlowAction.doAction();

    verify(executorManager).submitExecutableFlow(notNull(), eq("user"));
  }

  private ExecutorManagerAdapter getMockedExecutorManagerAdapter(
      final Project project, final List<ExecutableFlow> executableFlows)
      throws ExecutorManagerException {
    final ExecutorManagerAdapter executorManager = mock(ExecutorManagerAdapter.class);
    ExecuteFlowAction.setExecutorManager(executorManager);
    final ProjectManager projectManager = mock(ProjectManager.class);
    ExecuteFlowAction.setProjectManager(projectManager);

    when(projectManager.getProject(1)).thenReturn(project);

    //noinspection unchecked
    when(executorManager.getExecutableFlows(eq(1), eq("flow"), eq(0), intThat(i -> i >= 1),
        (List<ExecutableFlow>) notNull())).then(invocation -> {
      //noinspection unchecked
      ((List<ExecutableFlow>) invocation.getArgument(4)).addAll(executableFlows);
      return executableFlows.size();
    });
    return executorManager;
  }
}
