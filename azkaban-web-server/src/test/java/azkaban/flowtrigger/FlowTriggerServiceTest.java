/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.flowtrigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginManager;
import azkaban.flowtrigger.testplugin.TestDependencyCheck;
import azkaban.flowtrigger.util.TestUtil;
import azkaban.project.FlowTrigger;
import azkaban.project.FlowTriggerDependency;
import azkaban.project.Project;
import azkaban.utils.Emailer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;


public class FlowTriggerServiceTest {

  private static final FlowTriggerInstanceLoader flowTriggerInstanceLoader = new
      MockFlowTriggerInstanceLoader();
  private static TestDependencyCheck testDepCheck;
  private static FlowTriggerService flowTriggerService;
  private static ExecutorManager executorManager;

  @BeforeClass
  public static void setup() throws Exception {
    testDepCheck = new TestDependencyCheck();
    final FlowTriggerDependencyPluginManager pluginManager = mock(FlowTriggerDependencyPluginManager
        .class);
    when(pluginManager.getDependencyCheck(ArgumentMatchers.eq("TestDependencyCheck")))
        .thenReturn(testDepCheck);

    executorManager = mock(ExecutorManager.class);
    when(executorManager.submitExecutableFlow(any(), anyString())).thenReturn("return");

    final Emailer emailer = mock(Emailer.class);
    Mockito.doNothing().when(emailer).sendEmail(any(), anyString(), anyString());

    final TriggerInstanceProcessor triggerInstProcessor = new TriggerInstanceProcessor(
        executorManager,
        flowTriggerInstanceLoader, emailer);
    final DependencyInstanceProcessor depInstProcessor = new DependencyInstanceProcessor
        (flowTriggerInstanceLoader);

    final FlowTriggerExecutionCleaner executionCleaner = new FlowTriggerExecutionCleaner(
        flowTriggerInstanceLoader);

    flowTriggerService = new FlowTriggerService(pluginManager, triggerInstProcessor,
        depInstProcessor, flowTriggerInstanceLoader, executionCleaner);
    flowTriggerService.start();
  }

  @Before
  public void cleanup() {
    ((MockFlowTriggerInstanceLoader) flowTriggerInstanceLoader).clear();
    reset(executorManager);
  }

  private Project createProject() {
    final Project project = new Project(1, "project1");
    project.setVersion(1);
    final Flow flow = new Flow("testflow");
    final Map<String, Flow> flowMap = new HashMap<>();
    flowMap.put("testflow", flow);
    project.setFlows(flowMap);
    return project;
  }

  @Ignore("Too slow unit test - ignored until optimized")
  @Test
  public void testStartTriggerCancelledByTimeout() throws InterruptedException {

    final List<FlowTriggerDependency> deps = new ArrayList<>();
    deps.add(TestUtil.createTestDependency("2secs", 2, false));
    deps.add(TestUtil.createTestDependency("8secs", 8, false));
    deps.add(TestUtil.createTestDependency("9secs", 9, false));
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(deps, Duration.ofSeconds(5));
    for (int i = 0; i < 30; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofSeconds(6).toMillis());
    assertThat(flowTriggerService.getRunningTriggers()).isEmpty();
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(30);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.CANCELLED);
      for (final DependencyInstance depInst : inst.getDepInstances()) {
        if (depInst.getDepName().equals("2secs")) {
          assertThat(depInst.getStatus()).isEqualTo(Status.SUCCEEDED);
        } else if (depInst.getDepName().equals("8secs")) {
          assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
          assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.TIMEOUT);
        } else if (depInst.getDepName().equals("9secs")) {
          assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
          assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.TIMEOUT);
        }
      }
    }
  }

  @Test
  public void testStartTriggerCancelledManually() throws InterruptedException {
    final List<FlowTriggerDependency> deps = new ArrayList<>();
    deps.add(TestUtil.createTestDependency("2secs", 2, false));
    deps.add(TestUtil.createTestDependency("8secs", 8, false));
    deps.add(TestUtil.createTestDependency("9secs", 9, false));
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(deps, Duration.ofSeconds(5));
    for (int i = 0; i < 30; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }

    Thread.sleep(Duration.ofMillis(500).toMillis());
    for (final TriggerInstance runningTrigger : flowTriggerService.getRunningTriggers()) {
      flowTriggerService.cancelTriggerInstance(runningTrigger, CancellationCause.MANUAL);
    }
    Thread.sleep(Duration.ofMillis(500).toMillis());
    assertThat(flowTriggerService.getRunningTriggers()).isEmpty();
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(30);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.CANCELLED);
      for (final DependencyInstance depInst : inst.getDepInstances()) {
        assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
        assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.MANUAL);
      }
    }
  }

  @Test
  public void testStartTriggerCancelledByFailure() throws InterruptedException {
    final List<FlowTriggerDependency> deps = new ArrayList<>();
    deps.add(TestUtil.createTestDependency("2secs", 2, true));
    deps.add(TestUtil.createTestDependency("8secs", 8, false));
    deps.add(TestUtil.createTestDependency("9secs", 9, false));
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(deps, Duration.ofSeconds(10));
    for (int i = 0; i < 30; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofSeconds(1).toMillis());
    assertThat(flowTriggerService.getRunningTriggers()).isEmpty();
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(30);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.CANCELLED);
      for (final DependencyInstance depInst : inst.getDepInstances()) {
        if (depInst.getDepName().equals("2secs")) {
          assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
          assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.FAILURE);
        } else {
          assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
          assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.CASCADING);
        }
      }
    }
  }

  @Ignore("Too slow unit test - ignored until optimized")
  @Test
  public void testStartTriggerSuccess() throws InterruptedException {
    final List<FlowTriggerDependency> deps = new ArrayList<>();
    deps.add(TestUtil.createTestDependency("2secs", 2, false));
    deps.add(TestUtil.createTestDependency("3secs", 3, false));
    deps.add(TestUtil.createTestDependency("4secs", 4, false));
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(deps, Duration.ofSeconds(10));
    for (int i = 0; i < 30; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofSeconds(5).toMillis());
    assertThat(flowTriggerService.getRunningTriggers()).isEmpty();
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(30);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.SUCCEEDED);
    }
  }

  @Ignore("Flaky test - ignored until stabilized")
  @Test
  public void testStartZeroDependencyTrigger()
      throws InterruptedException, ExecutorManagerException {
    final List<FlowTriggerDependency> deps = new ArrayList<>();
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(deps, Duration.ofSeconds(10));
    for (int i = 0; i < 30; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofSeconds(1).toMillis());
    // zero dependency trigger will launch associated flow immediately
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRunningTriggers();
    assertThat(triggerInstances).isEmpty();
    verify(executorManager, times(30)).submitExecutableFlow(any(), anyString());
  }

  @Ignore("Flaky test - ignored until stabilized")
  @Test
  public void testRecovery() throws Exception {
    final List<FlowTriggerDependency> deps = new ArrayList<>();
    deps.add(TestUtil.createTestDependency("2secs", 2, false));
    deps.add(TestUtil.createTestDependency("3secs", 3, false));
    deps.add(TestUtil.createTestDependency("4secs", 4, false));
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(deps, Duration.ofSeconds(10));
    for (int i = 0; i < 30; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofSeconds(1).toMillis());
    flowTriggerService.shutdown();
    setup();
    Thread.sleep(Duration.ofSeconds(5).toMillis());
    assertThat(flowTriggerService.getRunningTriggers()).isEmpty();
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(30);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.SUCCEEDED);
    }
  }
}
