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
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutorManager;
import azkaban.flow.Flow;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginManager;
import azkaban.flowtrigger.testplugin.TestDependencyCheck;
import azkaban.flowtrigger.util.TestUtil;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.utils.Emailer;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;


public class FlowTriggerServiceTest {

  private static final String pluginDir = "dependencyplugin";
  private static final FlowTriggerInstanceLoader flowTriggerInstanceLoader = new
      MockFlowTriggerInstanceLoader();
  private static TestDependencyCheck testDepCheck;
  private static FlowTriggerService flowTriggerService;

  @BeforeClass
  public static void setup() throws Exception {
    testDepCheck = new TestDependencyCheck();
    final FlowTriggerDependencyPluginManager pluginManager = mock(FlowTriggerDependencyPluginManager
        .class);
    when(pluginManager.getDependencyCheck(ArgumentMatchers.eq("TestDependencyCheck")))
        .thenReturn(testDepCheck);

    final ExecutorManager executorManager = mock(ExecutorManager.class);
    when(executorManager.submitExecutableFlow(any(), anyString())).thenReturn("return");

    final Emailer emailer = mock(Emailer.class);
    Mockito.doNothing().when(emailer).sendEmail(any(), anyString(), anyString());

    final TriggerInstanceProcessor triggerInstProcessor = new TriggerInstanceProcessor(
        executorManager,
        flowTriggerInstanceLoader, emailer);
    final DependencyInstanceProcessor depInstProcessor = new DependencyInstanceProcessor
        (flowTriggerInstanceLoader);

    flowTriggerService = new FlowTriggerService(pluginManager,
        triggerInstProcessor, depInstProcessor, flowTriggerInstanceLoader);
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

  @Test
  public void testStartTriggerCancelledByTimeout() throws InterruptedException {
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(1);
    for (int i = 0; i < 10; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofMinutes(1).toMillis() + Duration.ofSeconds(3).toMillis());
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(10);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.CANCELLED);
      for (final DependencyInstance depInst : inst.getDepInstances()) {
        if (depInst.getDepName() == "10secs") {
          assertThat(depInst.getStatus()).isEqualTo(Status.SUCCEEDED);
        } else if (depInst.getDepName() == "65secs") {
          assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
          assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.TIMEOUT);
        } else if (depInst.getDepName() == "66secs") {
          assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
          assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.TIMEOUT);
        }
      }
    }
  }

  @Test
  public void testStartTriggerCancelledByFailure() throws InterruptedException {
    final FlowTrigger flowTrigger = TestUtil.createFailedTestFlowTrigger(1);
    for (int i = 0; i < 10; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofSeconds(2).toMillis());
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(10);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.CANCELLED);
      for (final DependencyInstance depInst : inst.getDepInstances()) {
        if (depInst.getDepName() == "15secs") {
          assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
          assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.FAILURE);
        } else {
          assertThat(depInst.getStatus()).isEqualTo(Status.CANCELLED);
          assertThat(depInst.getCancellationCause()).isEqualTo(CancellationCause.CASCADING);
        }
      }
    }
  }

  @Test
  public void testStartTriggerSuccess() throws InterruptedException {
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(5);
    for (int i = 0; i < 10; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofMinutes(1).toMillis() + Duration.ofSeconds(8).toMillis());
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(10);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.SUCCEEDED);
    }
  }

  @Test
  public void testRecovery() throws Exception {
    final FlowTrigger flowTrigger = TestUtil.createTestFlowTrigger(5);
    for (int i = 0; i < 10; i++) {
      flowTriggerService.startTrigger(flowTrigger, "testflow", 1, "test", createProject());
    }
    Thread.sleep(Duration.ofSeconds(1).toMillis());
    flowTriggerService.shutdown();
    setup();
    flowTriggerService.recoverIncompleteTriggerInstances();
    Thread.sleep(Duration.ofMinutes(1).toMillis() + Duration.ofSeconds(8).toMillis());
    final Collection<TriggerInstance> triggerInstances = flowTriggerService.getRecentlyFinished();
    assertThat(triggerInstances).hasSize(10);
    for (final TriggerInstance inst : triggerInstances) {
      assertThat(inst.getStatus()).isEqualTo(Status.SUCCEEDED);
    }
  }
}


