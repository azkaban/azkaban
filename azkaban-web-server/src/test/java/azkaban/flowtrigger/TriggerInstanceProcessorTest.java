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
package azkaban.flowtrigger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.project.CronSchedule;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.utils.Emailer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.junit.BeforeClass;
import org.junit.Test;


public class TriggerInstanceProcessorTest {

  private static final String EMAIL = "test@email.com";
  private static FlowTriggerInstanceLoader triggerInstLoader;
  private static ExecutorManager executorManager;
  private static Emailer emailer;
  private static TriggerInstanceProcessor processor;

  @BeforeClass
  public static void setup() throws Exception {
    triggerInstLoader = mock(FlowTriggerInstanceLoader.class);
    executorManager = mock(ExecutorManager.class);
    when(executorManager.submitExecutableFlow(any(), anyString())).thenReturn("return");
    emailer = mock(Emailer.class);
    doNothing().when(emailer).sendEmail(any(), any(), any());
    processor = new TriggerInstanceProcessor(executorManager, triggerInstLoader, emailer);
  }

  private static TriggerInstance createTriggerInstance() {
    final FlowTrigger flowTrigger = new FlowTrigger(
        new CronSchedule("* * * * ? *"),
        new ArrayList<>(),
        Duration.ofMinutes(1)
    );
    final Project proj = new Project(1, "proj");
    final Flow flow = new Flow("flowId");
    flow.addFailureEmails(Lists.newArrayList(EMAIL));
    proj.setFlows(Maps.newHashMap("flowId", flow));
    final List<DependencyInstance> depInstList = new ArrayList<>();
    return new TriggerInstance("instanceId", flowTrigger, "flowId", 1,
        "test", depInstList, -1, proj);
  }

  @Test
  public void testProcessSucceed() throws ExecutorManagerException {
    final TriggerInstance triggerInstance = createTriggerInstance();
    processor.processSucceed(triggerInstance);
    verify(executorManager).submitExecutableFlow(any(), anyString());
    verify(triggerInstLoader).updateAssociatedFlowExecId(triggerInstance);
  }

  @Test
  public void testProcessTermination() throws ExecutorManagerException {
    final TriggerInstance triggerInstance = createTriggerInstance();
    processor.processTermination(triggerInstance);
    verify(emailer).sendEmail(any(), any(), any());
  }

  @Test
  public void testNewInstance() throws ExecutorManagerException {
    final TriggerInstance triggerInstance = createTriggerInstance();
    processor.processNewInstance(triggerInstance);
    verify(triggerInstLoader).uploadTriggerInstance(triggerInstance);
  }
}
