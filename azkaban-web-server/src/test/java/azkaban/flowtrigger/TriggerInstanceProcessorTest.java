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
import static org.mockito.Mockito.doAnswer;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;


public class TriggerInstanceProcessorTest {

  private static final String EMAIL = "test@email.com";
  private FlowTriggerInstanceLoader triggerInstLoader;
  private ExecutorManager executorManager;
  private Emailer emailer;
  private TriggerInstanceProcessor processor;
  private CountDownLatch sendEmailLatch;

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

  @Before
  public void setUp() throws Exception {
    this.triggerInstLoader = mock(FlowTriggerInstanceLoader.class);
    this.executorManager = mock(ExecutorManager.class);
    when(this.executorManager.submitExecutableFlow(any(), anyString())).thenReturn("return");
    this.emailer = mock(Emailer.class);
    this.sendEmailLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      this.sendEmailLatch.countDown();
      return null;
    }).when(this.emailer).sendEmail(any(), any(), any());
    this.processor = new TriggerInstanceProcessor(this.executorManager, this.triggerInstLoader,
        this.emailer);
  }

  @Test
  public void testProcessSucceed() throws ExecutorManagerException {
    final TriggerInstance triggerInstance = createTriggerInstance();
    this.processor.processSucceed(triggerInstance);
    verify(this.executorManager).submitExecutableFlow(any(), anyString());
    verify(this.triggerInstLoader).updateAssociatedFlowExecId(triggerInstance);
  }

  @Test
  public void testProcessTermination() throws Exception {
    final TriggerInstance triggerInstance = createTriggerInstance();
    this.processor.processTermination(triggerInstance);
    this.sendEmailLatch.await(10L, TimeUnit.SECONDS);
    verify(this.emailer).sendEmail(any(), any(), any());
  }

  @Test
  public void testNewInstance() {
    final TriggerInstance triggerInstance = createTriggerInstance();
    this.processor.processNewInstance(triggerInstance);
    verify(this.triggerInstLoader).uploadTriggerInstance(triggerInstance);
  }
}
