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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.MockExecutorLoader;
import azkaban.flow.Flow;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.project.CronSchedule;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.utils.EmailMessage;
import azkaban.utils.EmailMessageCreator;
import azkaban.utils.Emailer;
import azkaban.utils.EmailerTest;
import azkaban.utils.TestUtils;
import com.codahale.metrics.MetricRegistry;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TriggerInstanceProcessorTest {

  private static final String EMAIL = "test@email.com";
  private FlowTriggerInstanceLoader triggerInstLoader;
  private ExecutorManagerAdapter executorManager;
  private Emailer emailer;
  private EmailMessage message;
  private EmailMessageCreator messageCreator;
  private TriggerInstanceProcessor processor;
  private CountDownLatch sendEmailLatch;
  private CountDownLatch submitFlowLatch;
  private CountDownLatch updateExecIDLatch;
  private ExecutorLoader executorLoader;

  private static TriggerInstance createTriggerInstance() throws ParseException {
    final FlowTrigger flowTrigger = new FlowTrigger(
        new CronSchedule("* * * * ? *"),
        new ArrayList<>(),
        Duration.ofMinutes(1)
    );
    final Project proj = new Project(1, "proj");
    final Flow flow = new Flow("123");
    flow.addFailureEmails(Lists.newArrayList(EMAIL));
    proj.setFlows(Maps.newHashMap("flowId", flow));
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sdf.setTimeZone(TimeZone.getDefault());
    final Date startDate = sdf.parse("2000-01-11 16:00:00");
    final Date endDate = sdf.parse("2000-01-11 16:00:00");

    final List<DependencyInstance> depInstList = Arrays.asList(
        new DependencyInstance("dep1", startDate.getTime(), endDate.getTime(), null,
            Status.CANCELLED, CancellationCause.MANUAL),
        new DependencyInstance("dep2", startDate.getTime(), endDate.getTime(), null,
            Status.SUCCEEDED, CancellationCause.NONE),
        new DependencyInstance("dep3", startDate.getTime(), endDate.getTime(), null,
            Status.CANCELLED, CancellationCause.TIMEOUT),
        new DependencyInstance("dep4", startDate.getTime(), endDate.getTime(), null,
            Status.CANCELLED, CancellationCause.CASCADING)
    );

    return new TriggerInstance("instanceId", flowTrigger, "flowId", 1,
        "test", depInstList, -1, proj);
  }

  @Before
  public void setUp() throws Exception {
    this.message = EmailerTest.mockEmailMessage();
    this.messageCreator = EmailerTest.mockMessageCreator(this.message);
    this.triggerInstLoader = mock(FlowTriggerInstanceLoader.class);
    this.executorManager = mock(ExecutorManagerAdapter.class);
    this.executorLoader = new MockExecutorLoader();
    when(this.executorManager.createExecutableFlow(any(), any())).thenReturn(mock(ExecutableFlow.class));
    when(this.executorManager.submitExecutableFlow(any(), anyString())).thenReturn("return");
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    this.emailer = Mockito.spy(new Emailer(EmailerTest.createMailProperties(), commonMetrics,
        this.messageCreator, this.executorLoader));

    this.sendEmailLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      this.sendEmailLatch.countDown();
      return null;
    }).when(this.emailer).sendEmail(any(EmailMessage.class), anyBoolean(), anyString());

    this.submitFlowLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      this.submitFlowLatch.countDown();
      return null;
    }).when(this.executorManager).submitExecutableFlow(any(), anyString());

    this.updateExecIDLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      this.updateExecIDLatch.countDown();
      return null;
    }).when(this.triggerInstLoader).updateAssociatedFlowExecId(any());

    this.processor = new TriggerInstanceProcessor(this.executorManager, this.triggerInstLoader,
        this.emailer);
  }

  @Test
  public void testProcessSucceed() throws Exception {
    final TriggerInstance triggerInstance = createTriggerInstance();
    this.processor.processSucceed(triggerInstance);
    this.submitFlowLatch.await(10L, TimeUnit.SECONDS);
    verify(this.executorManager).submitExecutableFlow(any(), anyString());
    this.updateExecIDLatch.await(10L, TimeUnit.SECONDS);
    verify(this.triggerInstLoader).updateAssociatedFlowExecId(triggerInstance);
  }

  @Test
  public void testProcessTermination() throws Exception {
    final TriggerInstance triggerInstance = createTriggerInstance();
    this.processor.processTermination(triggerInstance);
    this.sendEmailLatch.await(10L, TimeUnit.SECONDS);
    assertEquals(0, this.sendEmailLatch.getCount());
    verify(this.message).setSubject(
        "flow trigger for flow 'flowId', project 'proj' has been cancelled on azkaban");
    assertThat(TestUtils.readResource("/emailTemplate/flowtriggerfailureemail.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void testNewInstance() throws ParseException {
    final TriggerInstance triggerInstance = createTriggerInstance();
    this.processor.processNewInstance(triggerInstance);
    verify(this.triggerInstLoader).uploadTriggerInstance(triggerInstance);
  }
}

