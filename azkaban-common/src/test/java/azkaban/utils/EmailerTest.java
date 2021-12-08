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
package azkaban.utils;

import static azkaban.Constants.ConfigurationKeys.JETTY_PORT;
import static azkaban.Constants.ConfigurationKeys.JETTY_USE_SSL;
import static azkaban.Constants.EventReporterConstants.MODIFIED_BY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.MockExecutorLoader;
import azkaban.executor.mail.DefaultMailCreatorTest;
import azkaban.flow.Flow;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.mail.internet.AddressException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class EmailerTest {

  private static final String receiveAddr = "receive@domain.com";//receiver email address
  private final List<String> receiveAddrList = new ArrayList<>();

  private Project project;
  private Props props;
  private EmailMessageCreator messageCreator;
  private EmailMessage message;
  private ExecutorLoader executorLoader;

  public static EmailMessageCreator mockMessageCreator(final EmailMessage message) {
    final EmailMessageCreator mock = mock(EmailMessageCreator.class);
    when(mock.createMessage()).thenReturn(message);
    return mock;
  }

  public static EmailMessage mockEmailMessage() {
    final EmailMessage message = mock(EmailMessage.class);
    final StringBuffer body = new StringBuffer();

    when(message.println(any())).thenAnswer((Answer<EmailMessage>) invocation -> {
      body.append(invocation.<Object>getArgument(0));
      return message;
    });

    when(message.getBody()).thenAnswer(invocation -> body.toString());

    return message;
  }

  public static Props createMailProperties() {
    final Props props = new Props();
    props.put("job.failure.email", receiveAddr);
    props.put("server.port", "114");
    props.put(JETTY_USE_SSL, "false");
    props.put("server.useSSL", "false");
    props.put(JETTY_PORT, "8786");
    return props;
  }

  @Before
  public void setUp() throws Exception {
    this.message = mockEmailMessage();
    this.messageCreator = mockMessageCreator(this.message);
    this.receiveAddrList.add(this.receiveAddr);
    this.project = new Project(11, "myTestProject");
    this.executorLoader = new MockExecutorLoader();

    this.props = createMailProperties();
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(this.props);
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"));
    assertThat(loader.getErrors()).isEmpty();
    this.project.setFlows(loader.getFlowMap());
    this.project.setVersion(123);
  }

  @Test
  public void testSendErrorEmail() throws Exception {
    final Flow flow = this.project.getFlow("jobe");
    flow.addFailureEmails(this.receiveAddrList);
    Assert.assertNotNull(flow);

    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final Emailer emailer = new Emailer(this.props, commonMetrics, this.messageCreator,
        this.executorLoader);
    emailer.alertOnError(exFlow);
    verify(this.message).addAllToAddress(this.receiveAddrList);
    verify(this.message).setSubject("Flow 'jobe' has FAILED on azkaban");
    assertThat(TestUtils.readResource("errorEmail2.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void alertOnFailedUpdate() throws Exception {
    final Flow flow = this.project.getFlow("jobe");
    flow.addFailureEmails(this.receiveAddrList);
    Assert.assertNotNull(flow);
    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final Emailer emailer = new Emailer(this.props, commonMetrics, this.messageCreator,
        this.executorLoader);
    final Executor executor = new Executor(1, "executor1-host", 1234, true);
    final List<ExecutableFlow> executions = Arrays.asList(exFlow, exFlow);
    final ExecutorManagerException exception = DefaultMailCreatorTest.createTestStracktrace();
    emailer.alertOnFailedUpdate(executor, executions, exception);
    verify(this.message).addAllToAddress(this.receiveAddrList);
    verify(this.message)
        .setSubject("Flow status could not be updated from executor1-host on azkaban");
    assertThat(TestUtils.readResource("failedUpdateMessage2.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void testAlertOnJobPropertyOverridden() throws Exception {
    final Flow flow = this.project.getFlow(("jobe"));
    flow.addOverrideEmails(this.receiveAddrList);
    final Map<String, Object> eventData = new HashMap<>();
    eventData.put(MODIFIED_BY, "dementer");
    eventData.put("jobOverridden", "jobe");
    eventData.put("diffMessage","Newly created Properties: [ Xmx, 2G], [ Xms, 1G]");
    Assert.assertNotNull(flow);
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final Emailer emailer = new Emailer(this.props, commonMetrics, this.messageCreator,
        this.executorLoader);
    emailer.alertOnJobPropertyOverridden(this.project, flow, eventData);
    verify(this.message).addAllToAddress(this.receiveAddrList);
    verify(this.message).setSubject("[Project Property Overridden Alert]");
  }

  @Test
  public void testGetAzkabanURL() {
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final Emailer emailer = new Emailer(this.props, commonMetrics, this.messageCreator,
        this.executorLoader);
    assertThat(emailer.getAzkabanURL()).isEqualTo("http://localhost:8786");
  }

  @Test
  public void testCreateEmailMessage() {
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final Emailer emailer = new Emailer(this.props, commonMetrics, this.messageCreator,
        this.executorLoader);
    final EmailMessage em = emailer
        .createEmailMessage("subject", "text/html", this.receiveAddrList);
    verify(this.messageCreator).createMessage();
    assertThat(this.messageCreator.createMessage()).isEqualTo(em);
    verify(this.message).addAllToAddress(this.receiveAddrList);
    verify(this.message).setSubject("subject");
    verify(this.message).setMimeType("text/html");
  }

  @Test
  public void testSendEmailToInvalidAddress() throws Exception {
    doThrow(AddressException.class).when(this.message).sendEmail();
    final Flow flow = this.project.getFlow("jobe");
    flow.addFailureEmails(this.receiveAddrList);

    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    final CommonMetrics commonMetrics = mock(CommonMetrics.class);
    final Emailer emailer = new Emailer(this.props, commonMetrics, this.messageCreator,
        this.executorLoader);
    emailer.alertOnError(exFlow);
    verify(commonMetrics, never()).markSendEmailFail();
  }
}
