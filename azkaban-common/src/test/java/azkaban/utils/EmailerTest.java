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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.mail.DefaultMailCreatorTest;
import azkaban.flow.Flow;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class EmailerTest {

  private final String receiveAddr = "receive@domain.com";//receiver email address
  private final List<String> receiveAddrList = new ArrayList<>();

  private Project project;
  private Props props;
  private EmailMessageCreator messageCreator;
  private EmailMessage message;

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

  @Before
  public void setUp() throws Exception {
    this.message = mockEmailMessage();
    this.messageCreator = mockMessageCreator(this.message);
    this.receiveAddrList.add(this.receiveAddr);
    this.project = new Project(11, "myTestProject");

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
    final Emailer emailer = new Emailer(this.props, commonMetrics, this.messageCreator);
    emailer.sendErrorEmail(exFlow);
    verify(this.message).addAllToAddress(this.receiveAddrList);
    verify(this.message).setSubject("Flow 'jobe' has failed on azkaban");
    assertThat(DefaultMailCreatorTest.read("errorEmail2.html"))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void testCreateEmailMessage() {
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final Emailer emailer = new Emailer(this.props, commonMetrics, this.messageCreator);
    final EmailMessage em = emailer
        .createEmailMessage("subject", "text/html", this.receiveAddrList);
    verify(this.messageCreator).createMessage();
    assertThat(this.messageCreator.createMessage()).isEqualTo(em);
    verify(this.message).addAllToAddress(this.receiveAddrList);
    verify(this.message).setSubject("subject");
    verify(this.message).setMimeType("text/html");
  }

  private Props createMailProperties() {
    final Props props = new Props();
    props.put("job.failure.email", this.receiveAddr);
    props.put("server.port", "114");
    props.put("jetty.use.ssl", "false");
    props.put("server.useSSL", "false");
    props.put("jetty.port", "8786");
    return props;
  }
}
