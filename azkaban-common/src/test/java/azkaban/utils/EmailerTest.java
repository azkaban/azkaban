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

import azkaban.executor.ExecutableFlow;
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
import org.junit.Ignore;
import org.junit.Test;


public class EmailerTest {

  String host = "smtp.domain.com";//smtp server address
  int mailPort = 25;//smtp server port
  String sender = "somebody@domain.com";//sender email address
  String user = "somebody@domain.com";// the sender username
  String password = "pwd"; //the sender password

  String receiveAddr = "receive@domain.com";//receiver email address
  List<String> receiveAddrList = new ArrayList<>();

  private Project project;
  private Props props;


  @Before
  public void setUp() throws Exception {
    this.receiveAddrList.add(this.receiveAddr);
    this.project = new Project(11, "myTestProject");

    this.props = createMailProperties();
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(this.props);
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"));
    Assert.assertEquals(0, loader.getErrors().size());
    this.project.setFlows(loader.getFlowMap());
    this.project.setVersion(123);
  }


  /**
   * this is an integration test for Emailer sending  email. if you want to run this case and send
   * email successfully, please remove @Ignore and make sure these variable{host,mailPort,password,receiveAddr}
   * are set to real values. the test will currently succeed because email sending errors are
   * caught, you need to manually verify that a real email is sent and received.
   */
  @Ignore
  @Test
  public void testSendEmail() throws Exception {

    final Flow flow = this.project.getFlow("jobe");
    flow.addFailureEmails(this.receiveAddrList);
    Assert.assertNotNull(flow);

    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final Emailer emailer = new Emailer(this.props, commonMetrics);
    emailer.sendErrorEmail(exFlow);

  }

  @Test
  public void testCreateEmailMessage() {
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final Emailer emailer = new Emailer(this.props, commonMetrics);
    final EmailMessage em = emailer
        .createEmailMessage("subject", "text/html", this.receiveAddrList);
    assert em.getMailPort() == this.mailPort;

  }


  public Props createMailProperties() {
    final Props props = new Props();
    props.put("mail.user", this.user);
    props.put("mail.password", this.password);
    props.put("mail.sender", this.sender);
    props.put("mail.host", this.host);
    props.put("mail.port", this.mailPort);
    props.put("job.failure.email", this.receiveAddr);
    props.put("server.port", "114");
    props.put("jetty.use.ssl", "false");
    props.put("server.useSSL", "false");
    props.put("jetty.port", "8786");
    return props;
  }


}
