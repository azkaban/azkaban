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
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.test.executions.TestExecutions;
import com.google.common.io.Resources;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class EmailerTest {

    String host = "smtp.domain.com";
    int mailPort = 25;
    String sender = "somebody@domain.com";
    String user = "somebody@domain.com";
    String password = "pwd";

    String receiveAddr = "receive@domain.com";
    List<String> receiveAddrList = new ArrayList<String>();

    private Project project;
    private Props props;




    @Before
    public void setUp() throws Exception {
        receiveAddrList.add(receiveAddr);
        project = new Project(11, "myTestProject");
        Logger logger = Logger.getLogger(this.getClass());

        props =  createMailProperties();
        DirectoryFlowLoader loader = new DirectoryFlowLoader(props, logger);
        loader.loadProjectFlow(project, TestExecutions.getFlowDir("embedded"));
        Assert.assertEquals(0, loader.getErrors().size());
        project.setFlows(loader.getFlowMap());
        project.setVersion(123);
    }


    /**
     * (test send email)
     * if you want to run this case,
     * please make sure these variable{host,mailPort,password,receiveAddr} effective.
     *
     */
    @Ignore
    @Test
    public void testSendEmail() throws Exception{

        Flow flow = project.getFlow("jobe");
        flow.addFailureEmails(receiveAddrList);
        Assert.assertNotNull(flow);

        ExecutableFlow exFlow = new ExecutableFlow(project, flow);
        Emailer emailer = new Emailer(props);
        emailer.sendErrorEmail(exFlow);

    }
    @Test
    public void testCreateEmailMessage(){
        Emailer emailer = new Emailer(props);
        EmailMessage em = emailer.createEmailMessage("subject","text/html",receiveAddrList);
        assert  em.getMailPort() == mailPort;

    }




    public   Props  createMailProperties(){
        Props props = new Props();
        props.put("mail.user",user);
        props.put("mail.password",password);
        props.put("mail.sender",sender);
        props.put("mail.host",host);
        props.put("mail.port",mailPort);
        props.put("job.failure.email",receiveAddr);
        props.put("server.port","114");
        props.put("jetty.use.ssl","false");
        props.put("server.useSSL","false");
        props.put("jetty.port","8786");
        return props;
    }





}
