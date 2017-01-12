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

import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class EmailerTest {

    List<String> senderList = new ArrayList<String>();


    @Before
    public void setUp() throws Exception {
        senderList.add("sender@domain.com");
    }

    /**
     * test emailMessage properties
     */
    @Test
    public void testCreateEmailMessage() throws Exception{

        URL url = Resources.getResource("conf/emailtest/azkaban.properties");
        InputStream inputStream = new FileInputStream(url.getFile());

        Properties properties = new Properties();
        properties.load(inputStream);

        Props props = new Props();
        props.put(properties);
        props.put("server.port","114");
        props.put("jetty.use.ssl","false");
        props.put("server.useSSL","false");
        props.put("jetty.port","8786");

        Emailer emailer = new Emailer(props);
        EmailMessage emailMessage = emailer.createEmailMessage("subject","text/html",senderList);


        assert(properties.getProperty("mail.user").equals(emailer.getMailUser()));

        int mailport =  Integer.parseInt((String)properties.getOrDefault("mail.port","25"));
        assert emailer.getMailPort()== mailport;
        assert emailMessage.getMailPort() == mailport;
    }






}
