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

import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class AbstractMailerTest {

  List<String> senderList = new ArrayList<String>();

  public static Props createMailProperties() {
    Props props = new Props();
    props.put("mail.user", "somebody");
    props.put("mail.password", "pwd");
    props.put("mail.sender", "somebody@xxx.com");
    props.put("server.port", "114");
    props.put("jetty.use.ssl", "false");
    props.put("server.useSSL", "false");
    props.put("jetty.port", "8786");
    return props;

  }

  @Before
  public void setUp() throws Exception {
    senderList.add("sender@domain.com");
  }

  /**
   * test emailMessage properties
   */
  @Test
  public void testCreateEmailMessage() {

    Props props = createMailProperties();
    props.put("mail.port", "445");
    AbstractMailer mailer = new AbstractMailer(props);
    EmailMessage emailMessage = mailer.createEmailMessage("subject", "text/html", senderList);

    assert emailMessage.getMailPort() == 445;


  }

  @Test
  public void testCreateDefaultEmailMessage() {
    Props defaultProps = createMailProperties();
    AbstractMailer mailer = new AbstractMailer(defaultProps);
    EmailMessage emailMessage = mailer.createEmailMessage("subject", "text/html", senderList);
    assert emailMessage.getMailPort() == 25;

  }

}
