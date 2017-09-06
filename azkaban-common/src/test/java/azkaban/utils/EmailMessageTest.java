/*
 * Copyright 2014 LinkedIn Corp.
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

import java.io.IOException;
import javax.mail.MessagingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class EmailMessageTest {

  String host = "";
  int port = 25;
  String sender = "";
  String user = "";
  String password = "";

  String toAddr = "";

  private EmailMessage em;

  @Before
  public void setUp() throws Exception {
    this.em = new EmailMessage(this.host, this.port, this.user, this.password);
    this.em.setFromAddress(this.sender);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Ignore
  @Test
  public void testSendEmail() throws IOException {
    this.em.addToAddress(this.toAddr);
    // em.addToAddress("cyu@linkedin.com");
    this.em.setSubject("azkaban test email");
    this.em.setBody("azkaban test email");
    try {
      this.em.sendEmail();
    } catch (final MessagingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
