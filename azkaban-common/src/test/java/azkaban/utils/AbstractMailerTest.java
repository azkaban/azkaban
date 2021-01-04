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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class AbstractMailerTest {

  List<String> senderList = new ArrayList<>();
  private EmailMessage message;
  private EmailMessageCreator messageCreator;
  private Props props;

  @Before
  public void setUp() throws Exception {
    this.message = EmailerTest.mockEmailMessage();
    this.messageCreator = EmailerTest.mockMessageCreator(this.message);
    this.senderList.add("sender@domain.com");
    this.props = new Props();
    this.props.put("server.port", "114");
    this.props.put(JETTY_USE_SSL, "false");
    this.props.put("server.useSSL", "false");
    this.props.put(JETTY_PORT, "8786");
  }

  @Test
  public void testCreateEmailMessage() {
    final AbstractMailer mailer = new AbstractMailer(this.props, this.messageCreator);
    final EmailMessage em = mailer.createEmailMessage("subject", "text/html", this.senderList);
    verify(this.messageCreator).createMessage();
    assertThat(this.message).isEqualTo(em);
    verify(this.message).setSubject("subject");
  }

}
