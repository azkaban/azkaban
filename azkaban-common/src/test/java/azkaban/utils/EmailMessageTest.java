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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.internet.InternetAddress;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class EmailMessageTest {

  private final String host = "example.com";
  private final int port = 25;
  private final String sender = "from@example.com";
  private final String user = "user";
  private final String password = "pass";
  private final String toAddr = "to@example.com";

  private EmailMessage em;
  private JavaxMailSender mailSender;
  private Message mimeMessage;
  private Address[] addresses;
  private EmailMessageCreator creator;

  @Before
  public void setUp() throws Exception {
    this.creator = mock(EmailMessageCreator.class);
    this.mailSender = mock(JavaxMailSender.class);
    this.mimeMessage = mock(Message.class);
    this.addresses = new Address[]{new InternetAddress(this.toAddr, false)};
    when(this.creator.createSender(any())).thenReturn(this.mailSender);
    when(this.mailSender.createMessage()).thenReturn(this.mimeMessage);
    when(this.mimeMessage.getRecipients(Message.RecipientType.TO)).thenReturn(this.addresses);
    this.em = new EmailMessage(this.host, this.port, this.user, this.password, this.creator);
  }

  @Test
  public void testSendEmail() throws Exception {
    this.em.setTLS("true");
    this.em.addToAddress(this.toAddr);
    this.em.setFromAddress(this.sender);
    this.em.setSubject("azkaban test email");
    this.em.setBody("azkaban test email");
    this.em.sendEmail();
    verify(this.mimeMessage).addRecipient(RecipientType.TO, this.addresses[0]);
    verify(this.mailSender).sendMessage(this.mimeMessage, this.addresses);
  }

  @Test
  public void testSendEmailFailed() throws Exception {
    Mockito.doThrow(new IllegalArgumentException("mocked exception"))
        .when(this.mailSender).sendMessage(this.mimeMessage, this.addresses);
    this.em.setTLS("true");
    this.em.addToAddress(this.toAddr);
    this.em.setFromAddress(this.sender);
    this.em.setSubject("azkaban test email");
    this.em.setBody("azkaban test email");
    assertThatExceptionOfType(MessagingException.class).isThrownBy(() -> this.em.sendEmail());
    verify(this.mimeMessage).addRecipient(RecipientType.TO, this.addresses[0]);
    verify(this.mailSender, Mockito.times(5)).sendMessage(this.mimeMessage, this.addresses);
  }

}
