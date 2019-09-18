/*
 * Copyright 2018 LinkedIn Corp.
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

import java.util.Properties;
import javax.inject.Inject;
import javax.mail.NoSuchProviderException;


public class EmailMessageCreator {

  public static final int DEFAULT_SMTP_PORT = 25;
  public static final int DEFAULT_SMTP_SSL_PORT = 465;

  private final String mailHost;
  private final int mailPort;
  private final String mailUser;
  private final String mailPassword;
  private final String mailSender;
  private final String tls;
  private final String ssl;
  private final boolean usesAuth;

  @Inject
  public EmailMessageCreator(final Props props) {
    this.mailHost = props.getString("mail.host", "localhost");
    this.ssl = props.getString("mail.ssl", "false");
    this.mailPort = props.getInt("mail.port", "false".equalsIgnoreCase(this.ssl) ? DEFAULT_SMTP_PORT : DEFAULT_SMTP_SSL_PORT);
    this.mailUser = props.getString("mail.user", "");
    this.mailPassword = props.getString("mail.password", "");
    this.mailSender = props.getString("mail.sender", "");
    this.tls = props.getString("mail.tls", "false");
    this.usesAuth = props.getBoolean("mail.useAuth", true);
  }

  public EmailMessage createMessage() {
    final EmailMessage message = new EmailMessage(
        this.mailHost, this.mailPort, this.mailUser, this.mailPassword, this);
    message.setFromAddress(this.mailSender);
    message.setTLS(this.tls);
    message.setSSL(this.ssl);
    message.setAuth(this.usesAuth);
    return message;
  }

  public JavaxMailSender createSender(final Properties props) throws NoSuchProviderException {
    return new JavaxMailSender(props);
  }
}
