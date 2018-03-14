/*
 * Copyright 2012 LinkedIn Corp.
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

import java.util.Collection;

public class AbstractMailer {

  public static final int DEFAULT_SMTP_PORT = 25;
  private final boolean usesAuth;

  private final String mailHost;
  private final int mailPort;
  private final String mailUser;
  private final String mailPassword;
  private final String mailSender;
  private final String azkabanName;
  private final String tls;

  public AbstractMailer(final Props props) {
    this.azkabanName = props.getString("azkaban.name", "azkaban");
    this.mailHost = props.getString("mail.host", "localhost");
    this.mailPort = props.getInt("mail.port", DEFAULT_SMTP_PORT);
    this.mailUser = props.getString("mail.user", "");
    this.mailPassword = props.getString("mail.password", "");
    this.tls = props.getString("mail.tls", "false");
    this.mailSender = props.getString("mail.sender", "");
    this.usesAuth = props.getBoolean("mail.useAuth", true);
  }

  protected EmailMessage createEmailMessage(final String subject, final String mimetype,
      final Collection<String> emailList) {
    final EmailMessage message = new EmailMessage(this.mailHost, this.mailPort, this.mailUser,
        this.mailPassword);
    message.setFromAddress(this.mailSender);
    message.addAllToAddress(emailList);
    message.setMimeType(mimetype);
    message.setSubject(subject);
    message.setAuth(this.usesAuth);
    message.setTLS(this.tls);

    return message;
  }

  public String getAzkabanName() {
    return this.azkabanName;
  }

  public boolean hasMailAuth() {
    return this.usesAuth;
  }
}
