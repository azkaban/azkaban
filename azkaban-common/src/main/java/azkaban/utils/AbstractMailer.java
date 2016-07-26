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
  private static int MB_IN_BYTES = 1048576;
  private String clientHostname;
  private int clientPort;
  private boolean usesSSL;
  private boolean usesAuth;

  private String mailHost;
  private String mailUser;
  private String mailPassword;
  private String mailSender;
  private String azkabanName;
  private String tls;

  private String referenceURL;

  private long attachmentMazSizeInByte;

  public AbstractMailer(Props props) {
    this.azkabanName = props.getString("azkaban.name", "azkaban");
    this.mailHost = props.getString("mail.host", "localhost");
    this.mailUser = props.getString("mail.user", "");
    this.mailPassword = props.getString("mail.password", "");
    this.tls = props.getString("mail.tls", "false");
    long maxAttachmentSizeInMB =
        props.getInt("mail.max.attachment.size.mb", 100);

    attachmentMazSizeInByte = maxAttachmentSizeInMB * MB_IN_BYTES;

    this.mailSender = props.getString("mail.sender", "");
    this.usesAuth = props.getBoolean("mail.useAuth", true);

    this.clientHostname = props.get("server.hostname");
    this.clientPort = props.getInt("server.port");
    this.usesSSL = props.getBoolean("server.useSSL");

    if (usesSSL) {
      referenceURL =
          "https://" + clientHostname
              + (clientPort == 443 ? "/" : ":" + clientPort + "/");
    } else {
      referenceURL =
          "http://" + clientHostname
              + (clientPort == 80 ? "/" : ":" + clientPort + "/");
    }
  }

  public String getReferenceURL() {
    return referenceURL;
  }

  protected EmailMessage createEmailMessage(String subject, String mimetype,
      Collection<String> emailList) {
    EmailMessage message = new EmailMessage(mailHost, mailUser, mailPassword);
    message.setFromAddress(mailSender);
    message.addAllToAddress(emailList);
    message.setMimeType(mimetype);
    message.setSubject(subject);
    message.setAuth(usesAuth);
    message.setTLS(tls);

    return message;
  }

  public EmailMessage prepareEmailMessage(String subject, String mimetype,
      Collection<String> emailList) {
    return createEmailMessage(subject, mimetype, emailList);
  }

  public String getAzkabanName() {
    return azkabanName;
  }

  public String getMailHost() {
    return mailHost;
  }

  public String getMailUser() {
    return mailUser;
  }

  public String getMailPassword() {
    return mailPassword;
  }

  public String getMailSender() {
    return mailSender;
  }

  /**
   * Attachment maximum size in bytes
   * 
   * @return
   */
  public long getAttachmentMaxSize() {
    return attachmentMazSizeInByte;
  }

  public boolean hasMailAuth() {
    return usesAuth;
  }
}
