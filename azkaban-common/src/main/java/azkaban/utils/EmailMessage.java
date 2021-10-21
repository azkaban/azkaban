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

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import org.apache.log4j.Logger;

public class EmailMessage {

  private static final int MAX_EMAIL_RETRY_COUNT = 5;
  private static int _mailTimeout = 10000;
  private static int _connectionTimeout = 10000;
  private static long _totalAttachmentMaxSizeInByte = 1024 * 1024 * 1024; // 1
  private final Logger logger = Logger.getLogger(EmailMessage.class);
  private final List<String> _toAddress = new ArrayList<>();
  private final int _mailPort;
  private final ArrayList<BodyPart> _attachments = new ArrayList<>();
  private final String _mailHost;
  private final String _mailUser;
  private final String _mailPassword;
  private final EmailMessageCreator creator;
  private String _subject;
  private String _fromAddress;
  private String _mimeType = "text/plain";
  private String _tls;
  private long _totalAttachmentSizeSoFar;
  private boolean _usesAuth = true;
  private boolean _enableAttachementEmbedment = true;
  private StringBuffer _body = new StringBuffer();

  public EmailMessage(final String host, final int port, final String user, final String password,
      final EmailMessageCreator creator) {
    this._mailUser = user;
    this._mailHost = host;
    this._mailPort = port;
    this._mailPassword = password;
    this.creator = creator;
  }

  public static void setTimeout(final int timeoutMillis) {
    _mailTimeout = timeoutMillis;
  }

  public static void setConnectionTimeout(final int timeoutMillis) {
    _connectionTimeout = timeoutMillis;
  }

  public static void setTotalAttachmentMaxSize(final long sizeInBytes) {
    if (sizeInBytes < 1) {
      throw new IllegalArgumentException(
          "attachment max size can't be 0 or negative");
    }
    _totalAttachmentMaxSizeInByte = sizeInBytes;
  }

  public EmailMessage enableAttachementEmbedment(final boolean toEnable) {
    this._enableAttachementEmbedment = toEnable;
    return this;
  }

  public EmailMessage addAllToAddress(final Collection<? extends String> addresses) {
    this._toAddress.addAll(addresses);
    return this;
  }

  public EmailMessage addToAddress(final String address) {
    this._toAddress.add(address);
    return this;
  }

  public EmailMessage setFromAddress(final String fromAddress) {
    this._fromAddress = fromAddress;
    return this;
  }

  public EmailMessage setTLS(final String tls) {
    this._tls = tls;
    return this;
  }

  public EmailMessage setAuth(final boolean auth) {
    this._usesAuth = auth;
    return this;
  }

  public EmailMessage addAttachment(final File file) throws MessagingException {
    return addAttachment(file.getName(), file);
  }

  public EmailMessage addAttachment(final String attachmentName, final File file)
      throws MessagingException {

    this._totalAttachmentSizeSoFar += file.length();

    if (this._totalAttachmentSizeSoFar > _totalAttachmentMaxSizeInByte) {
      throw new MessageAttachmentExceededMaximumSizeException(
          "Adding attachment '" + attachmentName
              + "' will exceed the allowed maximum size of "
              + _totalAttachmentMaxSizeInByte);
    }

    final BodyPart attachmentPart = new MimeBodyPart();
    final DataSource fileDataSource = new FileDataSource(file);
    attachmentPart.setDataHandler(new DataHandler(fileDataSource));
    attachmentPart.setFileName(attachmentName);
    this._attachments.add(attachmentPart);
    return this;
  }

  public EmailMessage addAttachment(final String attachmentName, final InputStream stream)
      throws MessagingException {
    final BodyPart attachmentPart = new MimeBodyPart(stream);
    attachmentPart.setFileName(attachmentName);
    this._attachments.add(attachmentPart);
    return this;
  }

  private void checkSettings() {
    if (this._mailHost == null) {
      throw new RuntimeException("Mail host not set.");
    }

    if (this._fromAddress == null || this._fromAddress.length() == 0) {
      throw new RuntimeException("From address not set.");
    }

    if (this._subject == null) {
      throw new RuntimeException("Subject cannot be null");
    }

    if (this._toAddress.size() == 0) {
      throw new RuntimeException("T");
    }
  }

  public void sendEmail() throws MessagingException {
    checkSettings();
    final Properties props = new Properties();
    if (this._usesAuth) {
      props.put("mail.smtp.auth", "true");
      props.put("mail.user", this._mailUser);
      props.put("mail.password", this._mailPassword);
    } else {
      props.put("mail.smtp.auth", "false");
    }
    props.put("mail.smtp.host", this._mailHost);
    props.put("mail.smtp.port", this._mailPort);
    props.put("mail.smtp.timeout", _mailTimeout);
    props.put("mail.smtp.connectiontimeout", _connectionTimeout);
    props.put("mail.smtp.starttls.enable", this._tls);
    props.put("mail.smtp.ssl.trust", this._mailHost);

    final JavaxMailSender sender = this.creator.createSender(props);
    final Message message = sender.createMessage();

    final InternetAddress from = new InternetAddress(this._fromAddress, false);
    message.setFrom(from);
    for (final String toAddr : this._toAddress) {
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(
          toAddr, false));
    }
    message.setSubject(this._subject);
    message.setSentDate(new Date());

    if (this._attachments.size() > 0) {
      final MimeMultipart multipart =
          this._enableAttachementEmbedment ? new MimeMultipart("related")
              : new MimeMultipart();

      final BodyPart messageBodyPart = new MimeBodyPart();
      messageBodyPart.setContent(this._body.toString(), this._mimeType);
      multipart.addBodyPart(messageBodyPart);

      // Add attachments
      for (final BodyPart part : this._attachments) {
        multipart.addBodyPart(part);
      }

      message.setContent(multipart);
    } else {
      message.setContent(this._body.toString(), this._mimeType);
    }

    retryConnectToSMTPServer(sender);
    retrySendMessage(sender, message);
    sender.close();
  }

  private void connectToSMTPServer(final JavaxMailSender s) throws MessagingException {
    if (this._usesAuth) {
      s.connect(this._mailHost, this._mailPort, this._mailUser, this._mailPassword);
    } else {
      s.connect();
    }
  }

  private void retryConnectToSMTPServer(final JavaxMailSender s) throws MessagingException {
    int attempt;
    for (attempt = 0; attempt < MAX_EMAIL_RETRY_COUNT; attempt++) {
      try {
        connectToSMTPServer(s);
        return;
      } catch (final Exception e) {
        this.logger.error("Connecting to SMTP server failed, attempt: " + attempt, e);
      }
    }
    s.close();
    throw new MessagingException("Failed to connect to SMTP server after "
        + attempt + " attempts.");
  }

  private void retrySendMessage(final JavaxMailSender s, final Message message)
      throws MessagingException {
    int attempt;
    for (attempt = 0; attempt < MAX_EMAIL_RETRY_COUNT; attempt++) {
      try {
        s.sendMessage(message, message.getRecipients(Message.RecipientType.TO));
        return;
      } catch (final Exception e) {
        this.logger.error("Sending email message failed, attempt: " + attempt
            + ", message: " + messageToString(message), e);
      }
    }
    s.close();
    throw new MessagingException("Failed to send email messages after "
        + attempt + " attempts.");
  }

  private static String messageToString(Message message) throws MessagingException {
    return "[recipients: " + Arrays.toString(message.getRecipients(RecipientType.TO))
        + ", subject: " + message.getSubject() + "]";
  }

  public void setBody(final String body, final String mimeType) {
    this._body = new StringBuffer(body);
    this._mimeType = mimeType;
  }

  public EmailMessage setMimeType(final String mimeType) {
    this._mimeType = mimeType;
    return this;
  }

  public EmailMessage println(final Object str) {
    this._body.append(str);

    return this;
  }

  public String getBody() {
    return this._body.toString();
  }

  public void setBody(final String body) {
    setBody(body, this._mimeType);
  }

  public String getSubject() {
    return this._subject;
  }

  public EmailMessage setSubject(final String subject) {
    this._subject = subject;
    return this;
  }

  public int getMailPort() {
    return this._mailPort;
  }

}
