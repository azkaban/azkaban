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

import com.sun.mail.smtp.SMTPTransport;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.apache.log4j.Logger;

public class EmailMessage {

  private static final String protocol = "smtp";
  private static final int MAX_EMAIL_RETRY_COUNT = 5;
  private static int _mailTimeout = 10000;
  private static int _connectionTimeout = 10000;
  private final Logger logger = Logger.getLogger(EmailMessage.class);
  private final List<String> _toAddress = new ArrayList<>();
  private final int _mailPort;
  private final String _mailHost;
  private final String _mailUser;
  private final String _mailPassword;
  private String _subject;
  private String _fromAddress;
  private String _mimeType = "text/plain";
  private String _tls;
  private boolean _usesAuth = true;
  private StringBuffer _body = new StringBuffer();

  public EmailMessage() {
    this("localhost", AbstractMailer.DEFAULT_SMTP_PORT, "", "");
  }

  public EmailMessage(final String host, final int port, final String user, final String password) {
    this._mailUser = user;
    this._mailHost = host;
    this._mailPort = port;
    this._mailPassword = password;
  }

  public static void setTimeout(final int timeoutMillis) {
    _mailTimeout = timeoutMillis;
  }

  public static void setConnectionTimeout(final int timeoutMillis) {
    _connectionTimeout = timeoutMillis;
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
      props.put("mail." + protocol + ".auth", "true");
      props.put("mail.user", this._mailUser);
      props.put("mail.password", this._mailPassword);
    } else {
      props.put("mail." + protocol + ".auth", "false");
    }
    props.put("mail." + protocol + ".host", this._mailHost);
    props.put("mail." + protocol + ".port", this._mailPort);
    props.put("mail." + protocol + ".timeout", _mailTimeout);
    props.put("mail." + protocol + ".connectiontimeout", _connectionTimeout);
    props.put("mail.smtp.starttls.enable", this._tls);
    props.put("mail.smtp.ssl.trust", this._mailHost);

    final Session session = Session.getInstance(props, null);
    final Message message = new MimeMessage(session);
    final InternetAddress from = new InternetAddress(this._fromAddress, false);
    message.setFrom(from);
    for (final String toAddr : this._toAddress) {
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(
          toAddr, false));
    }
    message.setSubject(this._subject);
    message.setSentDate(new Date());

    message.setContent(this._body.toString(), this._mimeType);

    final SMTPTransport t = (SMTPTransport) session.getTransport(protocol);

    retryConnectToSMTPServer(t);
    retrySendMessage(t, message);
    t.close();
  }

  private void connectToSMTPServer(final SMTPTransport t) throws MessagingException {
    if (this._usesAuth) {
      t.connect(this._mailHost, this._mailPort, this._mailUser, this._mailPassword);
    } else {
      t.connect();
    }
  }

  private void retryConnectToSMTPServer(final SMTPTransport t) throws MessagingException {
    int attempt;
    for (attempt = 0; attempt < MAX_EMAIL_RETRY_COUNT; attempt++) {
      try {
        connectToSMTPServer(t);
        return;
      } catch (final Exception e) {
        this.logger.error("Connecting to SMTP server failed, attempt: " + attempt, e);
      }
    }
    t.close();
    throw new MessagingException("Failed to connect to SMTP server after "
        + attempt + " attempts.");
  }

  private void retrySendMessage(final SMTPTransport t, final Message message)
      throws MessagingException {
    int attempt;
    for (attempt = 0; attempt < MAX_EMAIL_RETRY_COUNT; attempt++) {
      try {
        t.sendMessage(message, message.getRecipients(Message.RecipientType.TO));
        return;
      } catch (final Exception e) {
        this.logger.error("Sending email messages failed, attempt: " + attempt, e);
      }
    }
    t.close();
    throw new MessagingException("Failed to send email messages after "
        + attempt + " attempts.");
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
