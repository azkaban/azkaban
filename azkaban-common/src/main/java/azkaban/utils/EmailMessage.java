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
import org.apache.log4j.Logger;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.File;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.*;

public class EmailMessage {
  private final Logger logger = Logger.getLogger(EmailMessage.class);

  private static String protocol = "smtp";
  private List<String> _toAddress = new ArrayList<String>();
  private String _mailHost;
  private int _mailPort;
  private String _mailUser;
  private String _mailPassword;
  private String _subject;
  private String _fromAddress;
  private String _mimeType = "text/plain";
  private String _tls;
  private long _totalAttachmentSizeSoFar;
  private boolean _usesAuth = true;
  private boolean _enableAttachementEmbedment = true;
  private StringBuffer _body = new StringBuffer();
  private static int _mailTimeout = 10000;
  private static int _connectionTimeout = 10000;
  private static long _totalAttachmentMaxSizeInByte = 1024 * 1024 * 1024; // 1
                                                                          // GB

  private ArrayList<BodyPart> _attachments = new ArrayList<BodyPart>();

  public EmailMessage() {
    this("localhost", AbstractMailer.DEFAULT_SMTP_PORT, "", "");
  }

  public EmailMessage(String host, int port, String user, String password) {
    _mailUser = user;
    _mailHost = host;
    _mailPort = port;
    _mailPassword = password;
  }

  public static void setTimeout(int timeoutMillis) {
    _mailTimeout = timeoutMillis;
  }

  public static void setConnectionTimeout(int timeoutMillis) {
    _connectionTimeout = timeoutMillis;
  }

  public static void setTotalAttachmentMaxSize(long sizeInBytes) {
    if (sizeInBytes < 1) {
      throw new IllegalArgumentException(
          "attachment max size can't be 0 or negative");
    }
    _totalAttachmentMaxSizeInByte = sizeInBytes;
  }

  public EmailMessage setMailHost(String host) {
    _mailHost = host;
    return this;
  }

  public EmailMessage setMailUser(String user) {
    _mailUser = user;
    return this;
  }

  public EmailMessage enableAttachementEmbedment(boolean toEnable) {
    _enableAttachementEmbedment = toEnable;
    return this;
  }

  public EmailMessage setMailPassword(String password) {
    _mailPassword = password;
    return this;
  }

  public EmailMessage addAllToAddress(Collection<? extends String> addresses) {
    _toAddress.addAll(addresses);
    return this;
  }

  public EmailMessage addToAddress(String address) {
    _toAddress.add(address);
    return this;
  }

  public EmailMessage setSubject(String subject) {
    _subject = subject;
    return this;
  }

  public EmailMessage setFromAddress(String fromAddress) {
    _fromAddress = fromAddress;
    return this;
  }

  public EmailMessage setTLS(String tls) {
    _tls = tls;
    return this;
  }

  public EmailMessage setAuth(boolean auth) {
    _usesAuth = auth;
    return this;
  }

  public EmailMessage addAttachment(File file) throws MessagingException {
    return addAttachment(file.getName(), file);
  }

  public EmailMessage addAttachment(String attachmentName, File file)
      throws MessagingException {

    _totalAttachmentSizeSoFar += file.length();

    if (_totalAttachmentSizeSoFar > _totalAttachmentMaxSizeInByte) {
      throw new MessageAttachmentExceededMaximumSizeException(
          "Adding attachment '" + attachmentName
              + "' will exceed the allowed maximum size of "
              + _totalAttachmentMaxSizeInByte);
    }

    BodyPart attachmentPart = new MimeBodyPart();
    DataSource fileDataSource = new FileDataSource(file);
    attachmentPart.setDataHandler(new DataHandler(fileDataSource));
    attachmentPart.setFileName(attachmentName);
    _attachments.add(attachmentPart);
    return this;
  }

  public EmailMessage addAttachment(String attachmentName, InputStream stream)
      throws MessagingException {
    BodyPart attachmentPart = new MimeBodyPart(stream);
    attachmentPart.setFileName(attachmentName);
    _attachments.add(attachmentPart);
    return this;
  }

  private void checkSettings() {
    if (_mailHost == null) {
      throw new RuntimeException("Mail host not set.");
    }

    if (_fromAddress == null || _fromAddress.length() == 0) {
      throw new RuntimeException("From address not set.");
    }

    if (_subject == null) {
      throw new RuntimeException("Subject cannot be null");
    }

    if (_toAddress.size() == 0) {
      throw new RuntimeException("T");
    }
  }

  public void sendEmail() throws MessagingException {
    checkSettings();
    Properties props = new Properties();
    if (_usesAuth) {
      props.put("mail." + protocol + ".auth", "true");
      props.put("mail.user", _mailUser);
      props.put("mail.password", _mailPassword);
    } else {
      props.put("mail." + protocol + ".auth", "false");
    }
    props.put("mail." + protocol + ".host", _mailHost);
    props.put("mail." + protocol + ".port", _mailPort);
    props.put("mail." + protocol + ".timeout", _mailTimeout);
    props.put("mail." + protocol + ".connectiontimeout", _connectionTimeout);
    props.put("mail.smtp.starttls.enable", _tls);
    props.put("mail.smtp.ssl.trust", _mailHost);

    Session session = Session.getInstance(props, null);
    Message message = new MimeMessage(session);
    InternetAddress from = new InternetAddress(_fromAddress, false);
    message.setFrom(from);
    for (String toAddr : _toAddress)
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(
          toAddr, false));
    message.setSubject(_subject);
    message.setSentDate(new Date());

    if (_attachments.size() > 0) {
      MimeMultipart multipart =
          this._enableAttachementEmbedment ? new MimeMultipart("related")
              : new MimeMultipart();

      BodyPart messageBodyPart = new MimeBodyPart();
      messageBodyPart.setContent(_body.toString(), _mimeType);
      multipart.addBodyPart(messageBodyPart);

      // Add attachments
      for (BodyPart part : _attachments) {
        multipart.addBodyPart(part);
      }

      message.setContent(multipart);
    } else {
      message.setContent(_body.toString(), _mimeType);
    }

    // Transport transport = session.getTransport();

    SMTPTransport t = (SMTPTransport) session.getTransport(protocol);

    try {
      connectToSMTPServer(t);
    } catch (MessagingException ste) {
      if (ste.getCause() instanceof SocketTimeoutException) {
        try {
          // retry on SocketTimeoutException
          connectToSMTPServer(t);
          logger.info("Email retry on SocketTimeoutException succeeded");
        } catch (MessagingException me) {
          logger.error("Email retry on SocketTimeoutException failed", me);
          throw me;
        }
      } else {
        logger.error("Encountered issue while connecting to email server", ste);
        throw ste;
      }
    }
    t.sendMessage(message, message.getRecipients(Message.RecipientType.TO));
    t.close();
  }

  private void connectToSMTPServer(SMTPTransport t) throws MessagingException {
    if (_usesAuth) {
      t.connect(_mailHost, _mailPort, _mailUser, _mailPassword);
    } else {
      t.connect();
    }
  }

  public void setBody(String body) {
    setBody(body, _mimeType);
  }

  public void setBody(String body, String mimeType) {
    _body = new StringBuffer(body);
    _mimeType = mimeType;
  }

  public EmailMessage setMimeType(String mimeType) {
    _mimeType = mimeType;
    return this;
  }

  public EmailMessage println(Object str) {
    _body.append(str);

    return this;
  }

  public String getBody() {
    return _body.toString();
  }

  public String getSubject() {
    return _subject;
  }

  public int getMailPort(){
    return _mailPort;
  }

}
