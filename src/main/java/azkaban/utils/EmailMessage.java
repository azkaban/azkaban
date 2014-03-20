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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;

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

import com.sun.mail.smtp.SMTPTransport;

public class EmailMessage {
	private static String protocol = "smtp";
	private List<String> _toAddress = new ArrayList<String>();
	private String _mailHost;
	private String _mailUser;
	private String _mailPassword;
	private String _subject;
	private String _fromAddress;
	private String _mimeType = "text/plain";
	private StringBuffer _body = new StringBuffer();
	private static int _mailTimeout = 10000;
	private static int _connectionTimeout = 10000;
	
	private ArrayList<BodyPart> _attachments = new ArrayList<BodyPart>();

	public EmailMessage() {
		this("localhost", "", "");
	}

	public EmailMessage(String host, String user, String password) {
		_mailUser = user;
		_mailHost = host;
		_mailPassword = password;
	}
	
	public static void setTimeout(int timeoutMillis) {
		_mailTimeout = timeoutMillis;
	}
	
	public static void setConnectionTimeout(int timeoutMillis) {
		_connectionTimeout = timeoutMillis;
	}
	
	public EmailMessage setMailHost(String host) {
		_mailHost = host;
		return this;
	}

	public EmailMessage setMailUser(String user) {
		_mailUser = user;
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

	public EmailMessage addAttachment(File file) throws MessagingException {
		return addAttachment(file.getName(), file);
	}

	public EmailMessage addAttachment(String attachmentName, File file)
			throws MessagingException {
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

//		if (_mailUser == null) {
//			throw new RuntimeException("Mail user not set.");
//		}
//
//		if (_mailPassword == null) {
//			throw new RuntimeException("Mail password not set.");
//		}

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
//		props.setProperty("mail.transport.protocol", "smtp");
		props.put("mail."+protocol+".host", _mailHost);
		props.put("mail."+protocol+".auth", "true");
		props.put("mail.user", _mailUser);
		props.put("mail.password", _mailPassword);
		props.put("mail."+protocol+".timeout", _mailTimeout);
		props.put("mail."+protocol+".connectiontimeout", _connectionTimeout);

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
			MimeMultipart multipart = new MimeMultipart("related");
			
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

//		Transport transport = session.getTransport();
		
		SMTPTransport t = (SMTPTransport) session.getTransport(protocol);
		t.connect(_mailHost, _mailUser, _mailPassword);
		t.sendMessage(message,
				message.getRecipients(Message.RecipientType.TO));
		t.close();
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
}
