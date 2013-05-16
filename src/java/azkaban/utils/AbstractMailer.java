package azkaban.utils;

import java.util.Collection;

public class AbstractMailer {
	private String clientHostname;
	private int clientPort;
	private boolean usesSSL;
	
	private String mailHost;
	private String mailUser;
	private String mailPassword;
	private String mailSender;
	private String azkabanName;
	
	private String referenceURL;
	
	public AbstractMailer(Props props) {
		this.azkabanName = props.getString("azkaban.name", "azkaban");
		this.mailHost = props.getString("mail.host", "localhost");
		this.mailUser = props.getString("mail.user", "");
		this.mailPassword = props.getString("mail.password", "");
		this.mailSender = props.getString("mail.sender", "");

		this.clientHostname = props.get("server.hostname");
		this.clientPort = props.getInt("server.port");
		this.usesSSL = props.getBoolean("server.useSSL");
		
		if (usesSSL) {
			referenceURL = "https://" + clientHostname + (clientPort==443 ? "/" : ":" + clientPort + "/");
		}
		else  {
			referenceURL = "http://" + clientHostname + (clientPort==80 ? "/" : ":" + clientPort + "/");
		}
	}
	
	public String getReferenceURL() {
		return referenceURL;
	}
	
	protected EmailMessage createEmailMessage(String subject, String mimetype, Collection<String> emailList) {
		EmailMessage message = new EmailMessage(mailHost, mailUser, mailPassword);
		message.setFromAddress(mailSender);
		message.addAllToAddress(emailList);
		message.setMimeType(mimetype);
		message.setSubject(subject);
		
		return message;
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
}
