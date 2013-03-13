package azkaban.sla;

import java.util.List;

import javax.mail.MessagingException;

import org.apache.log4j.Logger;

import azkaban.sla.SLA;
import azkaban.utils.EmailMessage;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class SlaMailer {
	private static Logger logger = Logger.getLogger(SlaMailer.class);
	
	private boolean testMode = false;
	private String clientHostname;
	private String clientPortNumber;
	
	private String mailHost;
	private String mailUser;
	private String mailPassword;
	private String mailSender;
	private String azkabanName;
	
	public SlaMailer(Props props) {
		this.azkabanName = props.getString("azkaban.name", "azkaban");
		this.mailHost = props.getString("mail.host", "localhost");
		this.mailUser = props.getString("mail.user", "");
		this.mailPassword = props.getString("mail.password", "");
		this.mailSender = props.getString("mail.sender", "");
		
		this.clientHostname = props.getString("jetty.hostname", "localhost");
		this.clientPortNumber = Utils.nonNull(props.getString("jetty.ssl.port"));
		
		testMode = props.getBoolean("test.mode", false);
	}
	
	public void sendSlaEmail(SLA s, String ... extraReasons) {
		List<String> emailList = s.getEmails();
		
		if (emailList != null && !emailList.isEmpty()) {
			EmailMessage message = new EmailMessage(mailHost, mailUser, mailPassword);
			message.setFromAddress(mailSender);
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("SLA violation on " + azkabanName);
			
//			message.println("<h2 style=\"color:#FF0000\"> Execution '" + s.getExecId() + "' of flow '" + flow.getFlowId() + "' failed to meet SLA on " + azkabanName + "</h2>");
//			message.println("<table>");
//			message.println("<tr><td>Start Time</td><td>" + flow.getStartTime() +"</td></tr>");
//			message.println("<tr><td>End Time</td><td>" + flow.getEndTime() +"</td></tr>");
//			message.println("<tr><td>Duration</td><td>" + Utils.formatDuration(flow.getStartTime(), flow.getEndTime()) +"</td></tr>");
//			message.println("</table>");
//			message.println("");
//			String executionUrl = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid=" + execId;
//			message.println("<a href='\"" + executionUrl + "\">" + flow.getFlowId() + " Execution Link</a>");
//			
//			message.println("");
//			message.println("<h3>Reason</h3>");
//			List<String> failedJobs = findFailedJobs(flow);
//			message.println("<ul>");
//			for (String jobId : failedJobs) {
//				message.println("<li><a href=\"" + executionUrl + "&job=" + jobId + "\">Failed job '" + jobId + "' Link</a></li>" );
//			}
			for (String reasons: extraReasons) {
				message.println("<li>" + reasons + "</li>");
			}
			
			message.println("</ul>");
			
			if (!testMode) {
				try {
					message.sendEmail();
				} catch (MessagingException e) {
					logger.error("Email message send failed" , e);
				}
			}
		}
	}
}