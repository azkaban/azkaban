package azkaban.executor;

import java.util.ArrayList;
import java.util.List;

import javax.mail.MessagingException;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow.FailureAction;
import azkaban.utils.EmailMessage;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class ExecutorMailer {
	private static Logger logger = Logger.getLogger(ExecutorMailer.class);
	
	private boolean testMode = false;
	private String clientHostname;
	private String clientPortNumber;
	
	private String mailHost;
	private String mailUser;
	private String mailPassword;
	private String mailSender;
	private String azkabanName;
	
	public ExecutorMailer(Props props) {
		this.azkabanName = props.getString("azkaban.name", "azkaban");
		this.mailHost = props.getString("mail.host", "localhost");
		this.mailUser = props.getString("mail.user", "");
		this.mailPassword = props.getString("mail.password", "");
		this.mailSender = props.getString("mail.sender", "");
		
		this.clientHostname = props.getString("jetty.hostname", "localhost");
		this.clientPortNumber = Utils.nonNull(props.getString("jetty.ssl.port"));
		
		testMode = props.getBoolean("test.mode", false);
	}
	
	public void sendFirstErrorMessage(ExecutableFlow flow) {
		List<String> emailList = flow.getFailureEmails();
		int execId = flow.getExecutionId();
		
		if (emailList != null && !emailList.isEmpty()) {
			EmailMessage message = new EmailMessage(mailHost, mailUser, mailPassword);
			message.setFromAddress(mailSender);
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("Flow '" + flow.getFlowId() + "' has failed on " + azkabanName);
			
			message.println("<h2 style=\"color:#FF0000\"> Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' has encountered a failure on " + azkabanName + "</h2>");
			
			if (flow.getFailureAction() == FailureAction.CANCEL_ALL) {
				message.println("This flow is set to cancel all currently running jobs.");
			}
			else if (flow.getFailureAction() == FailureAction.FINISH_ALL_POSSIBLE){
				message.println("This flow is set to complete all jobs that aren't blocked by the failure.");
			}
			else {
				message.println("This flow is set to complete all currently running jobs before stopping.");
			}
			
			message.println("<table>");
			message.println("<tr><td>Start Time</td><td>" + flow.getStartTime() +"</td></tr>");
			message.println("<tr><td>End Time</td><td>" + flow.getEndTime() +"</td></tr>");
			message.println("<tr><td>Duration</td><td>" + Utils.formatDuration(flow.getStartTime(), flow.getEndTime()) +"</td></tr>");
			message.println("</table>");
			message.println("");
			String executionUrl = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid=" + execId;
			message.println("<a href='\"" + executionUrl + "\">" + flow.getFlowId() + " Execution Link</a>");
			
			message.println("");
			message.println("<h3>Reason</h3>");
			List<String> failedJobs = findFailedJobs(flow);
			message.println("<ul>");
			for (String jobId : failedJobs) {
				message.println("<li><a href=\"" + executionUrl + "&job=" + jobId + "\">Failed job '" + jobId + "' Link</a></li>" );
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
	
	public void sendErrorEmail(ExecutableFlow flow, String ... extraReasons) {
		List<String> emailList = flow.getFailureEmails();
		int execId = flow.getExecutionId();
		
		if (emailList != null && !emailList.isEmpty()) {
			EmailMessage message = new EmailMessage(mailHost, mailUser, mailPassword);
			message.setFromAddress(mailSender);
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("Flow '" + flow.getFlowId() + "' has failed on " + azkabanName);
			
			message.println("<h2 style=\"color:#FF0000\"> Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' has failed on " + azkabanName + "</h2>");
			message.println("<table>");
			message.println("<tr><td>Start Time</td><td>" + flow.getStartTime() +"</td></tr>");
			message.println("<tr><td>End Time</td><td>" + flow.getEndTime() +"</td></tr>");
			message.println("<tr><td>Duration</td><td>" + Utils.formatDuration(flow.getStartTime(), flow.getEndTime()) +"</td></tr>");
			message.println("</table>");
			message.println("");
			String executionUrl = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid=" + execId;
			message.println("<a href='\"" + executionUrl + "\">" + flow.getFlowId() + " Execution Link</a>");
			
			message.println("");
			message.println("<h3>Reason</h3>");
			List<String> failedJobs = findFailedJobs(flow);
			message.println("<ul>");
			for (String jobId : failedJobs) {
				message.println("<li><a href=\"" + executionUrl + "&job=" + jobId + "\">Failed job '" + jobId + "' Link</a></li>" );
			}
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

	public void sendSuccessEmail(ExecutableFlow flow) {
		List<String> emailList = flow.getSuccessEmails();

		int execId = flow.getExecutionId();
		
		if (emailList != null && !emailList.isEmpty()) {
			EmailMessage message = new EmailMessage(mailHost, mailUser, mailPassword);
			message.setFromAddress(mailSender);
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("Flow '" + flow.getFlowId() + "' has succeeded on " + azkabanName);
			
			message.println("<h2> Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' has succeeded on " + azkabanName + "</h2>");
			message.println("<table>");
			message.println("<tr><td>Start Time</td><td>" + flow.getStartTime() +"</td></tr>");
			message.println("<tr><td>End Time</td><td>" + flow.getEndTime() +"</td></tr>");
			message.println("<tr><td>Duration</td><td>" + Utils.formatDuration(flow.getStartTime(), flow.getEndTime()) +"</td></tr>");
			message.println("</table>");
			message.println("");
			String executionUrl = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid=" + execId;
			message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId() + " Execution Link</a>");
			
			if (!testMode) {
				try {
					message.sendEmail();
				} catch (MessagingException e) {
					logger.error("Email message send failed" , e);
				}
			}
		}
	}
	
	private List<String> findFailedJobs(ExecutableFlow flow) {
		ArrayList<String> failedJobs = new ArrayList<String>();
		for (ExecutableNode node: flow.getExecutableNodes()) {
			if (node.getStatus() == Status.FAILED) {
				failedJobs.add(node.getJobId());
			}
		}
		
		return failedJobs;
	}
}