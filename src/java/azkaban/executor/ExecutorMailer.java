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

package azkaban.executor;

import java.util.ArrayList;
import java.util.List;

import javax.mail.MessagingException;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.utils.AbstractMailer;
import azkaban.utils.EmailMessage;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class ExecutorMailer extends AbstractMailer {
	private static Logger logger = Logger.getLogger(ExecutorMailer.class);
	
	private boolean testMode = false;
	
	public ExecutorMailer(Props props) {
		super(props);

		testMode = props.getBoolean("test.mode", false);
	}
	
	public void sendFirstErrorMessage(ExecutableFlow flow) {
		ExecutionOptions option = flow.getExecutionOptions();
		List<String> emailList = option.getDisabledJobs();
		int execId = flow.getExecutionId();
		
		if (emailList != null && !emailList.isEmpty()) {
			EmailMessage message = super.createEmailMessage(
					"Flow '" + flow.getFlowId() + "' has failed on " + getAzkabanName(), 
					"text/html", 
					emailList);
			
			message.println("<h2 style=\"color:#FF0000\"> Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' has encountered a failure on " + getAzkabanName() + "</h2>");
			
			if (option.getFailureAction() == FailureAction.CANCEL_ALL) {
				message.println("This flow is set to cancel all currently running jobs.");
			}
			else if (option.getFailureAction() == FailureAction.FINISH_ALL_POSSIBLE){
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
			String executionUrl = super.getReferenceURL() + "executor?" + "execid=" + execId;
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
		ExecutionOptions option = flow.getExecutionOptions();
		
		List<String> emailList = option.getFailureEmails();
		int execId = flow.getExecutionId();
		
		if (emailList != null && !emailList.isEmpty()) {
			EmailMessage message = super.createEmailMessage(
					"Flow '" + flow.getFlowId() + "' has failed on " + getAzkabanName(), 
					"text/html", 
					emailList);
			
			message.println("<h2 style=\"color:#FF0000\"> Execution '" + execId + "' of flow '" + flow.getFlowId() + "' has failed on " + getAzkabanName() + "</h2>");
			message.println("<table>");
			message.println("<tr><td>Start Time</td><td>" + flow.getStartTime() +"</td></tr>");
			message.println("<tr><td>End Time</td><td>" + flow.getEndTime() +"</td></tr>");
			message.println("<tr><td>Duration</td><td>" + Utils.formatDuration(flow.getStartTime(), flow.getEndTime()) +"</td></tr>");
			message.println("</table>");
			message.println("");
			
			String executionUrl = super.getReferenceURL() + "executor?" + "execid=" + execId;
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
		ExecutionOptions option = flow.getExecutionOptions();
		List<String> emailList = option.getSuccessEmails();

		int execId = flow.getExecutionId();
		
		if (emailList != null && !emailList.isEmpty()) {
			EmailMessage message = super.createEmailMessage(
					"Flow '" + flow.getFlowId() + "' has succeeded on " + getAzkabanName(), 
					"text/html", 
					emailList);
			
			message.println("<h2> Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' has succeeded on " + getAzkabanName() + "</h2>");
			message.println("<table>");
			message.println("<tr><td>Start Time</td><td>" + flow.getStartTime() +"</td></tr>");
			message.println("<tr><td>End Time</td><td>" + flow.getEndTime() +"</td></tr>");
			message.println("<tr><td>Duration</td><td>" + Utils.formatDuration(flow.getStartTime(), flow.getEndTime()) +"</td></tr>");
			message.println("</table>");
			message.println("");
			String executionUrl = super.getReferenceURL() + "executor?" + "execid=" + execId;
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
