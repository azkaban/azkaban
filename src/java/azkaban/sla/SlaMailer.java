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

package azkaban.sla;

import java.util.List;

import javax.mail.MessagingException;

import org.apache.log4j.Logger;

import azkaban.sla.SLA;
import azkaban.utils.AbstractMailer;
import azkaban.utils.EmailMessage;
import azkaban.utils.Props;

public class SlaMailer extends AbstractMailer {
	private static Logger logger = Logger.getLogger(SlaMailer.class);
	
	private boolean testMode = false;
	
	public SlaMailer(Props props) {
		super(props);

		testMode = props.getBoolean("test.mode", false);
	}
	
	public void sendSlaEmail(SLA s, String ... extraReasons) {
		List<String> emailList = s.getEmails();
		
		if (emailList != null && !emailList.isEmpty()) {
			EmailMessage message = super.createEmailMessage("SLA violation on " + getAzkabanName(), "text/html", emailList);

//			message.println("<h2 style=\"color:#FF0000\"> Execution '" + s.getExecId() + "' of flow '" + flow.getFlowId() + "' failed to meet SLA on " + azkabanName + "</h2>");
//			message.println("<table>");
//			message.println("<tr><td>Start Time</td><td>" + flow.getStartTime() +"</td></tr>");
//			message.println("<tr><td>End Time</td><td>" + flow.getEndTime() +"</td></tr>");
//			message.println("<tr><td>Duration</td><td>" + Utils.formatDuration(flow.getStartTime(), flow.getEndTime()) +"</td></tr>");
//			message.println("</table>");
//			message.println("");
//			String executionUrl = super.getReferenceURL() + "executor?" + "execid=" + execId;
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
