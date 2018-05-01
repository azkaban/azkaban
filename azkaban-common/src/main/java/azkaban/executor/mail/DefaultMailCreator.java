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

package azkaban.executor.mail;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.Status;
import azkaban.utils.EmailMessage;
import azkaban.utils.Utils;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class DefaultMailCreator implements MailCreator {

  public static final String DEFAULT_MAIL_CREATOR = "default";
  private static final DateFormat DATE_FORMATTER = new SimpleDateFormat(
      "yyyy/MM/dd HH:mm:ss z");
  private static final HashMap<String, MailCreator> registeredCreators = new HashMap<>();
  private static final MailCreator defaultCreator;

  static {
    defaultCreator = new DefaultMailCreator();
    registerCreator(DEFAULT_MAIL_CREATOR, defaultCreator);
  }

  public static void registerCreator(final String name, final MailCreator creator) {
    registeredCreators.put(name, creator);
  }

  public static MailCreator getCreator(final String name) {
    MailCreator creator = registeredCreators.get(name);
    if (creator == null) {
      creator = defaultCreator;
    }
    return creator;
  }

  private static String convertMSToString(final long timeInMS) {
    if (timeInMS < 0) {
      return "N/A";
    } else {
      return DATE_FORMATTER.format(new Date(timeInMS));
    }
  }

  private static List<String> findFailedJobs(final ExecutableFlow flow) {
    final ArrayList<String> failedJobs = new ArrayList<>();
    for (final ExecutableNode node : flow.getExecutableNodes()) {
      if (node.getStatus() == Status.FAILED) {
        failedJobs.add(node.getId());
      }
    }
    return failedJobs;
  }

  @Override
  public boolean createFirstErrorMessage(final ExecutableFlow flow,
      final EmailMessage message, final String azkabanName, final String scheme,
      final String clientHostname, final String clientPortNumber, final String... vars) {

    final ExecutionOptions option = flow.getExecutionOptions();
    final List<String> emailList = option.getFailureEmails();
    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow '" + flow.getFlowId() + "' has encountered a failure on "
          + azkabanName);

      message.println("<h2 style=\"color:#FF0000\"> Execution '"
          + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' of project '"
          + flow.getProjectName() + "' has encountered a failure on " + azkabanName + "</h2>");

      if (option.getFailureAction() == FailureAction.CANCEL_ALL) {
        message
            .println("This flow is set to cancel all currently running jobs.");
      } else if (option.getFailureAction() == FailureAction.FINISH_ALL_POSSIBLE) {
        message
            .println("This flow is set to complete all jobs that aren't blocked by the failure.");
      } else {
        message
            .println("This flow is set to complete all currently running jobs before stopping.");
      }

      message.println("<table>");
      message.println("<tr><td>Start Time</td><td>"
          + convertMSToString(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>End Time</td><td>"
          + convertMSToString(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>Duration</td><td>"
          + Utils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>Status</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
      final String executionUrl =
          scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
              + "executor?" + "execid=" + execId;
      message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId()
          + " Execution Link</a>");

      message.println("");
      message.println("<h3>Reason</h3>");
      final List<String> failedJobs = findFailedJobs(flow);
      message.println("<ul>");
      for (final String jobId : failedJobs) {
        message.println("<li><a href=\"" + executionUrl + "&job=" + jobId
            + "\">Failed job '" + jobId + "' Link</a></li>");
      }

      message.println("</ul>");
      return true;
    }

    return false;
  }

  @Override
  public boolean createErrorEmail(final ExecutableFlow flow, final EmailMessage message,
      final String azkabanName, final String scheme, final String clientHostname,
      final String clientPortNumber, final String... vars) {

    final ExecutionOptions option = flow.getExecutionOptions();

    final List<String> emailList = option.getFailureEmails();
    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow '" + flow.getFlowId() + "' has failed on "
          + azkabanName);

      message.println("<h2 style=\"color:#FF0000\"> Execution '" + execId
          + "' of flow '" + flow.getFlowId() + "' of project '"
          + flow.getProjectName() + "' has failed on " + azkabanName + "</h2>");
      message.println("<table>");
      message.println("<tr><td>Start Time</td><td>"
          + convertMSToString(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>End Time</td><td>"
          + convertMSToString(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>Duration</td><td>"
          + Utils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>Status</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
      final String executionUrl =
          scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
              + "executor?" + "execid=" + execId;
      message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId()
          + " Execution Link</a>");

      message.println("");
      message.println("<h3>Reason</h3>");
      final List<String> failedJobs = findFailedJobs(flow);
      message.println("<ul>");
      for (final String jobId : failedJobs) {
        message.println("<li><a href=\"" + executionUrl + "&job=" + jobId
            + "\">Failed job '" + jobId + "' Link</a></li>");
      }
      for (final String reasons : vars) {
        message.println("<li>" + reasons + "</li>");
      }

      message.println("</ul>");
      return true;
    }
    return false;
  }

  @Override
  public boolean createSuccessEmail(final ExecutableFlow flow, final EmailMessage message,
      final String azkabanName, final String scheme, final String clientHostname,
      final String clientPortNumber, final String... vars) {

    final ExecutionOptions option = flow.getExecutionOptions();
    final List<String> emailList = option.getSuccessEmails();

    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow '" + flow.getFlowId() + "' has succeeded on "
          + azkabanName);

      message.println("<h2> Execution '" + flow.getExecutionId()
          + "' of flow '" + flow.getFlowId() + "' of project '"
          + flow.getProjectName() + "' has succeeded on " + azkabanName + "</h2>");
      message.println("<table>");
      message.println("<tr><td>Start Time</td><td>"
          + convertMSToString(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>End Time</td><td>"
          + convertMSToString(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>Duration</td><td>"
          + Utils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>Status</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
      final String executionUrl =
          scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
              + "executor?" + "execid=" + execId;
      message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId()
          + " Execution Link</a>");
      return true;
    }
    return false;
  }
}
