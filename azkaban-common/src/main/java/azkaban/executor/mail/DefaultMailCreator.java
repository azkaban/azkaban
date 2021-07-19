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
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.utils.EmailMessage;
import azkaban.utils.TimeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang.exception.ExceptionUtils;

public class DefaultMailCreator implements MailCreator {

  public static final String DEFAULT_MAIL_CREATOR = "default";
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
      final String clientHostname, final String clientPortNumber) {

    final ExecutionOptions option = flow.getExecutionOptions();
    final List<String> emailList = option.getFailureEmails();
    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow '" + flow.getFlowId() + "' has encountered a failure on "
          + azkabanName);

      message.println("\n\r <html><head><meta http-equiv='content-type' content='text/html; charset=utf-8' /></head><body>");
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
          + TimeUtils.formatDateTimeZone(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>End Time</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>Duration</td><td>"
          + TimeUtils.formatDuration(flow.getStartTime(), flow.getEndTime())
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
      message.println("</body></html>");
      return true;
    }

    return false;
  }

  @Override
  public boolean createErrorEmail(final ExecutableFlow flow, final List<ExecutableFlow>
      pastExecutions, final EmailMessage message, final String azkabanName, final String scheme,
      final String clientHostname, final String clientPortNumber, final String... reasons) {

    final ExecutionOptions option = flow.getExecutionOptions();

    final List<String> emailList = option.getFailureEmails();
    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow '" + flow.getFlowId() + "' has failed on "
          + azkabanName);

      message.println("\n\r <html><head><meta http-equiv='content-type' content='text/html; charset=utf-8' /></head><body>");
      message.println("<h2 style=\"color:#FF0000\"> Execution '" + execId
          + "' of flow '" + flow.getFlowId() + "' of project '"
          + flow.getProjectName() + "' has failed on " + azkabanName + "</h2>");
      message.println("<table>");
      message.println("<tr><td>Start Time</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>End Time</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>Duration</td><td>"
          + TimeUtils.formatDuration(flow.getStartTime(), flow.getEndTime())
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
      for (final String reason : reasons) {
        message.println("<li>" + reason + "</li>");
      }

      message.println("</ul>");

      message.println("");

      int failedCount = 0;
      for (final ExecutableFlow executableFlow : pastExecutions) {
        if (executableFlow.getStatus().equals(Status.FAILED)) {
          failedCount++;
        }
      }

      message.println(String.format("<h3>Executions from past 72 hours (%s out %s) failed</h3>",
          failedCount, pastExecutions.size()));
      for (final ExecutableFlow executableFlow : pastExecutions) {
        message.println("<table>");
        message.println(
            "<tr><td>Execution Id</td><td>" + (executableFlow.getExecutionId()) + "</td></tr>");
        message.println("<tr><td>Start Time</td><td>"
            + TimeUtils.formatDateTimeZone(executableFlow.getStartTime()) + "</td></tr>");
        message.println("<tr><td>End Time</td><td>"
            + TimeUtils.formatDateTimeZone(executableFlow.getEndTime()) + "</td></tr>");
        message.println("<tr><td>Status</td><td>" + executableFlow.getStatus() + "</td></tr>");
        message.println("</table>");
      }

      message.println("</body></html>");
      return true;
    }
    return false;
  }

  @Override
  public boolean createSuccessEmail(final ExecutableFlow flow, final EmailMessage message,
      final String azkabanName, final String scheme, final String clientHostname,
      final String clientPortNumber) {

    final ExecutionOptions option = flow.getExecutionOptions();
    final List<String> emailList = option.getSuccessEmails();

    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow '" + flow.getFlowId() + "' has succeeded on "
          + azkabanName);

      message.println("\n\r <html><head><meta http-equiv='content-type' content='text/html; charset=utf-8' /></head><body>");
      message.println("<h2> Execution '" + flow.getExecutionId()
          + "' of flow '" + flow.getFlowId() + "' of project '"
          + flow.getProjectName() + "' has succeeded on " + azkabanName + "</h2>");
      message.println("<table>");
      message.println("<tr><td>Start Time</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>End Time</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>Duration</td><td>"
          + TimeUtils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>Status</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
      final String executionUrl =
          scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
              + "executor?" + "execid=" + execId;
      message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId()
          + " Execution Link</a>");
      message.println("</body></html>");
      return true;
    }
    return false;
  }

  @Override
  public boolean createFailedUpdateMessage(final List<ExecutableFlow> flows,
      final Executor executor, final ExecutorManagerException updateException,
      final EmailMessage message, final String azkabanName,
      final String scheme, final String clientHostname, final String clientPortNumber) {

    final ExecutionOptions option = flows.get(0).getExecutionOptions();
    final List<String> emailList = option.getFailureEmails();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject(
          "Flow status could not be updated from " + executor.getHost() + " on " + azkabanName);

      message.println("\n\r <html><head><meta http-equiv='content-type' content='text/html; charset=utf-8' /></head><body>");
      message.println(
          "<h2 style=\"color:#FF0000\"> Flow status could not be updated from " + executor.getHost()
              + " on " + azkabanName + "</h2>");

      message.println("The actual status of these executions is unknown, "
          + "because getting status update from azkaban executor is failing");

      message.println("");
      message.println("<h3>Error detail</h3>");
      message.println("<pre>" + ExceptionUtils.getStackTrace(updateException) + "</pre>");

      message.println("");
      message.println("<h3>Affected executions</h3>");
      message.println("<ul>");
      appendFlowLinksToMessage(message, flows, scheme, clientHostname, clientPortNumber);
      message.println("</ul>");
      message.println("</body></html>");
      return true;
    }

    return false;
  }

  @Override
  public boolean createFailedExecutorHealthCheckMessage(final List<ExecutableFlow> flows,
      final Executor executor, final ExecutorManagerException failureException,
      final EmailMessage message, final String azkabanName,
      final String scheme, final String clientHostname, final String clientPortNumber,
      final List<String> emailList) {

    if (emailList == null || emailList.isEmpty()) {
      return false;
    }
    message.addAllToAddress(emailList);
    message.setMimeType("text/html");
    message.setSubject(
        "Alert: Executor is unreachable, " + executor.getHost() + " on " + azkabanName);

    message.println("\n\r <html><head><meta http-equiv='content-type' content='text/html; charset=utf-8' /></head><body>");
    message.println(
        "<h2 style=\"color:#FFA500\"> Executor is unreachable. Executor host - " + executor
            .getHost() + " on Cluster - " + azkabanName + "</h2>");

    message.println("Remedial action will be attempted on affected executions - <br>");
    message.println("Following flows were reported as running on the executor and will be "
        + "finalized.");

    message.println("");
    message.println("<h3>Affected executions</h3>");
    message.println("<ul>");
    appendFlowLinksToMessage(message, flows, scheme, clientHostname, clientPortNumber);
    message.println("</ul>");

    message.println("");
    message.println("<h3>Error detail</h3>");
    message.println(String.format("Following error was reported for executor-id: %s, "
            + "executor-host: %s, executor-port: %d", executor.getId(), executor.getHost(),
        executor.getPort()));
    message.println("<pre>" + ExceptionUtils.getStackTrace(failureException) + "</pre>");
    message.println("</body></html>");

    return true;
  }

  private void appendFlowLinksToMessage(final EmailMessage message,
      final List<ExecutableFlow> flows,
      final String scheme, final String clientHostname, final String clientPortNumber) {
    for (final ExecutableFlow flow : flows) {
      final int execId = flow.getExecutionId();
      final String executionUrl =
          scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
              + "executor?" + "execid=" + execId;

      message.println("<li>Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId()
          + "' of project '" + flow.getProjectName() + "' - " +
          " <a href=\"" + executionUrl + "\">Execution Link</a></li>");
    }
  }
}
