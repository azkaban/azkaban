/*
 * Copyright 2018 LinkedIn Corp.
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
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.utils.EmailMessage;
import azkaban.utils.Emailer;
import azkaban.utils.Utils;
import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemplateBasedMailCreator implements MailCreator {

  private static final Logger logger = Logger.getLogger(TemplateBasedMailCreator.class);

  public static final String NAME = "template-based";
  private static final DateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

  private static String convertMSToString(final long timeInMS) {
    return timeInMS < 0 ? "N/A" : DATE_FORMATTER.format(new Date(timeInMS));
  }

  private static String readResource(final String file) throws Exception {
    final InputStream is = TemplateBasedMailCreator.class.getResourceAsStream(file);
    return IOUtils.toString(is, Charsets.UTF_8).trim();
  }

  private static String readFile(final String file) throws Exception {
    return new String(Files.readAllBytes(Paths.get(file)), Charset.defaultCharset());
  }

  private String errorTemplate;
  private String firstErrorTemplate;
  private String successTemplate;

  public static TemplateBasedMailCreator fromResources(String emailTemplatesPath) throws Exception {
    return new TemplateBasedMailCreator(
        readResource(emailTemplatesPath + "/errorEmail.html"),
        readResource(emailTemplatesPath + "/firstErrorEmail.html"),
        readResource(emailTemplatesPath + "/successEmail.html")
    );
  }

  public static TemplateBasedMailCreator fromPath(String emailTemplatesPath) throws Exception {
    return new TemplateBasedMailCreator(
        readFile(emailTemplatesPath + "/errorEmail.html"),
        readFile(emailTemplatesPath + "/firstErrorEmail.html"),
        readFile(emailTemplatesPath + "/successEmail.html")
    );
  }

  public TemplateBasedMailCreator(String errorTemplate, String firstErrorTemplate,
      String successTemplate) throws Exception {
    this.errorTemplate = errorTemplate;
    this.firstErrorTemplate = firstErrorTemplate;
    this.successTemplate = successTemplate;
  }

  private static String regexVar(String name) {
    return "\\$\\{" + name + "\\}";
  }

  private static String regexGroup(String name) {
    return "<" + name + ">(.*?)</" + name + ">";
  }

  private static <T> String substituteList(String in, Pattern pattern, Iterable<T> list,
      BiFunction<String, T, String> substitution) {
    Matcher m = pattern.matcher(in);
    StringBuffer sb = new StringBuffer(in.length());
    while (m.find()) {
      String inside = m.group(1);
      StringBuffer failedJobsList = new StringBuffer();
      for (final T element : list) {
        failedJobsList.append(substitution.apply(inside, element));
      }
      m.appendReplacement(sb, failedJobsList.toString());
    }
    m.appendTail(sb);
    return sb.toString();
  }

  private static String fillTemplate(final String template, final ExecutableFlow flow,
      final String azkabanName, final String scheme, final String clientHostname,
      final String clientPortNumber) {
    final ExecutionOptions option = flow.getExecutionOptions();
    final int execId = flow.getExecutionId();
    final String azkabanUrl = scheme + "://" + clientHostname + ":" + clientPortNumber;
    final String executionUrl = azkabanUrl + "/executor?execid=" + execId;
    final String projectUrl = azkabanUrl + "/manager?project=" + flow.getProjectName();
    final String flowUrl = projectUrl + "&flow=" + flow.getFlowId();

    String failureActionMessage;
    if (option.getFailureAction() == FailureAction.CANCEL_ALL) {
      failureActionMessage = "This flow is set to cancel all currently running jobs.";
    } else if (option.getFailureAction() == FailureAction.FINISH_ALL_POSSIBLE) {
      failureActionMessage = "This flow is set to complete all jobs that aren't blocked by the failure.";
    } else {
      failureActionMessage = "This flow is set to complete all currently running jobs before stopping.";
    }

    // stage I: vars
    String partiallySubstituted = template
        .replaceAll("(?m)^\\s*#.*$", "") // remove comments
        .replaceAll(regexVar("azkabanName"), azkabanName)
        .replaceAll(regexVar("azkabanUrl"), azkabanUrl)
        .replaceAll(regexVar("projectName"), flow.getProjectName())
        .replaceAll(regexVar("projectUrl"), projectUrl)
        .replaceAll(regexVar("flowId"), flow.getFlowId())
        .replaceAll(regexVar("flowPath"), flow.getFlowPath())
        .replaceAll(regexVar("flowUrl"), flowUrl)
        .replaceAll(regexVar("executionId"), String.valueOf(flow.getExecutionId()))
        .replaceAll(regexVar("executionPath"), flow.getExecutionPath())
        .replaceAll(regexVar("executionUrl"), executionUrl)
        .replaceAll(regexVar("duration"),
            Utils.formatTimeDiffSI(flow.getStartTime(), flow.getEndTime()))
        .replaceAll(regexVar("submitTime"), convertMSToString(flow.getSubmitTime()))
        .replaceAll(regexVar("startTime"), convertMSToString(flow.getStartTime()))
        .replaceAll(regexVar("endTime"), convertMSToString(flow.getEndTime()))
        .replaceAll(regexVar("status"), flow.getStatus().toString())
        .replaceAll(regexVar("concurrentOption"), option.getConcurrentOption().toUpperCase())
        .replaceAll(regexVar("failureAction"), option.getFailureAction().toString())
        .replaceAll(regexVar("failureActionMessage"), failureActionMessage)
        .replaceAll(regexVar("mailCreator"), option.getMailCreator());

    // stage II: lists
    partiallySubstituted = substituteList(
        partiallySubstituted,
        Pattern.compile(regexGroup("failedJobs"), Pattern.DOTALL),
        Emailer.findFailedJobs(flow),
        (String inside, String jobId) ->
            inside
                .replaceAll(regexVar("jobUrl"), executionUrl + "&job=" + jobId)
                .replaceAll(regexVar("jobId"), jobId)
    );

    partiallySubstituted = substituteList(
        partiallySubstituted,
        Pattern.compile(regexGroup("flowParameters"), Pattern.DOTALL),
        new TreeMap(option.getFlowParameters()).entrySet(),
        (String inside, Map.Entry<String, String> keyValue) ->
            inside
                .replaceAll(regexVar("key"), keyValue.getKey())
                .replaceAll(regexVar("value"), keyValue.getValue())
    );

    return partiallySubstituted.replaceAll("\n+", "\n");
  }

  private boolean fillMessage(final String template, final String subject,
      final ExecutableFlow flow, final EmailMessage message, final String azkabanName,
      final String scheme, final String clientHostname, final String clientPortNumber) {
    final List<String> emailList = flow.getExecutionOptions().getFailureEmails();
    final boolean isActuallySendingEmails = emailList != null && !emailList.isEmpty();

    if (isActuallySendingEmails) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject(subject);
      message.println(
          fillTemplate(template, flow, azkabanName, scheme, clientHostname, clientPortNumber));
    }

    return isActuallySendingEmails;
  }

  @Override
  public String getName() {
    return NAME;
  }

  // NOTE: content of $vars is thrown away until it is documented how is it used (or removed altogether)
  @Override
  public boolean createFirstErrorMessage(final ExecutableFlow flow, final EmailMessage message,
      final String azkabanName, final String scheme, final String clientHostname,
      final String clientPortNumber, final String... vars) {
    logger.info("building first error email");
    return fillMessage(
        firstErrorTemplate,
        flow.getProjectName() + " / " + flow.getFlowId() + " has encountered a failure on "
            + azkabanName,
        flow, message, azkabanName, scheme, clientHostname, clientPortNumber);
  }

  @Override
  public boolean createErrorEmail(final ExecutableFlow flow, final EmailMessage message,
      final String azkabanName, final String scheme, final String clientHostname,
      final String clientPortNumber, final String... vars) {
    logger.info("building error email");
    return fillMessage(
        errorTemplate,
        flow.getProjectName() + " / " + flow.getFlowId() + " has failed on " + azkabanName,
        flow, message, azkabanName, scheme, clientHostname, clientPortNumber);
  }

  @Override
  public boolean createSuccessEmail(final ExecutableFlow flow, final EmailMessage message,
      final String azkabanName, final String scheme, final String clientHostname,
      final String clientPortNumber, final String... vars) {
    logger.info("building success email");
    return fillMessage(
        successTemplate,
        flow.getProjectName() + " / " + flow.getFlowId() + " has succeeded on " + azkabanName,
        flow, message, azkabanName, scheme, clientHostname, clientPortNumber);
  }
}
