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

import static azkaban.Constants.ConfigurationKeys.JETTY_HOSTNAME;
import static azkaban.Constants.EventReporterConstants.MODIFIED_BY;
import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.mail.DefaultMailCreator;
import azkaban.executor.mail.MailCreator;
import azkaban.flow.Flow;
import azkaban.metrics.CommonMetrics;
import azkaban.project.Project;
import azkaban.sla.SlaOption;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.mail.internet.AddressException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

@Singleton
public class Emailer extends AbstractMailer implements Alerter {

  private static final String HTTPS = "https";
  private static final String HTTP = "http";
  private static final Logger logger = Logger.getLogger(Emailer.class);
  private final CommonMetrics commonMetrics;
  private final String scheme;
  private final String clientHostname;
  private final String clientPortNumber;
  private final String azkabanName;
  private final ExecutorLoader executorLoader;

  @Inject
  public Emailer(final Props props, final CommonMetrics commonMetrics,
      final EmailMessageCreator messageCreator, final ExecutorLoader executorLoader) {
    super(props, messageCreator);
    this.executorLoader = requireNonNull(executorLoader, "executorLoader is null.");
    this.commonMetrics = requireNonNull(commonMetrics, "commonMetrics is null.");
    this.azkabanName = props.getString("azkaban.name", "azkaban");

    final int mailTimeout = props.getInt("mail.timeout.millis", 30000);
    EmailMessage.setTimeout(mailTimeout);
    final int connectionTimeout = props.getInt("mail.connection.timeout.millis", 30000);
    EmailMessage.setConnectionTimeout(connectionTimeout);

    EmailMessage.setTotalAttachmentMaxSize(getAttachmentMaxSize());

    this.clientHostname = props.getString(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME,
        props.getString(JETTY_HOSTNAME, "localhost"));

    if (props.getBoolean(ConfigurationKeys.JETTY_USE_SSL, true)) {
      this.scheme = HTTPS;
      this.clientPortNumber = Integer.toString(props
          .getInt(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_SSL_PORT,
              props.getInt(ConfigurationKeys.JETTY_SSL_PORT,
                  Constants.DEFAULT_SSL_PORT_NUMBER)));
    } else {
      this.scheme = HTTP;
      this.clientPortNumber = Integer.toString(
          props.getInt(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_PORT,
              props.getInt(ConfigurationKeys.JETTY_PORT, Constants.DEFAULT_PORT_NUMBER)));
    }
  }

  @Override
  public String getAzkabanURL() {
    return this.scheme + "://" + this.clientHostname + ":" + this.clientPortNumber;
  }

  /**
   * Send an email to the specified email list
   */
  public void sendEmail(final List<String> emailList, final String subject, final String body) {
    if (emailList != null && !emailList.isEmpty()) {
      final EmailMessage message = super.createEmailMessage(subject, "text/html", emailList);
      message.setBody(body);
      sendEmail(message, true, "email message " + body);
    }
  }

  @Override
  public void alertOnSla(final SlaOption slaOption, final String slaMessage) {
    final String subject =
        "SLA violation for " + getJobOrFlowName(slaOption) + " on " + getAzkabanName();
    final List<String> emailList =
        (List<String>) slaOption.getEmails();
    logger.info("Sending SLA email " + slaMessage);
    sendEmail(emailList, subject, slaMessage);
  }

  @Override
  public void alertOnFirstError(final ExecutableFlow flow) {
    final EmailMessage message = this.messageCreator.createMessage();
    final MailCreator mailCreator = getMailCreator(flow);
    final boolean mailCreated = mailCreator.createFirstErrorMessage(flow, message, this.azkabanName,
        this.scheme, this.clientHostname, this.clientPortNumber);
    sendEmail(message, mailCreated,
        "first error email message for execution " + flow.getExecutionId());
  }

  @Override
  public void alertOnError(final ExecutableFlow flow, final String... extraReasons) {
    final List<String> emailRecipients = flow.getExecutionOptions().getFailureEmails();
    if (emailRecipients == null || emailRecipients.isEmpty()) {
      return;
    }

    final EmailMessage message = this.messageCreator.createMessage();
    final MailCreator mailCreator = getMailCreator(flow);
    List<ExecutableFlow> last72hoursExecutions = new ArrayList<>();

    if (flow.getStartTime() > 0) {
      final long startTime = flow.getStartTime() - Duration.ofHours(72).toMillis();
      try {
        last72hoursExecutions = this.executorLoader.fetchFlowHistory(flow.getProjectId(), flow
            .getFlowId(), startTime);
      } catch (final ExecutorManagerException e) {
        logger.error("unable to fetch past executions", e);
      }
    }

    final boolean mailCreated = mailCreator.createErrorEmail(flow, last72hoursExecutions, message,
        this.azkabanName, this.scheme, this.clientHostname, this.clientPortNumber, extraReasons);
    sendEmail(message, mailCreated, "error email message for execution " + flow.getExecutionId());
  }

  @Override
  public void alertOnSuccess(final ExecutableFlow flow) {
    final List<String> emailRecipients = flow.getExecutionOptions().getSuccessEmails();
    if (emailRecipients == null || emailRecipients.isEmpty()) {
      return;
    }

    final EmailMessage message = this.messageCreator.createMessage();
    final MailCreator mailCreator = getMailCreator(flow);
    final boolean mailCreated = mailCreator.createSuccessEmail(flow, message, this.azkabanName,
        this.scheme, this.clientHostname, this.clientPortNumber);
    sendEmail(message, mailCreated, "success email message for execution " + flow.getExecutionId());
  }

  /**
   * Sends as many emails as there are unique combinations of:
   * <p>
   * [mail creator] x [failure email address list]
   * <p>
   * Executions with the same combo are grouped into a single message.
   */
  @Override
  public void alertOnFailedUpdate(final Executor executor, List<ExecutableFlow> flows,
      final ExecutorManagerException updateException) {

    flows = flows.stream()
        .filter(flow -> flow.getExecutionOptions() != null)
        .filter(flow -> CollectionUtils.isNotEmpty(flow.getExecutionOptions().getFailureEmails()))
        .collect(Collectors.toList());

    // group by mail creator in case some flows use different creators
    final ImmutableListMultimap<String, ExecutableFlow> creatorsToFlows = Multimaps
        .index(flows, flow -> flow.getExecutionOptions().getMailCreator());

    for (final String mailCreatorName : creatorsToFlows.keySet()) {

      final ImmutableList<ExecutableFlow> creatorFlows = creatorsToFlows.get(mailCreatorName);
      final MailCreator mailCreator = getMailCreator(mailCreatorName);

      // group by recipients in case some flows have different failure email addresses
      final ImmutableListMultimap<List<String>, ExecutableFlow> emailsToFlows = Multimaps
          .index(creatorFlows, flow -> flow.getExecutionOptions().getFailureEmails());

      for (final List<String> emailList : emailsToFlows.keySet()) {
        sendFailedUpdateEmail(executor, updateException, mailCreator, emailsToFlows.get(emailList));
      }
    }
  }

  /**
   * Use the default mail creator to send a failed executor healthcheck message to the given list of
   * addresses. Message includes a list of flows impacted on the executor.
   */
  @Override
  public void alertOnFailedExecutorHealthCheck(final Executor executor,
      final List<ExecutableFlow> flows, final ExecutorManagerException failureException,
      final List<String> emailList) {
    if (emailList == null || emailList.isEmpty()) {
      // We should consider throwing an exception here. For now this follows the model of the rest
      // of the file and simply returns.
      logger.error("No email list specified for failed health check alert");
      return;
    }
    final MailCreator mailCreator =
        DefaultMailCreator.getCreator(DefaultMailCreator.DEFAULT_MAIL_CREATOR);
    final EmailMessage message = this.messageCreator.createMessage();
    final boolean mailCreated = mailCreator
        .createFailedExecutorHealthCheckMessage(flows, executor, failureException, message,
            this.azkabanName, this.scheme, this.clientHostname, this.clientPortNumber, emailList);
    final List<Integer> executionIds = Lists.transform(flows, ExecutableFlow::getExecutionId);
    sendEmail(message, mailCreated, "failed health check message for executions " + executionIds);
  }

  /**
   * Alert user when a job property is overridden in a project
   * @param project
   * @param flow
   * @param eventData
   */
  @Override
  public void alertOnJobPropertyOverridden(final Project project, final Flow flow,
      final Map<String, Object> eventData) {
    final List<String> emailList = flow.getOverrideEmails();
    final String emailSubject = "[Project Property Overridden Alert]";
    final String modifier = String.valueOf(eventData.get(MODIFIED_BY));
    final String jobName = String.valueOf(eventData.get("jobOverridden"));
    final String diffMessage = String.valueOf(eventData.get("diffMessage"));
    final String emailBody =
        "User " + modifier + " has overridden following job properties in project " + project.getName()
            + " flow " + flow.getId() + " job " + jobName + ": " + "\n" + diffMessage;
    sendEmail(emailList, emailSubject, emailBody);
  }

  /**
   * Sends a single email about failed updates.
   */
  private void sendFailedUpdateEmail(final Executor executor,
      final ExecutorManagerException exception, final MailCreator mailCreator,
      final ImmutableList<ExecutableFlow> flows) {
    final EmailMessage message = this.messageCreator.createMessage();
    final boolean mailCreated = mailCreator
        .createFailedUpdateMessage(flows, executor, exception, message,
            this.azkabanName, this.scheme, this.clientHostname, this.clientPortNumber);
    final List<Integer> executionIds = Lists.transform(flows, ExecutableFlow::getExecutionId);
    sendEmail(message, mailCreated, "failed update email message for executions " + executionIds);
  }

  private MailCreator getMailCreator(final ExecutableFlow flow) {
    final String name = flow.getExecutionOptions().getMailCreator();
    return getMailCreator(name);
  }

  private MailCreator getMailCreator(final String name) {
    final MailCreator mailCreator = DefaultMailCreator.getCreator(name);
    logger.debug("ExecutorMailer using mail creator:" + mailCreator.getClass().getCanonicalName());
    return mailCreator;
  }

  public void sendEmail(final EmailMessage message, final boolean mailCreated,
      final String operation) {
    if (mailCreated) {
      try {
        message.sendEmail();
        logger.info("Sent " + operation);
        this.commonMetrics.markSendEmailSuccess();
      } catch (final Exception e) {
        logger.error("Failed to send " + operation, e);
        if (!(e instanceof AddressException)) {
          this.commonMetrics.markSendEmailFail();
        }
      }
    }
  }

  private String getJobOrFlowName(final SlaOption slaOption) {
    if (org.apache.commons.lang.StringUtils.isNotBlank(slaOption.getJobName())) {
      return slaOption.getFlowName() + ":" + slaOption.getJobName();
    } else {
      return slaOption.getFlowName();
    }
  }
}
