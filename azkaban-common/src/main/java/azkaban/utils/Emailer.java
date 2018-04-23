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

import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.mail.DefaultMailCreator;
import azkaban.executor.mail.MailCreator;
import azkaban.metrics.CommonMetrics;
import azkaban.sla.SlaOption;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
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

  @Inject
  public Emailer(final Props props, final CommonMetrics commonMetrics,
      final EmailMessageCreator messageCreator) {
    super(props, messageCreator);
    this.commonMetrics = requireNonNull(commonMetrics, "commonMetrics is null.");
    this.azkabanName = props.getString("azkaban.name", "azkaban");

    final int mailTimeout = props.getInt("mail.timeout.millis", 30000);
    EmailMessage.setTimeout(mailTimeout);
    final int connectionTimeout =
        props.getInt("mail.connection.timeout.millis", 30000);
    EmailMessage.setConnectionTimeout(connectionTimeout);

    EmailMessage.setTotalAttachmentMaxSize(getAttachmentMaxSize());

    this.clientHostname = props.getString(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME,
        props.getString("jetty.hostname", "localhost"));

    if (props.getBoolean("jetty.use.ssl", true)) {
      this.scheme = HTTPS;
      this.clientPortNumber = Integer.toString(props
          .getInt(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_SSL_PORT,
              props.getInt("jetty.ssl.port",
                  Constants.DEFAULT_SSL_PORT_NUMBER)));
    } else {
      this.scheme = HTTP;
      this.clientPortNumber = Integer.toString(
          props.getInt(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_PORT, props.getInt("jetty.port",
              Constants.DEFAULT_PORT_NUMBER)));
    }
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
        (List<String>) slaOption.getInfo().get(SlaOption.INFO_EMAIL_LIST);
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
    final EmailMessage message = this.messageCreator.createMessage();
    final MailCreator mailCreator = getMailCreator(flow);
    final boolean mailCreated = mailCreator.createErrorEmail(flow, message, this.azkabanName,
        this.scheme, this.clientHostname, this.clientPortNumber, extraReasons);
    sendEmail(message, mailCreated, "error email message for execution " + flow.getExecutionId());
  }

  @Override
  public void alertOnSuccess(final ExecutableFlow flow) {
    final EmailMessage message = this.messageCreator.createMessage();
    final MailCreator mailCreator = getMailCreator(flow);
    final boolean mailCreated = mailCreator.createSuccessEmail(flow, message, this.azkabanName,
        this.scheme, this.clientHostname, this.clientPortNumber);
    sendEmail(message, mailCreated, "success email message for execution" + flow.getExecutionId());
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

  private void sendEmail(final EmailMessage message, final boolean mailCreated,
      final String operation) {
    if (mailCreated) {
      try {
        message.sendEmail();
        logger.info("Sent " + operation);
        this.commonMetrics.markSendEmailSuccess();
      } catch (final Exception e) {
        logger.error("Failed to send " + operation, e);
        this.commonMetrics.markSendEmailFail();
      }
    }
  }

  private String getJobOrFlowName(final SlaOption slaOption) {
    final String flowName = (String) slaOption.getInfo().get(SlaOption.INFO_FLOW_NAME);
    final String jobName = (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
    if (org.apache.commons.lang.StringUtils.isNotBlank(jobName)) {
      return flowName + ":" + jobName;
    } else {
      return flowName;
    }
  }

  @Override
  public void alertOnSuccess(final ExecutableFlow exflow) {
    sendSuccessEmail(exflow);
  }

  @Override
  public void alertOnError(final ExecutableFlow exflow, final String... extraReasons) {
    sendErrorEmail(exflow, extraReasons);
  }

  @Override
  public void alertOnFlowTriggerCancel(final String triggerInstanceId, String projectName, String
   flowName) {
    sendErrorEmail(exflow, extraReasons);
  }

  @Override
  public void alertOnFirstError(final ExecutableFlow exflow) {
    sendFirstErrorMessage(exflow);
  }

}
