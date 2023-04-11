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

package azkaban.flowtrigger;

import azkaban.Constants;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.project.Project;
import azkaban.utils.EmailMessage;
import azkaban.utils.Emailer;
import azkaban.utils.TimeUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@SuppressWarnings("FutureReturnValueIgnored")
public class TriggerInstanceProcessor {

  private static final Logger logger = LoggerFactory.getLogger(TriggerInstanceProcessor.class);
  private static final String FAILURE_EMAIL_SUBJECT = "flow trigger for flow '%s', project '%s' "
      + "has been cancelled on %s";
  private final static int THREAD_POOL_SIZE = 32;
  private final ExecutorManagerAdapter executorManager;
  private final FlowTriggerInstanceLoader flowTriggerInstanceLoader;
  private final Emailer emailer;
  private final ExecutorService executorService;

  @Inject
  public TriggerInstanceProcessor(final ExecutorManagerAdapter executorManager,
      final FlowTriggerInstanceLoader flowTriggerInstanceLoader,
      final Emailer emailer) {
    Preconditions.checkNotNull(executorManager);
    Preconditions.checkNotNull(flowTriggerInstanceLoader);
    Preconditions.checkNotNull(emailer);
    this.emailer = emailer;
    this.executorManager = executorManager;
    this.flowTriggerInstanceLoader = flowTriggerInstanceLoader;
    this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE,
        new ThreadFactoryBuilder().setNameFormat("azk-trigger-instance-pool-%d").build());
  }

  private void executeFlowAndUpdateExecID(final TriggerInstance triggerInst) {
    try {
      final Project project = triggerInst.getProject();
      final Flow flow = FlowUtils.getFlow(project, triggerInst.getFlowId());
      final ExecutableFlow executableFlow = FlowUtils.createExecutableFlow(project, flow,
          this.executorManager, logger);
      // execute the flow with default execution option(concurrency option being "ignore
      // currently running")
      executableFlow.setUploadUser(project.getUploadUser());
      executableFlow.setExecutionSource(Constants.EXECUTION_SOURCE_EVENT);
      this.executorManager.submitExecutableFlow(executableFlow, triggerInst.getSubmitUser());
      triggerInst.setFlowExecId(executableFlow.getExecutionId());
    } catch (final Exception ex) {
      logger.error("exception when executing the associated flow and updating flow exec id for "
              + "trigger instance[id: {}]",
          triggerInst.getId(), ex);
      // if flow fails to be executed(e.g. running execution exceeds the allowed concurrent run
      // limit), set associated flow exec id to Constants.FAILED_EXEC_ID. Upon web server
      // restart, recovery process will skip those flows.
      triggerInst.setFlowExecId(Constants.FAILED_EXEC_ID);
    }

    this.flowTriggerInstanceLoader.updateAssociatedFlowExecId(triggerInst);
  }

  private String generateFailureEmailSubject(final TriggerInstance triggerInstance) {
    return String.format(FAILURE_EMAIL_SUBJECT, triggerInstance.getFlowId(), triggerInstance
        .getProjectName(), this.emailer.getAzkabanName());
  }

  private EmailMessage createFlowTriggerFailureEmailMessage(final TriggerInstance triggerInst) {
    final EmailMessage message = this.emailer.createEmailMessage(generateFailureEmailSubject
        (triggerInst), "text/html", triggerInst.getFailureEmails());
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    message.addAllToAddress(triggerInst.getFailureEmails());
    message.setMimeType("text/html");
    message.println("<table>");
    message.println("<tr><td>Start Time</td><td>");
    message.println("<tr><td>" + sdf.format(new Date(triggerInst.getStartTime())) + "</td><td>");

    message.println("<tr><td>End Time</td><td>");
    message.println("<tr><td>" + sdf.format(new Date(triggerInst.getEndTime())) + "</td><td>");
    message.println("<tr><td>Duration</td><td>"
        + TimeUtils.formatDuration(triggerInst.getStartTime(), triggerInst.getEndTime())
        + "</td></tr>");
    message.println("<tr><td>Status</td><td>" + triggerInst.getStatus() + "</td></tr>");
    message.println("</table>");
    message.println("");
    final String executionUrl = this.emailer.getAzkabanURL() + "/executor?triggerinstanceid="
        + triggerInst.getId();

    message.println("<a href=\"" + executionUrl + "\">" + triggerInst.getFlowId()
        + " Flow Trigger Instance Link</a>");

    message.println("");
    message.println("<h3>Cancelled Dependencies</h3>");

    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      if (depInst.getStatus() == Status.CANCELLED) {
        message.println("<table>");
        message.println("<tr><td>Dependency Name: " + depInst.getDepName() + "</td><td>");
        message
            .println("<tr><td>Cancellation Cause: " + depInst.getCancellationCause() + "</td><td>");
        message.println("</table>");
      }
    }

    return message;
  }

  private void sendFailureEmailIfConfigured(final TriggerInstance triggerInstance) {
    final List<String> failureEmails = triggerInstance.getFailureEmails();
    if (!failureEmails.isEmpty()) {
      final EmailMessage message = this.createFlowTriggerFailureEmailMessage(triggerInstance);
      this.emailer.sendEmail(message, true, "email message failure email for flow trigger "
          + triggerInstance.getId());
    }
  }

  /**
   * Process the case where status of trigger instance becomes success
   */
  public void processSucceed(final TriggerInstance triggerInst) {
    //todo chengren311: publishing Trigger events to Azkaban Project Events page
    this.executorService.submit(() -> executeFlowAndUpdateExecID(triggerInst));
  }

  /**
   * Process the case where status of trigger instance becomes cancelled
   */
  public void processTermination(final TriggerInstance triggerInst) {
    //sendFailureEmailIfConfigured takes 1/3 secs
    this.executorService.submit(() -> sendFailureEmailIfConfigured(triggerInst));
  }

  /**
   * Process the case where a new trigger instance is created
   */
  public void processNewInstance(final TriggerInstance triggerInst) {
    this.flowTriggerInstanceLoader.uploadTriggerInstance(triggerInst);
  }

  public void shutdown() {
    this.executorService.shutdown();
    this.executorService.shutdownNow();
  }
}
