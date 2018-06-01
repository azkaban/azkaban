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
import azkaban.executor.ExecutorManager;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.project.Project;
import azkaban.utils.Emailer;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("FutureReturnValueIgnored")
@Singleton
public class TriggerInstanceProcessor {

  private static final Logger logger = LoggerFactory.getLogger(TriggerInstanceProcessor.class);
  private static final String FAILURE_EMAIL_SUBJECT = "flow trigger for %s "
      + "cancelled from %s";
  private static final String FAILURE_EMAIL_BODY = "Your flow trigger cancelled [id: %s]";
  private final static int THREAD_POOL_SIZE = 32;
  private final ExecutorManager executorManager;
  private final FlowTriggerInstanceLoader flowTriggerInstanceLoader;
  private final Emailer emailer;
  private final ExecutorService executorService;

  @Inject
  public TriggerInstanceProcessor(final ExecutorManager executorManager,
      final FlowTriggerInstanceLoader flowTriggerInstanceLoader,
      final Emailer emailer) {
    Preconditions.checkNotNull(executorManager);
    Preconditions.checkNotNull(flowTriggerInstanceLoader);
    Preconditions.checkNotNull(emailer);
    this.emailer = emailer;
    this.executorManager = executorManager;
    this.flowTriggerInstanceLoader = flowTriggerInstanceLoader;
    this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
  }

  private void executeFlowAndUpdateExecID(final TriggerInstance triggerInst) {
    try {
      final Project project = triggerInst.getProject();
      final Flow flow = FlowUtils.getFlow(project, triggerInst.getFlowId());
      final ExecutableFlow executableFlow = FlowUtils.createExecutableFlow(project, flow);
      // execute the flow with default execution option(concurrency option being "ignore
      // currently running")
      this.executorManager.submitExecutableFlow(executableFlow, triggerInst.getSubmitUser());
      triggerInst.setFlowExecId(executableFlow.getExecutionId());
    } catch (final Exception ex) {
      logger.error(String.format(
          "exception when executing the associated flow and updating flow exec id for trigger instance[id: %s]",
          triggerInst.getId()), ex);
      // if flow fails to be executed(e.g. running execution exceeds the allowed concurrent run
      // limit), set associated flow exec id to Constants.FAILED_EXEC_ID. Upon web server
      // restart, recovery process will skip those flows.
      triggerInst.setFlowExecId(Constants.FAILED_EXEC_ID);
    }

    this.flowTriggerInstanceLoader.updateAssociatedFlowExecId(triggerInst);
  }

  private String generateFailureEmailSubject(final TriggerInstance triggerInstance) {
    final String flowFullName =
        triggerInstance.getProjectName() + "." + triggerInstance.getFlowId();
    return String.format(FAILURE_EMAIL_SUBJECT, flowFullName, this.emailer.getAzkabanName());
  }

  private String generateFailureEmailBody(final TriggerInstance triggerInstance) {
    final String triggerInstFullName =
        triggerInstance.getProjectName() + "." + triggerInstance.getFlowId();
    return String.format(FAILURE_EMAIL_BODY, triggerInstFullName);
  }

  private void sendFailureEmailIfConfigured(final TriggerInstance triggerInstance) {
    final List<String> failureEmails = triggerInstance.getFailureEmails();
    if (!failureEmails.isEmpty()) {
      this.emailer.sendEmail(failureEmails, generateFailureEmailSubject(triggerInstance),
          generateFailureEmailBody(triggerInstance));
    }
  }

  /**
   * Process the case where status of trigger instance becomes success
   */
  public void processSucceed(final TriggerInstance triggerInst) {
    logger.debug("process succeed for " + triggerInst);
    //todo chengren311: publishing Trigger events to Azkaban Project Events page
    executeFlowAndUpdateExecID(triggerInst);
  }

  /**
   * Process the case where status of trigger instance becomes cancelled
   */
  public void processTermination(final TriggerInstance triggerInst) {
    logger.debug("process termination for " + triggerInst);
    //sendFailureEmailIfConfigured takes 1/3 secs
    this.executorService.submit(() -> sendFailureEmailIfConfigured(triggerInst));
  }

  /**
   * Process the case where a new trigger instance is created
   */
  public void processNewInstance(final TriggerInstance triggerInst) {
    logger.debug("process new instance for " + triggerInst);
    this.flowTriggerInstanceLoader.uploadTriggerInstance(triggerInst);
  }

  public void shutdown() {
    this.executorService.shutdown();
    this.executorService.shutdownNow();
  }
}
