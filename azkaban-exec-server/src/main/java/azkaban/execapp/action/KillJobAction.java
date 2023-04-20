/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.execapp.action;

import azkaban.executor.IFlowRunnerManager;
import azkaban.trigger.TriggerAction;
import java.util.Map;
import org.apache.log4j.Logger;


public class KillJobAction implements TriggerAction {

  public static final String type = "KillJobAction";

  private static final Logger logger = Logger.getLogger(KillJobAction.class);

  private final String actionId;
  private final int execId;
  private final String jobId;
  private final IFlowRunnerManager flowRunnerManager;

  public KillJobAction(final String actionId, final int execId, final String jobId,
      final IFlowRunnerManager flowRunnerManager) {
    this.execId = execId;
    this.actionId = actionId;
    this.jobId = jobId;
    this.flowRunnerManager = flowRunnerManager;
  }

  @Override
  public String getId() {
    return this.actionId;
  }

  @Override
  public String getType() {
    return type;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KillJobAction fromJson(final Object obj) throws Exception {
    throw new UnsupportedOperationException("Operation not supported for this trigger action.");
  }

  @Override
  public Object toJson() {
    throw new UnsupportedOperationException("Operation not supported for this trigger action.");
  }

  @Override
  public void doAction() throws Exception {
    logger.info("ready to do action " + getDescription());
    this.flowRunnerManager.cancelJobBySLA(this.execId, this.jobId);
  }

  @Override
  public void setContext(final Map<String, Object> context) {
  }

  @Override
  public String getDescription() {
    return type + " for execution " + this.execId + " jobId " + this.jobId;
  }

}
