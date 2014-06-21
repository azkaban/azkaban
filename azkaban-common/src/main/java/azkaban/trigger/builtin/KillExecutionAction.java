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

package azkaban.trigger.builtin;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.Status;
import azkaban.trigger.TriggerAction;

public class KillExecutionAction implements TriggerAction {

  public static final String type = "KillExecutionAction";

  private static final Logger logger = Logger
      .getLogger(KillExecutionAction.class);

  private String actionId;
  private int execId;
  private static ExecutorManagerAdapter executorManager;

  public KillExecutionAction(String actionId, int execId) {
    this.execId = execId;
    this.actionId = actionId;
  }

  public static void setExecutorManager(ExecutorManagerAdapter em) {
    executorManager = em;
  }

  @Override
  public String getId() {
    return actionId;
  }

  @Override
  public String getType() {
    return type;
  }

  @SuppressWarnings("unchecked")
  public static KillExecutionAction createFromJson(Object obj) {
    return createFromJson((HashMap<String, Object>) obj);
  }

  public static KillExecutionAction createFromJson(HashMap<String, Object> obj) {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    String objType = (String) jsonObj.get("type");
    if (!objType.equals(type)) {
      throw new RuntimeException("Cannot create action of " + type + " from "
          + objType);
    }
    String actionId = (String) jsonObj.get("actionId");
    int execId = Integer.valueOf((String) jsonObj.get("execId"));
    return new KillExecutionAction(actionId, execId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public KillExecutionAction fromJson(Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("actionId", actionId);
    jsonObj.put("type", type);
    jsonObj.put("execId", String.valueOf(execId));
    return jsonObj;
  }

  @Override
  public void doAction() throws Exception {
    ExecutableFlow exFlow = executorManager.getExecutableFlow(execId);
    logger.info("ready to kill execution " + execId);
    if (!Status.isStatusFinished(exFlow.getStatus())) {
      logger.info("Killing execution " + execId);
      executorManager.cancelFlow(exFlow, "azkaban_sla");
    }
  }

  @Override
  public void setContext(Map<String, Object> context) {
  }

  @Override
  public String getDescription() {
    return type + " for " + execId;
  }

}
