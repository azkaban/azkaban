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

import azkaban.Constants;
import azkaban.ServiceProvider;
import azkaban.execapp.FlowRunnerManager;
import azkaban.trigger.builtin.SlaChecker;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.trigger.TriggerAction;

public class KillJobAction implements TriggerAction {

  public static final String type = "KillJobAction";

  private static final Logger logger = Logger
      .getLogger(KillJobAction.class);

  private String actionId;
  private int execId;
  private String jobId;

  public KillJobAction(String actionId, int execId, String jobId) {
    this.execId = execId;
    this.actionId = actionId;
    this.jobId = jobId;
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
  public static KillJobAction createFromJson(Object obj) {
    return createFromJson((HashMap<String, Object>) obj);
  }

  public static KillJobAction createFromJson(HashMap<String, Object> obj) {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    String objType = (String) jsonObj.get("type");
    if (!objType.equals(type)) {
      throw new RuntimeException("Cannot create action of " + type + " from "
          + objType);
    }
    String actionId = (String) jsonObj.get("actionId");
    int execId = Integer.valueOf((String) jsonObj.get("execId"));
    String jobId = (String) jsonObj.get("jobId");
    return new KillJobAction(actionId, execId, jobId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public KillJobAction fromJson(Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("actionId", actionId);
    jsonObj.put("type", type);
    jsonObj.put("execId", String.valueOf(execId));
    jsonObj.put("jobId", String.valueOf(jobId));
    return jsonObj;
  }

  @Override
  public void doAction() throws Exception {
    logger.info("ready to do action " + getDescription());
    FlowRunnerManager flowRunnerManager = ServiceProvider.SERVICE_PROVIDER.getInstance(FlowRunnerManager.class);
    flowRunnerManager.cancelJobBySLA(execId, jobId);
  }

  @Override
  public void setContext(Map<String, Object> context) {
  }

  @Override
  public String getDescription() {
    return type + " for execution " + execId + " jobId " + jobId;
  }

}