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

import azkaban.Constants;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.sla.SlaOption;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


public class ExecuteFlowAction implements TriggerAction {

  public static final String type = "ExecuteFlowAction";

  public static final String EXEC_ID = "ExecuteFlowAction.execid";

  private static ExecutorManagerAdapter executorManagerAdapter;
  private static TriggerManager triggerManager;
  private static ProjectManager projectManager;
  private static Logger logger = Logger.getLogger(ExecuteFlowAction.class);
  private final String actionId;
  private final String projectName;
  private int projectId;
  private String flowName;
  private String submitUser;
  private ExecutionOptions executionOptions = new ExecutionOptions();

  public ExecuteFlowAction(final String actionId, final int projectId, final String projectName,
      final String flowName, final String submitUser, final ExecutionOptions executionOptions) {
    this.actionId = actionId;
    this.projectId = projectId;
    this.projectName = projectName;
    this.flowName = flowName;
    this.submitUser = submitUser;
    this.executionOptions = executionOptions;
  }

  public static void setLogger(final Logger logger) {
    ExecuteFlowAction.logger = logger;
  }

  public static ExecutorManagerAdapter getExecutorManager() {
    return executorManagerAdapter;
  }

  public static void setExecutorManager(
      final ExecutorManagerAdapter executorManagerAdapter) {
    ExecuteFlowAction.executorManagerAdapter = executorManagerAdapter;
  }

  public static TriggerManager getTriggerManager() {
    return triggerManager;
  }

  public static void setTriggerManager(final TriggerManager triggerManager) {
    ExecuteFlowAction.triggerManager = triggerManager;
  }

  public static ProjectManager getProjectManager() {
    return projectManager;
  }

  public static void setProjectManager(final ProjectManager projectManager) {
    ExecuteFlowAction.projectManager = projectManager;
  }

  public static TriggerAction createFromJson(final HashMap<String, Object> obj) {
    final Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    final String objType = (String) jsonObj.get("type");
    if (!objType.equals(type)) {
      throw new RuntimeException("Cannot create action of " + type + " from "
          + objType);
    }
    final String actionId = (String) jsonObj.get("actionId");
    final int projectId = Integer.valueOf((String) jsonObj.get("projectId"));
    final String projectName = (String) jsonObj.get("projectName");
    final String flowName = (String) jsonObj.get("flowName");
    final String submitUser = (String) jsonObj.get("submitUser");
    ExecutionOptions executionOptions = null;
    if (jsonObj.containsKey("executionOptions")) {
      executionOptions =
          ExecutionOptions.createFromObject(jsonObj.get("executionOptions"));
    }
    if (jsonObj.containsKey("slaOptions")) {
      ArrayList<SlaOption> slaOptions = new ArrayList<>();
      final List<Object> slaOptionsObj = (List<Object>) jsonObj.get("slaOptions");
      for (final Object slaObj : slaOptionsObj) {
        slaOptions.add(SlaOption.fromObject(slaObj));
      }
      executionOptions.setSlaOptions(slaOptions);
    }
    return new ExecuteFlowAction(actionId, projectId, projectName, flowName,
        submitUser, executionOptions);
  }

  public String getProjectName() {
    return this.projectName;
  }

  public int getProjectId() {
    return this.projectId;
  }

  protected void setProjectId(final int projectId) {
    this.projectId = projectId;
  }

  public String getFlowName() {
    return this.flowName;
  }

  protected void setFlowName(final String flowName) {
    this.flowName = flowName;
  }

  public String getSubmitUser() {
    return this.submitUser;
  }

  protected void setSubmitUser(final String submitUser) {
    this.submitUser = submitUser;
  }

  public ExecutionOptions getExecutionOptions() {
    return this.executionOptions;
  }

  protected void setExecutionOptions(final ExecutionOptions executionOptions) {
    this.executionOptions = executionOptions;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public TriggerAction fromJson(final Object obj) {
    return createFromJson((HashMap<String, Object>) obj);
  }

  @Override
  public Object toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("actionId", this.actionId);
    jsonObj.put("type", type);
    jsonObj.put("projectId", String.valueOf(this.projectId));
    jsonObj.put("projectName", this.projectName);
    jsonObj.put("flowName", this.flowName);
    jsonObj.put("submitUser", this.submitUser);
    if (this.executionOptions != null) {
      jsonObj.put("executionOptions", this.executionOptions.toObject());
    }
    List<Object> slaOptionsObj = SlaOption.convertToObjects(this.executionOptions.getSlaOptions());
    if (slaOptionsObj != null) {
      jsonObj.put("slaOptions", slaOptionsObj);
    }
    return jsonObj;
  }

  @Override
  public void doAction() throws Exception {
    if (projectManager == null || executorManagerAdapter == null) {
      throw new Exception("ExecuteFlowAction not properly initialized!");
    }

    final Project project = FlowUtils.getProject(projectManager, this.projectId);
    final Flow flow = FlowUtils.getFlow(project, this.flowName);

    final ExecutableFlow exflow = executorManagerAdapter.createExecutableFlow(project, flow);

    exflow.setSubmitUser(this.submitUser);
    exflow.setExecutionSource(Constants.EXECUTION_SOURCE_SCHEDULED);

    if (this.executionOptions == null) {
      this.executionOptions = new ExecutionOptions();
    }
    if (!this.executionOptions.isFailureEmailsOverridden()) {
      this.executionOptions.setFailureEmails(flow.getFailureEmails());
    }
    if (!this.executionOptions.isSuccessEmailsOverridden()) {
      this.executionOptions.setSuccessEmails(flow.getSuccessEmails());
    }

    // this.executionOptions comes from the input parsed from webUI, need to merge it together with
    // the project-defined flow-params
    exflow.getExecutionOptions().merge(this.executionOptions);
    exflow.setUploadUser(project.getUploadUser());
    logger.info("Invoking flow " + project.getName() + "." + this.flowName);
    executorManagerAdapter.submitExecutableFlow(exflow, this.submitUser);
    logger.info("Invoked flow " + project.getName() + "." + this.flowName);
  }

  @Override
  public String getDescription() {
    return "Execute flow " + getFlowName() + " from project "
        + getProjectName();
  }

  @Override
  public void setContext(final Map<String, Object> context) {
  }

  @Override
  public String getId() {
    return this.actionId;
  }
}
