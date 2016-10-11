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

package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;

import azkaban.executor.ConnectorParams;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.plugin.PluginRegistry;
import azkaban.webapp.plugin.ViewerPlugin;

public class ExecutorServlet extends LoginAbstractAzkabanServlet {
  private static final Logger LOGGER = 
      Logger.getLogger(ExecutorServlet.class.getName());
  private static final long serialVersionUID = 1L;  
  private ProjectManager projectManager;
  private ExecutorManagerAdapter executorManager;
  private ScheduleManager scheduleManager;
  private ExecutorVelocityHelper velocityHelper;
  private UserManager userManager;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    userManager = server.getUserManager();
    projectManager = server.getProjectManager();
    executorManager = server.getExecutorManager();
    scheduleManager = server.getScheduleManager();
    velocityHelper = new ExecutorVelocityHelper();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else if (hasParam(req, "execid")) {
      if (hasParam(req, "job")) {
        handleExecutionJobDetailsPage(req, resp, session);
      } else {
        handleExecutionFlowPage(req, resp, session);
      }
    } else {
      handleExecutionsPage(req, resp, session);
    }
  }

  private void handleAJAXAction(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    String ajaxName = getParam(req, "ajax");

    if (hasParam(req, "execid")) {
      int execid = getIntParam(req, "execid");
      ExecutableFlow exFlow = null;

      try {
        exFlow = executorManager.getExecutableFlow(execid);
      } catch (ExecutorManagerException e) {
        ret.put("error",
            "Error fetching execution '" + execid + "': " + e.getMessage());
      }

      if (exFlow == null) {
        ret.put("error", "Cannot find execution '" + execid + "'");
      } else {
        if (ajaxName.equals("fetchexecflow")) {
          ajaxFetchExecutableFlow(req, resp, ret, session.getUser(), exFlow);
        } else if (ajaxName.equals("fetchexecflowupdate")) {
          ajaxFetchExecutableFlowUpdate(req, resp, ret, session.getUser(),
              exFlow);
        } else if (ajaxName.equals("cancelFlow")) {
          ajaxCancelFlow(req, resp, ret, session.getUser(), exFlow);
        } else if (ajaxName.equals("pauseFlow")) {
          ajaxPauseFlow(req, resp, ret, session.getUser(), exFlow);
        } else if (ajaxName.equals("resumeFlow")) {
          ajaxResumeFlow(req, resp, ret, session.getUser(), exFlow);
        } else if (ajaxName.equals("fetchExecFlowLogs")) {
          ajaxFetchExecFlowLogs(req, resp, ret, session.getUser(), exFlow);
        } else if (ajaxName.equals("fetchExecJobLogs")) {
          ajaxFetchJobLogs(req, resp, ret, session.getUser(), exFlow);
        } else if (ajaxName.equals("fetchExecJobStats")) {
          ajaxFetchJobStats(req, resp, ret, session.getUser(), exFlow);
        } else if (ajaxName.equals("retryFailedJobs")) {
          ajaxRestartFailed(req, resp, ret, session.getUser(), exFlow);
        } else if (ajaxName.equals("flowInfo")) {
          ajaxFetchExecutableFlowInfo(req, resp, ret, session.getUser(), exFlow);
        }
      }
    } else if (ajaxName.equals("reloadExecutors")) {
      ajaxReloadExecutors(req, resp, ret, session.getUser());
    } else if (ajaxName.equals("enableQueueProcessor")) {
      ajaxUpdateQueueProcessor(req, resp, ret, session.getUser(), true);
    } else if (ajaxName.equals("disableQueueProcessor")) {
      ajaxUpdateQueueProcessor(req, resp, ret, session.getUser(), false);
    } else if (ajaxName.equals("getRunning")) {
      String projectName = getParam(req, "project");
      String flowName = getParam(req, "flow");
      ajaxGetFlowRunning(req, resp, ret, session.getUser(), projectName,
          flowName);
    } else if (ajaxName.equals("flowInfo")) {
      String projectName = getParam(req, "project");
      String flowName = getParam(req, "flow");
      ajaxFetchFlowInfo(req, resp, ret, session.getUser(), projectName,
          flowName);
    } else {
      String projectName = getParam(req, "project");

      ret.put("project", projectName);
      if (ajaxName.equals("executeFlow")) {
        ajaxAttemptExecuteFlow(req, resp, ret, session.getUser());
      }
    }
    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  /**
   * <pre>
   * Enables queueProcessor if @param status is true
   * disables queueProcessor if @param status is false.
   * </pre>
   */
  private void ajaxUpdateQueueProcessor(HttpServletRequest req,
    HttpServletResponse resp, HashMap<String, Object> returnMap, User user,
    boolean enableQueue) {
    boolean wasSuccess = false;
    if (HttpRequestUtils.hasPermission(userManager, user, Type.ADMIN)) {
      try {
        if (enableQueue) {
          executorManager.enableQueueProcessorThread();
        } else {
          executorManager.disableQueueProcessorThread();
        }
        returnMap.put(ConnectorParams.STATUS_PARAM,
          ConnectorParams.RESPONSE_SUCCESS);
        wasSuccess = true;
      } catch (ExecutorManagerException e) {
        returnMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
      }
    } else {
      returnMap.put(ConnectorParams.RESPONSE_ERROR,
        "Only Admins are allowed to update queue processor");
    }
    if (!wasSuccess) {
      returnMap.put(ConnectorParams.STATUS_PARAM,
        ConnectorParams.RESPONSE_ERROR);
    }
  }

  /* Reloads executors from DB and azkaban.properties via executorManager */
  private void ajaxReloadExecutors(HttpServletRequest req,
    HttpServletResponse resp, HashMap<String, Object> returnMap, User user) {
    boolean wasSuccess = false;
    if (HttpRequestUtils.hasPermission(userManager, user, Type.ADMIN)) {
      try {
        executorManager.setupExecutors();
        returnMap.put(ConnectorParams.STATUS_PARAM,
          ConnectorParams.RESPONSE_SUCCESS);
        wasSuccess = true;
      } catch (ExecutorManagerException e) {
        returnMap.put(ConnectorParams.RESPONSE_ERROR,
          "Failed to refresh the executors " + e.getMessage());
      }
    } else {
      returnMap.put(ConnectorParams.RESPONSE_ERROR,
        "Only Admins are allowed to refresh the executors");
    }
    if (!wasSuccess) {
      returnMap.put(ConnectorParams.STATUS_PARAM,
        ConnectorParams.RESPONSE_ERROR);
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    }
  }

  private void handleExecutionJobDetailsPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/jobdetailspage.vm");
    User user = session.getUser();
    int execId = getIntParam(req, "execid");
    String jobId = getParam(req, "job");
    int attempt = getIntParam(req, "attempt", 0);
    page.add("execid", execId);
    page.add("jobid", jobId);
    page.add("attempt", attempt);

    ExecutableFlow flow = null;
    ExecutableNode node = null;
    try {
      flow = executorManager.getExecutableFlow(execId);
      if (flow == null) {
        page.add("errorMsg", "Error loading executing flow " + execId
            + ": not found.");
        page.render();
        return;
      }

      node = flow.getExecutableNodePath(jobId);
      if (node == null) {
        page.add("errorMsg",
            "Job " + jobId + " doesn't exist in " + flow.getExecutionId());
        return;
      }

      List<ViewerPlugin> jobViewerPlugins =
          PluginRegistry.getRegistry().getViewerPluginsForJobType(
              node.getType());
      page.add("jobViewerPlugins", jobViewerPlugins);
    } catch (ExecutorManagerException e) {
      page.add("errorMsg", "Error loading executing flow: " + e.getMessage());
      page.render();
      return;
    }

    int projectId = flow.getProjectId();
    Project project =
        getProjectPageByPermission(page, projectId, user, Type.READ);
    if (project == null) {
      page.render();
      return;
    }

    page.add("projectName", project.getName());
    page.add("flowid", flow.getId());
    page.add("parentflowid", node.getParentFlow().getFlowId());
    page.add("jobname", node.getId());

    page.render();
  }

  private void handleExecutionsPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/executionspage.vm");

    List<Pair<ExecutableFlow, Executor>> runningFlows =
      executorManager.getActiveFlowsWithExecutor();
    page.add("runningFlows", runningFlows.isEmpty() ? null : runningFlows);

    List<ExecutableFlow> finishedFlows =
        executorManager.getRecentlyFinishedFlows();
    page.add("recentlyFinished", finishedFlows.isEmpty() ? null : finishedFlows);
    page.add("vmutils", velocityHelper);
    page.render();
  }

  private void handleExecutionFlowPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/executingflowpage.vm");
    User user = session.getUser();
    int execId = getIntParam(req, "execid");
    page.add("execid", execId);

    ExecutableFlow flow = null;
    try {
      flow = executorManager.getExecutableFlow(execId);
      if (flow == null) {
        page.add("errorMsg", "Error loading executing flow " + execId
            + " not found.");
        page.render();
        return;
      }
    } catch (ExecutorManagerException e) {
      page.add("errorMsg", "Error loading executing flow: " + e.getMessage());
      page.render();
      return;
    }

    int projectId = flow.getProjectId();
    Project project =
        getProjectPageByPermission(page, projectId, user, Type.READ);
    if (project == null) {
      page.render();
      return;
    }
    
    Props props = getApplication().getServerProps();
    String execExternalLinkURL = 
        ExternalAnalyzerUtils.getExternalAnalyzer(props, req);

    if(execExternalLinkURL.length() > 0) {
      page.add("executionExternalLinkURL", execExternalLinkURL);
      LOGGER.debug("Added an External analyzer to the page");
      LOGGER.debug("External analyzer url: " + execExternalLinkURL);
      
      String execExternalLinkLabel = 
          props.getString(ExternalAnalyzerUtils.EXECUTION_EXTERNAL_LINK_LABEL, 
              "External Analyzer");
      page.add("executionExternalLinkLabel", execExternalLinkLabel);
      LOGGER.debug("External analyzer label set to : " + execExternalLinkLabel);
    }
    
    page.add("projectId", project.getId());
    page.add("projectName", project.getName());
    page.add("flowid", flow.getFlowId());

    page.render();
  }

  protected Project getProjectPageByPermission(Page page, int projectId,
      User user, Permission.Type type) {
    Project project = projectManager.getProject(projectId);

    if (project == null) {
      page.add("errorMsg", "Project " + project + " not found.");
    } else if (!hasPermission(project, user, type)) {
      page.add("errorMsg",
          "User " + user.getUserId() + " doesn't have " + type.name()
              + " permissions on " + project.getName());
    } else {
      return project;
    }

    return null;
  }

  protected Project getProjectAjaxByPermission(Map<String, Object> ret,
      String projectName, User user, Permission.Type type) {
    Project project = projectManager.getProject(projectName);

    if (project == null) {
      ret.put("error", "Project '" + project + "' not found.");
    } else if (!hasPermission(project, user, type)) {
      ret.put("error",
          "User '" + user.getUserId() + "' doesn't have " + type.name()
              + " permissions on " + project.getName());
    } else {
      return project;
    }

    return null;
  }

  protected Project getProjectAjaxByPermission(Map<String, Object> ret,
      int projectId, User user, Permission.Type type) {
    Project project = projectManager.getProject(projectId);

    if (project == null) {
      ret.put("error", "Project '" + project + "' not found.");
    } else if (!hasPermission(project, user, type)) {
      ret.put("error",
          "User '" + user.getUserId() + "' doesn't have " + type.name()
              + " permissions on " + project.getName());
    } else {
      return project;
    }

    return null;
  }

  private void ajaxRestartFailed(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      ExecutableFlow exFlow) throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user,
            Type.EXECUTE);
    if (project == null) {
      return;
    }

    if (exFlow.getStatus() == Status.FAILED
        || exFlow.getStatus() == Status.SUCCEEDED) {
      ret.put("error", "Flow has already finished. Please re-execute.");
      return;
    }

    try {
      executorManager.retryFailures(exFlow, user.getUserId());
    } catch (ExecutorManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  /**
   * Gets the logs through plain text stream to reduce memory overhead.
   *
   * @param req
   * @param resp
   * @param user
   * @param exFlow
   * @throws ServletException
   */
  private void ajaxFetchExecFlowLogs(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      ExecutableFlow exFlow) throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    int offset = this.getIntParam(req, "offset");
    int length = this.getIntParam(req, "length");

    resp.setCharacterEncoding("utf-8");

    try {
      LogData data =
          executorManager.getExecutableFlowLog(exFlow, offset, length);
      if (data == null) {
        ret.put("length", 0);
        ret.put("offset", offset);
        ret.put("data", "");
      } else {
        ret.put("length", data.getLength());
        ret.put("offset", data.getOffset());
        ret.put("data", StringEscapeUtils.escapeHtml(data.getData()));
      }
    } catch (ExecutorManagerException e) {
      throw new ServletException(e);
    }
  }

  /**
   * Gets the logs through ajax plain text stream to reduce memory overhead.
   *
   * @param req
   * @param resp
   * @param user
   * @param exFlow
   * @throws ServletException
   */
  private void ajaxFetchJobLogs(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      ExecutableFlow exFlow) throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    int offset = this.getIntParam(req, "offset");
    int length = this.getIntParam(req, "length");

    String jobId = this.getParam(req, "jobId");
    resp.setCharacterEncoding("utf-8");

    try {
      ExecutableNode node = exFlow.getExecutableNodePath(jobId);
      if (node == null) {
        ret.put("error",
            "Job " + jobId + " doesn't exist in " + exFlow.getExecutionId());
        return;
      }

      int attempt = this.getIntParam(req, "attempt", node.getAttempt());
      LogData data =
          executorManager.getExecutionJobLog(exFlow, jobId, offset, length,
              attempt);
      if (data == null) {
        ret.put("length", 0);
        ret.put("offset", offset);
        ret.put("data", "");
      } else {
        ret.put("length", data.getLength());
        ret.put("offset", data.getOffset());
        ret.put("data", StringEscapeUtils.escapeHtml(data.getData()));
      }
    } catch (ExecutorManagerException e) {
      throw new ServletException(e);
    }
  }

  private void ajaxFetchJobStats(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      ExecutableFlow exFlow) throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    String jobId = this.getParam(req, "jobid");
    resp.setCharacterEncoding("utf-8");

    try {
      ExecutableNode node = exFlow.getExecutableNodePath(jobId);
      if (node == null) {
        ret.put("error",
            "Job " + jobId + " doesn't exist in " + exFlow.getExecutionId());
        return;
      }

      List<Object> jsonObj =
          executorManager
              .getExecutionJobStats(exFlow, jobId, node.getAttempt());
      ret.put("jobStats", jsonObj);
    } catch (ExecutorManagerException e) {
      ret.put("error", "Error retrieving stats for job " + jobId);
      return;
    }
  }

  private void ajaxFetchFlowInfo(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      String projectName, String flowId) throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, projectName, user, Type.READ);
    if (project == null) {
      return;
    }

    Flow flow = project.getFlow(flowId);
    if (flow == null) {
      ret.put("error", "Error loading flow. Flow " + flowId
          + " doesn't exist in " + projectName);
      return;
    }

    ret.put("successEmails", flow.getSuccessEmails());
    ret.put("failureEmails", flow.getFailureEmails());

    Schedule sflow = null;
    try {
      for (Schedule sched : scheduleManager.getSchedules()) {
        if (sched.getProjectId() == project.getId()
            && sched.getFlowName().equals(flowId)) {
          sflow = sched;
          break;
        }
      }
    } catch (ScheduleManagerException e) {
      // TODO Auto-generated catch block
      throw new ServletException(e);
    }

    if (sflow != null) {
      ret.put("scheduled", sflow.getNextExecTime());
    }
  }

  private void ajaxFetchExecutableFlowInfo(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      ExecutableFlow exflow) throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, exflow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    Flow flow = project.getFlow(exflow.getFlowId());
    if (flow == null) {
      ret.put("error", "Error loading flow. Flow " + exflow.getFlowId()
          + " doesn't exist in " + exflow.getProjectId());
      return;
    }

    ExecutionOptions options = exflow.getExecutionOptions();

    ret.put("successEmails", options.getSuccessEmails());
    ret.put("failureEmails", options.getFailureEmails());
    ret.put("flowParam", options.getFlowParameters());

    FailureAction action = options.getFailureAction();
    String failureAction = null;
    switch (action) {
    case FINISH_CURRENTLY_RUNNING:
      failureAction = "finishCurrent";
      break;
    case CANCEL_ALL:
      failureAction = "cancelImmediately";
      break;
    case FINISH_ALL_POSSIBLE:
      failureAction = "finishPossible";
      break;
    }
    ret.put("failureAction", failureAction);

    ret.put("notifyFailureFirst", options.getNotifyOnFirstFailure());
    ret.put("notifyFailureLast", options.getNotifyOnLastFailure());

    ret.put("failureEmailsOverride", options.isFailureEmailsOverridden());
    ret.put("successEmailsOverride", options.isSuccessEmailsOverridden());

    ret.put("concurrentOptions", options.getConcurrentOption());
    ret.put("pipelineLevel", options.getPipelineLevel());
    ret.put("pipelineExecution", options.getPipelineExecutionId());
    ret.put("queueLevel", options.getQueueLevel());

    HashMap<String, String> nodeStatus = new HashMap<String, String>();
    for (ExecutableNode node : exflow.getExecutableNodes()) {
      nodeStatus.put(node.getId(), node.getStatus().toString());
    }
    ret.put("nodeStatus", nodeStatus);
    ret.put("disabled", options.getDisabledJobs());
  }

  private void ajaxCancelFlow(HttpServletRequest req, HttpServletResponse resp,
      HashMap<String, Object> ret, User user, ExecutableFlow exFlow)
      throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user,
            Type.EXECUTE);
    if (project == null) {
      return;
    }

    try {
      executorManager.cancelFlow(exFlow, user.getUserId());
    } catch (ExecutorManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxGetFlowRunning(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      String projectId, String flowId) throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, projectId, user, Type.EXECUTE);
    if (project == null) {
      return;
    }

    List<Integer> refs =
        executorManager.getRunningFlows(project.getId(), flowId);
    if (!refs.isEmpty()) {
      ret.put("execIds", refs);
    }
  }

  private void ajaxPauseFlow(HttpServletRequest req, HttpServletResponse resp,
      HashMap<String, Object> ret, User user, ExecutableFlow exFlow)
      throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user,
            Type.EXECUTE);
    if (project == null) {
      return;
    }

    try {
      executorManager.pauseFlow(exFlow, user.getUserId());
    } catch (ExecutorManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxResumeFlow(HttpServletRequest req, HttpServletResponse resp,
      HashMap<String, Object> ret, User user, ExecutableFlow exFlow)
      throws ServletException {
    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user,
            Type.EXECUTE);
    if (project == null) {
      return;
    }

    try {
      executorManager.resumeFlow(exFlow, user.getUserId());
    } catch (ExecutorManagerException e) {
      ret.put("resume", e.getMessage());
    }
  }

  private Map<String, Object> getExecutableFlowUpdateInfo(ExecutableNode node,
      long lastUpdateTime) {
    HashMap<String, Object> nodeObj = new HashMap<String, Object>();
    if (node instanceof ExecutableFlowBase) {
      ExecutableFlowBase base = (ExecutableFlowBase) node;
      ArrayList<Map<String, Object>> nodeList =
          new ArrayList<Map<String, Object>>();

      for (ExecutableNode subNode : base.getExecutableNodes()) {
        Map<String, Object> subNodeObj =
            getExecutableFlowUpdateInfo(subNode, lastUpdateTime);
        if (!subNodeObj.isEmpty()) {
          nodeList.add(subNodeObj);
        }
      }

      if (!nodeList.isEmpty()) {
        nodeObj.put("flow", base.getFlowId());
        nodeObj.put("nodes", nodeList);
      }
    }

    if (node.getUpdateTime() > lastUpdateTime || !nodeObj.isEmpty()) {
      nodeObj.put("id", node.getId());
      nodeObj.put("status", node.getStatus());
      nodeObj.put("startTime", node.getStartTime());
      nodeObj.put("endTime", node.getEndTime());
      nodeObj.put("updateTime", node.getUpdateTime());

      nodeObj.put("attempt", node.getAttempt());
      if (node.getAttempt() > 0) {
        nodeObj.put("pastAttempts", node.getAttemptObjects());
      }
    }

    return nodeObj;
  }

  private Map<String, Object> getExecutableNodeInfo(ExecutableNode node) {
    HashMap<String, Object> nodeObj = new HashMap<String, Object>();
    nodeObj.put("id", node.getId());
    nodeObj.put("status", node.getStatus());
    nodeObj.put("startTime", node.getStartTime());
    nodeObj.put("endTime", node.getEndTime());
    nodeObj.put("updateTime", node.getUpdateTime());
    nodeObj.put("type", node.getType());
    nodeObj.put("nestedId", node.getNestedId());

    nodeObj.put("attempt", node.getAttempt());
    if (node.getAttempt() > 0) {
      nodeObj.put("pastAttempts", node.getAttemptObjects());
    }

    if (node.getInNodes() != null && !node.getInNodes().isEmpty()) {
      nodeObj.put("in", node.getInNodes());
    }

    if (node instanceof ExecutableFlowBase) {
      ExecutableFlowBase base = (ExecutableFlowBase) node;
      ArrayList<Map<String, Object>> nodeList =
          new ArrayList<Map<String, Object>>();

      for (ExecutableNode subNode : base.getExecutableNodes()) {
        Map<String, Object> subNodeObj = getExecutableNodeInfo(subNode);
        if (!subNodeObj.isEmpty()) {
          nodeList.add(subNodeObj);
        }
      }

      nodeObj.put("flow", base.getFlowId());
      nodeObj.put("nodes", nodeList);
      nodeObj.put("flowId", base.getFlowId());
    }

    return nodeObj;
  }

  private void ajaxFetchExecutableFlowUpdate(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      ExecutableFlow exFlow) throws ServletException {
    Long lastUpdateTime = Long.parseLong(getParam(req, "lastUpdateTime"));
    System.out.println("Fetching " + exFlow.getExecutionId());

    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    Map<String, Object> map =
        getExecutableFlowUpdateInfo(exFlow, lastUpdateTime);
    map.put("status", exFlow.getStatus());
    map.put("startTime", exFlow.getStartTime());
    map.put("endTime", exFlow.getEndTime());
    map.put("updateTime", exFlow.getUpdateTime());
    ret.putAll(map);
  }

  private void ajaxFetchExecutableFlow(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user,
      ExecutableFlow exFlow) throws ServletException {
    System.out.println("Fetching " + exFlow.getExecutionId());

    Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    ret.put("submitTime", exFlow.getSubmitTime());
    ret.put("submitUser", exFlow.getSubmitUser());
    ret.put("execid", exFlow.getExecutionId());
    ret.put("projectId", exFlow.getProjectId());
    ret.put("project", project.getName());

    Map<String, Object> flowObj = getExecutableNodeInfo(exFlow);
    ret.putAll(flowObj);
  }

  private void ajaxAttemptExecuteFlow(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user)
      throws ServletException {
    String projectName = getParam(req, "project");
    String flowId = getParam(req, "flow");

    Project project =
        getProjectAjaxByPermission(ret, projectName, user, Type.EXECUTE);
    if (project == null) {
      ret.put("error", "Project '" + projectName + "' doesn't exist.");
      return;
    }

    ret.put("flow", flowId);
    Flow flow = project.getFlow(flowId);
    if (flow == null) {
      ret.put("error", "Flow '" + flowId + "' cannot be found in project "
          + project);
      return;
    }

    ajaxExecuteFlow(req, resp, ret, user);
  }

  private void ajaxExecuteFlow(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret, User user)
      throws ServletException {
    String projectName = getParam(req, "project");
    String flowId = getParam(req, "flow");

    Project project =
        getProjectAjaxByPermission(ret, projectName, user, Type.EXECUTE);
    if (project == null) {
      ret.put("error", "Project '" + projectName + "' doesn't exist.");
      return;
    }

    ret.put("flow", flowId);
    Flow flow = project.getFlow(flowId);
    if (flow == null) {
      ret.put("error", "Flow '" + flowId + "' cannot be found in project "
          + project);
      return;
    }

    ExecutableFlow exflow = new ExecutableFlow(project, flow);
    exflow.setSubmitUser(user.getUserId());
    exflow.addAllProxyUsers(project.getProxyUsers());

    ExecutionOptions options = HttpRequestUtils.parseFlowOptions(req);
    exflow.setExecutionOptions(options);
    if (!options.isFailureEmailsOverridden()) {
      options.setFailureEmails(flow.getFailureEmails());
    }
    if (!options.isSuccessEmailsOverridden()) {
      options.setSuccessEmails(flow.getSuccessEmails());
    }
    options.setMailCreator(flow.getMailCreator());

    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(userManager, options, user);
      String message =
          executorManager.submitExecutableFlow(exflow, user.getUserId());
      ret.put("message", message);
    } catch (Exception e) {
      e.printStackTrace();
      ret.put("error",
          "Error submitting flow " + exflow.getFlowId() + ". " + e.getMessage());
    }

    ret.put("execid", exflow.getExecutionId());
  }

  public class ExecutorVelocityHelper {
    public String getProjectName(int id) {
      Project project = projectManager.getProject(id);
      if (project == null) {
        return String.valueOf(id);
      }

      return project.getName();
    }
  }
}
