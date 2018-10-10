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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.Constants;
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
import azkaban.flow.FlowUtils;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.flowtrigger.TriggerInstance;
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
import azkaban.utils.ExternalLinkUtils;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.WebMetrics;
import azkaban.webapp.plugin.PluginRegistry;
import azkaban.webapp.plugin.ViewerPlugin;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutorServlet extends LoginAbstractAzkabanServlet {

  private static final Logger logger = LoggerFactory.getLogger(ExecutorServlet.class.getName());
  private static final long serialVersionUID = 1L;
  private WebMetrics webMetrics;
  private ProjectManager projectManager;
  private FlowTriggerService flowTriggerService;
  private ExecutorManagerAdapter executorManager;
  private ScheduleManager scheduleManager;
  private UserManager userManager;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.userManager = server.getUserManager();
    this.projectManager = server.getProjectManager();
    this.executorManager = server.getExecutorManager();
    this.scheduleManager = server.getScheduleManager();
    this.flowTriggerService = server.getFlowTriggerService();
    // TODO: reallocf fully guicify
    this.webMetrics = SERVICE_PROVIDER.getInstance(WebMetrics.class);
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else if (hasParam(req, "execid")) {
      if (hasParam(req, "job")) {
        handleExecutionJobDetailsPage(req, resp, session);
      } else {
        handleExecutionFlowPageByExecId(req, resp, session);
      }
    } else if (hasParam(req, "triggerinstanceid")) {
      handleExecutionFlowPageByTriggerInstanceId(req, resp, session);
    } else {
      handleExecutionsPage(req, resp, session);
    }
  }

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");

    if (hasParam(req, "execid")) {
      final int execid = getIntParam(req, "execid");
      ExecutableFlow exFlow = null;

      try {
        exFlow = this.executorManager.getExecutableFlow(execid);
      } catch (final ExecutorManagerException e) {
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
    } else if (ajaxName.equals("fetchscheduledflowgraph")) {
      final String projectName = getParam(req, "project");
      final String flowName = getParam(req, "flow");
      ajaxFetchScheduledFlowGraph(projectName, flowName, ret, session.getUser());
    } else if (ajaxName.equals("reloadExecutors")) {
      ajaxReloadExecutors(req, resp, ret, session.getUser());
    } else if (ajaxName.equals("enableQueueProcessor")) {
      ajaxUpdateQueueProcessor(req, resp, ret, session.getUser(), true);
    } else if (ajaxName.equals("disableQueueProcessor")) {
      ajaxUpdateQueueProcessor(req, resp, ret, session.getUser(), false);
    } else if (ajaxName.equals("getRunning")) {
      final String projectName = getParam(req, "project");
      final String flowName = getParam(req, "flow");
      ajaxGetFlowRunning(req, resp, ret, session.getUser(), projectName,
          flowName);
    } else if (ajaxName.equals("flowInfo")) {
      final String projectName = getParam(req, "project");
      final String flowName = getParam(req, "flow");
      ajaxFetchFlowInfo(req, resp, ret, session.getUser(), projectName,
          flowName);
    } else {
      final String projectName = getParam(req, "project");

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
  private void ajaxUpdateQueueProcessor(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> returnMap, final User user,
      final boolean enableQueue) {
    boolean wasSuccess = false;
    if (HttpRequestUtils.hasPermission(this.userManager, user, Type.ADMIN)) {
      try {
        if (enableQueue) {
          this.executorManager.enableQueueProcessorThread();
        } else {
          this.executorManager.disableQueueProcessorThread();
        }
        returnMap.put(ConnectorParams.STATUS_PARAM,
            ConnectorParams.RESPONSE_SUCCESS);
        wasSuccess = true;
      } catch (final ExecutorManagerException e) {
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

  private void ajaxFetchScheduledFlowGraph(final String projectName, final String flowName,
      final HashMap<String, Object> ret, final User user) throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, projectName, user, Type.EXECUTE);
    if (project == null) {
      ret.put("error", "Project '" + projectName + "' doesn't exist.");
      return;
    }
    try {
      final Schedule schedule = this.scheduleManager.getSchedule(project.getId(), flowName);
      final ExecutionOptions executionOptions =
          schedule != null ? schedule.getExecutionOptions() : new ExecutionOptions();
      final Flow flow = project.getFlow(flowName);
      if (flow == null) {
        ret.put("error", "Flow '" + flowName + "' cannot be found in project " + project);
        return;
      }
      final ExecutableFlow exFlow = new ExecutableFlow(project, flow);
      exFlow.setExecutionOptions(executionOptions);
      ret.put("submitTime", exFlow.getSubmitTime());
      ret.put("submitUser", exFlow.getSubmitUser());
      ret.put("execid", exFlow.getExecutionId());
      ret.put("projectId", exFlow.getProjectId());
      ret.put("project", project.getName());
      FlowUtils.applyDisabledJobs(executionOptions.getDisabledJobs(), exFlow);
      final Map<String, Object> flowObj = getExecutableNodeInfo(exFlow);
      ret.putAll(flowObj);
    } catch (final ScheduleManagerException ex) {
      throw new ServletException(ex);
    }
  }

  /* Reloads executors from DB and azkaban.properties via executorManager */
  private void ajaxReloadExecutors(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> returnMap, final User user) {
    boolean wasSuccess = false;
    if (HttpRequestUtils.hasPermission(this.userManager, user, Type.ADMIN)) {
      try {
        this.executorManager.setupExecutors();
        returnMap.put(ConnectorParams.STATUS_PARAM,
            ConnectorParams.RESPONSE_SUCCESS);
        wasSuccess = true;
      } catch (final ExecutorManagerException e) {
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
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    }
  }

  private void handleExecutionJobDetailsPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/jobdetailspage.vm");
    final User user = session.getUser();
    final int execId = getIntParam(req, "execid");
    final String jobId = getParam(req, "job");
    final int attempt = getIntParam(req, "attempt", 0);
    page.add("execid", execId);
    page.add("jobid", jobId);
    page.add("attempt", attempt);

    ExecutableFlow flow = null;
    ExecutableNode node = null;
    final String jobLinkUrl;
    try {
      flow = this.executorManager.getExecutableFlow(execId);
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

      jobLinkUrl = this.executorManager.getJobLinkUrl(flow, jobId, attempt);

      final List<ViewerPlugin> jobViewerPlugins =
          PluginRegistry.getRegistry().getViewerPluginsForJobType(
              node.getType());
      page.add("jobViewerPlugins", jobViewerPlugins);
    } catch (final ExecutorManagerException e) {
      page.add("errorMsg", "Error loading executing flow: " + e.getMessage());
      page.render();
      return;
    }

    final int projectId = flow.getProjectId();
    final Project project =
        getProjectPageByPermission(page, projectId, user, Type.READ);
    if (project == null) {
      page.render();
      return;
    }

    page.add("projectName", project.getName());
    page.add("flowid", flow.getId());
    page.add("parentflowid", node.getParentFlow().getFlowId());
    page.add("jobname", node.getId());
    page.add("jobLinkUrl", jobLinkUrl);
    page.add("jobType", node.getType());

    if (node.getStatus() == Status.FAILED || node.getStatus() == Status.KILLED) {
      page.add("jobFailed", true);
    } else {
      page.add("jobFailed", false);
    }

    page.render();
  }

  private void handleExecutionsPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/executionspage.vm");

    final List<Pair<ExecutableFlow, Optional<Executor>>> runningFlows =
        this.executorManager.getActiveFlowsWithExecutor();
    page.add("runningFlows", runningFlows.isEmpty() ? null : runningFlows);

    final List<ExecutableFlow> finishedFlows =
        this.executorManager.getRecentlyFinishedFlows();
    page.add("recentlyFinished", finishedFlows.isEmpty() ? null : finishedFlows);
    page.add("vmutils", new VelocityUtil(this.projectManager));
    page.render();
  }

  private void handleExecutionFlowPageByTriggerInstanceId(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/executingflowpage.vm");
    final User user = session.getUser();
    final String triggerInstanceId = getParam(req, "triggerinstanceid");

    final TriggerInstance triggerInst = this.flowTriggerService
        .findTriggerInstanceById(triggerInstanceId);

    if (triggerInst == null) {
      page.add("errorMsg", "Error loading trigger instance " + triggerInstanceId
          + " not found.");
      page.render();
      return;
    }

    page.add("triggerInstanceId", triggerInstanceId);
    page.add("execid", triggerInst.getFlowExecId());

    final int projectId = triggerInst.getProject().getId();
    final Project project =
        getProjectPageByPermission(page, projectId, user, Type.READ);

    if (project == null) {
      page.render();
      return;
    }

    addExternalLinkLabel(req, page);

    page.add("projectId", project.getId());
    page.add("projectName", project.getName());
    page.add("flowid", triggerInst.getFlowId());

    page.render();
  }

  private void addExternalLinkLabel(final HttpServletRequest req, final Page page) {
    final Props props = getApplication().getServerProps();
    final String execExternalLinkURL = ExternalLinkUtils.getExternalAnalyzerOnReq(props, req);

    if (execExternalLinkURL.length() > 0) {
      page.add("executionExternalLinkURL", execExternalLinkURL);
      logger.debug("Added an External analyzer to the page");
      logger.debug("External analyzer url: " + execExternalLinkURL);

      final String execExternalLinkLabel =
          props.getString(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_LABEL,
              "External Analyzer");
      page.add("executionExternalLinkLabel", execExternalLinkLabel);
      logger.debug("External analyzer label set to : " + execExternalLinkLabel);
    }
  }

  private void handleExecutionFlowPageByExecId(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/executingflowpage.vm");
    final User user = session.getUser();
    final int execId = getIntParam(req, "execid");
    page.add("execid", execId);
    page.add("triggerInstanceId", "-1");

    ExecutableFlow flow = null;
    try {
      flow = this.executorManager.getExecutableFlow(execId);
      if (flow == null) {
        page.add("errorMsg", "Error loading executing flow " + execId
            + " not found.");
        page.render();
        return;
      }
    } catch (final ExecutorManagerException e) {
      page.add("errorMsg", "Error loading executing flow: " + e.getMessage());
      page.render();
      return;
    }

    final int projectId = flow.getProjectId();
    final Project project =
        getProjectPageByPermission(page, projectId, user, Type.READ);
    if (project == null) {
      page.render();
      return;
    }

    addExternalLinkLabel(req, page);

    page.add("projectId", project.getId());
    page.add("projectName", project.getName());
    page.add("flowid", flow.getFlowId());

    page.render();
  }

  protected Project getProjectPageByPermission(final Page page, final int projectId,
      final User user, final Permission.Type type) {
    final Project project = this.projectManager.getProject(projectId);

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

  protected Project getProjectAjaxByPermission(final Map<String, Object> ret,
      final String projectName, final User user, final Permission.Type type) {
    final Project project = this.projectManager.getProject(projectName);

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

  protected Project getProjectAjaxByPermission(final Map<String, Object> ret,
      final int projectId, final User user, final Permission.Type type) {
    final Project project = this.projectManager.getProject(projectId);

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

  private void ajaxRestartFailed(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final ExecutableFlow exFlow) throws ServletException {
    final Project project =
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
      this.executorManager.retryFailures(exFlow, user.getUserId());
    } catch (final ExecutorManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  /**
   * Gets the logs through plain text stream to reduce memory overhead.
   */
  private void ajaxFetchExecFlowLogs(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final ExecutableFlow exFlow) throws ServletException {
    final long startMs = System.currentTimeMillis();
    final Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    final int offset = this.getIntParam(req, "offset");
    final int length = this.getIntParam(req, "length");

    resp.setCharacterEncoding("utf-8");

    try {
      final LogData data =
          this.executorManager.getExecutableFlowLog(exFlow, offset, length);
      if (data == null) {
        ret.put("length", 0);
        ret.put("offset", offset);
        ret.put("data", "");
      } else {
        ret.put("length", data.getLength());
        ret.put("offset", data.getOffset());
        ret.put("data", StringEscapeUtils.escapeHtml(data.getData()));
      }
    } catch (final ExecutorManagerException e) {
      throw new ServletException(e);
    }

    /*
     * We originally consider leverage Drop Wizard's Timer API {@link com.codahale.metrics.Timer}
     * to measure the duration time.
     * However, Timer will result in too many accompanying metrics (e.g., min, max, 99th quantile)
     * regarding one metrics. We decided to use gauge to do that and monitor how it behaves.
     */
    this.webMetrics.setFetchLogLatency(System.currentTimeMillis() - startMs);
  }

  /**
   * Gets the logs through ajax plain text stream to reduce memory overhead.
   */
  private void ajaxFetchJobLogs(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final ExecutableFlow exFlow) throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    final int offset = this.getIntParam(req, "offset");
    final int length = this.getIntParam(req, "length");

    final String jobId = this.getParam(req, "jobId");
    resp.setCharacterEncoding("utf-8");

    try {
      final ExecutableNode node = exFlow.getExecutableNodePath(jobId);
      if (node == null) {
        ret.put("error",
            "Job " + jobId + " doesn't exist in " + exFlow.getExecutionId());
        return;
      }

      final int attempt = this.getIntParam(req, "attempt", node.getAttempt());
      final LogData data =
          this.executorManager.getExecutionJobLog(exFlow, jobId, offset, length,
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
    } catch (final ExecutorManagerException e) {
      throw new ServletException(e);
    }
  }

  private void ajaxFetchJobStats(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final ExecutableFlow exFlow) throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    final String jobId = this.getParam(req, "jobid");
    resp.setCharacterEncoding("utf-8");

    try {
      final ExecutableNode node = exFlow.getExecutableNodePath(jobId);
      if (node == null) {
        ret.put("error",
            "Job " + jobId + " doesn't exist in " + exFlow.getExecutionId());
        return;
      }

      final List<Object> jsonObj =
          this.executorManager
              .getExecutionJobStats(exFlow, jobId, node.getAttempt());
      ret.put("jobStats", jsonObj);
    } catch (final ExecutorManagerException e) {
      ret.put("error", "Error retrieving stats for job " + jobId);
      return;
    }
  }

  private void ajaxFetchFlowInfo(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final String projectName, final String flowId) throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, projectName, user, Type.READ);
    if (project == null) {
      return;
    }

    final Flow flow = project.getFlow(flowId);
    if (flow == null) {
      ret.put("error", "Error loading flow. Flow " + flowId
          + " doesn't exist in " + projectName);
      return;
    }

    ret.put("successEmails", flow.getSuccessEmails());
    ret.put("failureEmails", flow.getFailureEmails());

    Schedule sflow = null;
    try {
      for (final Schedule sched : this.scheduleManager.getSchedules()) {
        if (sched.getProjectId() == project.getId()
            && sched.getFlowName().equals(flowId)) {
          sflow = sched;
          break;
        }
      }
    } catch (final ScheduleManagerException e) {
      // TODO Auto-generated catch block
      throw new ServletException(e);
    }

    if (sflow != null) {
      ret.put("scheduled", sflow.getNextExecTime());
    }
  }

  private void ajaxFetchExecutableFlowInfo(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final ExecutableFlow exflow) throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, exflow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    final Flow flow = project.getFlow(exflow.getFlowId());
    if (flow == null) {
      ret.put("error", "Error loading flow. Flow " + exflow.getFlowId()
          + " doesn't exist in " + exflow.getProjectId());
      return;
    }

    final ExecutionOptions options = exflow.getExecutionOptions();

    ret.put("successEmails", options.getSuccessEmails());
    ret.put("failureEmails", options.getFailureEmails());
    ret.put("flowParam", options.getFlowParameters());

    final FailureAction action = options.getFailureAction();
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

    final HashMap<String, String> nodeStatus = new HashMap<>();
    for (final ExecutableNode node : exflow.getExecutableNodes()) {
      nodeStatus.put(node.getId(), node.getStatus().toString());
    }
    ret.put("nodeStatus", nodeStatus);
    ret.put("disabled", options.getDisabledJobs());
  }

  private void ajaxCancelFlow(final HttpServletRequest req, final HttpServletResponse resp,
      final HashMap<String, Object> ret, final User user, final ExecutableFlow exFlow)
      throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user,
            Type.EXECUTE);
    if (project == null) {
      return;
    }

    try {
      this.executorManager.cancelFlow(exFlow, user.getUserId());
    } catch (final ExecutorManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxGetFlowRunning(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final String projectId, final String flowId) throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, projectId, user, Type.EXECUTE);
    if (project == null) {
      return;
    }

    final List<Integer> refs =
        this.executorManager.getRunningFlows(project.getId(), flowId);
    if (!refs.isEmpty()) {
      ret.put("execIds", refs);
    }
  }

  private void ajaxPauseFlow(final HttpServletRequest req, final HttpServletResponse resp,
      final HashMap<String, Object> ret, final User user, final ExecutableFlow exFlow)
      throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user,
            Type.EXECUTE);
    if (project == null) {
      return;
    }

    try {
      this.executorManager.pauseFlow(exFlow, user.getUserId());
    } catch (final ExecutorManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxResumeFlow(final HttpServletRequest req, final HttpServletResponse resp,
      final HashMap<String, Object> ret, final User user, final ExecutableFlow exFlow)
      throws ServletException {
    final Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user,
            Type.EXECUTE);
    if (project == null) {
      return;
    }

    try {
      this.executorManager.resumeFlow(exFlow, user.getUserId());
    } catch (final ExecutorManagerException e) {
      ret.put("resume", e.getMessage());
    }
  }

  private Map<String, Object> getExecutableFlowUpdateInfo(final ExecutableNode node,
      final long lastUpdateTime) {
    final HashMap<String, Object> nodeObj = new HashMap<>();
    if (node instanceof ExecutableFlowBase) {
      final ExecutableFlowBase base = (ExecutableFlowBase) node;
      final ArrayList<Map<String, Object>> nodeList =
          new ArrayList<>();

      for (final ExecutableNode subNode : base.getExecutableNodes()) {
        final Map<String, Object> subNodeObj =
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

  private Map<String, Object> getExecutableNodeInfo(final ExecutableNode node) {
    final HashMap<String, Object> nodeObj = new HashMap<>();
    nodeObj.put("id", node.getId());
    nodeObj.put("status", node.getStatus());
    nodeObj.put("startTime", node.getStartTime());
    nodeObj.put("endTime", node.getEndTime());
    nodeObj.put("updateTime", node.getUpdateTime());
    nodeObj.put("type", node.getType());
    nodeObj.put("condition", node.getCondition());
    nodeObj.put("nestedId", node.getNestedId());

    nodeObj.put("attempt", node.getAttempt());
    if (node.getAttempt() > 0) {
      nodeObj.put("pastAttempts", node.getAttemptObjects());
    }

    if (node.getInNodes() != null && !node.getInNodes().isEmpty()) {
      nodeObj.put("in", node.getInNodes());
    }

    if (node instanceof ExecutableFlowBase) {
      final ExecutableFlowBase base = (ExecutableFlowBase) node;
      final ArrayList<Map<String, Object>> nodeList =
          new ArrayList<>();

      for (final ExecutableNode subNode : base.getExecutableNodes()) {
        final Map<String, Object> subNodeObj = getExecutableNodeInfo(subNode);
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

  private void ajaxFetchExecutableFlowUpdate(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final ExecutableFlow exFlow) throws ServletException {
    final Long lastUpdateTime = Long.parseLong(getParam(req, "lastUpdateTime"));
    logger.info("Fetching " + exFlow.getExecutionId());

    final Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    final Map<String, Object> map =
        getExecutableFlowUpdateInfo(exFlow, lastUpdateTime);
    map.put("status", exFlow.getStatus());
    map.put("startTime", exFlow.getStartTime());
    map.put("endTime", exFlow.getEndTime());
    map.put("updateTime", exFlow.getUpdateTime());
    ret.putAll(map);
  }

  private void ajaxFetchExecutableFlow(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final ExecutableFlow exFlow) throws ServletException {
    logger.info("Fetching " + exFlow.getExecutionId());

    final Project project =
        getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    ret.put("submitTime", exFlow.getSubmitTime());
    ret.put("submitUser", exFlow.getSubmitUser());
    ret.put("execid", exFlow.getExecutionId());
    ret.put("projectId", exFlow.getProjectId());
    ret.put("project", project.getName());

    final Map<String, Object> flowObj = getExecutableNodeInfo(exFlow);
    ret.putAll(flowObj);
  }

  private void ajaxAttemptExecuteFlow(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user)
      throws ServletException {
    final String projectName = getParam(req, "project");
    final String flowId = getParam(req, "flow");

    final Project project =
        getProjectAjaxByPermission(ret, projectName, user, Type.EXECUTE);
    if (project == null) {
      ret.put("error", "Project '" + projectName + "' doesn't exist.");
      return;
    }

    ret.put("flow", flowId);
    final Flow flow = project.getFlow(flowId);
    if (flow == null) {
      ret.put("error", "Flow '" + flowId + "' cannot be found in project "
          + project);
      return;
    }

    ajaxExecuteFlow(req, resp, ret, user);
  }

  private void ajaxExecuteFlow(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user)
      throws ServletException {
    final String projectName = getParam(req, "project");
    final String flowId = getParam(req, "flow");

    final Project project =
        getProjectAjaxByPermission(ret, projectName, user, Type.EXECUTE);
    if (project == null) {
      ret.put("error", "Project '" + projectName + "' doesn't exist.");
      return;
    }

    ret.put("flow", flowId);
    final Flow flow = project.getFlow(flowId);
    if (flow == null) {
      ret.put("error", "Flow '" + flowId + "' cannot be found in project "
          + project);
      return;
    }

    final ExecutableFlow exflow = FlowUtils.createExecutableFlow(project, flow);
    exflow.setSubmitUser(user.getUserId());

    final ExecutionOptions options = HttpRequestUtils.parseFlowOptions(req);
    exflow.setExecutionOptions(options);
    if (!options.isFailureEmailsOverridden()) {
      options.setFailureEmails(flow.getFailureEmails());
    }
    if (!options.isSuccessEmailsOverridden()) {
      options.setSuccessEmails(flow.getSuccessEmails());
    }
    options.setMailCreator(flow.getMailCreator());

    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(this.userManager, options, user);
      final String message =
          this.executorManager.submitExecutableFlow(exflow, user.getUserId());
      ret.put("message", message);
    } catch (final Exception e) {
      e.printStackTrace();
      ret.put("error",
          "Error submitting flow " + exflow.getFlowId() + ". " + e.getMessage());
    }

    ret.put("execid", exflow.getExecutionId());
  }
}
