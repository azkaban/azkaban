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

import azkaban.Constants;
import azkaban.Constants.EventReporterConstants;
import azkaban.Constants.FlowParameters;
import azkaban.executor.ClusterInfo;
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
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.flowtrigger.TriggerInstance;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.server.AzkabanAPI;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.ExternalLink;
import azkaban.utils.ExternalLinkUtils;
import azkaban.utils.ExternalLinkUtils.ExternalLinkScope;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.plugin.PluginRegistry;
import azkaban.webapp.plugin.ViewerPlugin;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang.StringEscapeUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutorServlet extends LoginAbstractAzkabanServlet {

  private static final String API_FETCH_EXEC_FLOW = "fetchexecflow";
  private static final String API_FETCH_EXEC_FLOW_UPDATE = "fetchexecflowupdate";
  private static final String API_CANCEL_FLOW = "cancelFlow";
  private static final String API_PAUSE_FLOW = "pauseFlow";
  private static final String API_RESUME_FLOW = "resumeFlow";
  private static final String API_FETCH_EXEC_FLOW_LOGS = "fetchExecFlowLogs";
  private static final String API_FETCH_EXEC_JOB_LOGS = "fetchExecJobLogs";
  private static final String API_FETCH_EXEC_JOB_STATS = "fetchExecJobStats";
  private static final String API_RETRY_FAILED_JOBS = "retryFailedJobs";
  private static final String API_FLOW_INFO = "flowInfo";
  private static final String API_FETCH_SCHEDULED_FLOW_GRAPH = "fetchscheduledflowgraph";
  private static final String API_RELOAD_EXECUTORS = "reloadExecutors";
  private static final String API_ENABLE_QUEUE_PROCESSOR = "enableQueueProcessor";
  private static final String API_DISABLE_QUEUE_PROCESSOR = "disableQueueProcessor";
  private static final String API_GET_RUNNING = "getRunning";
  private static final String API_EXECUTE_FLOW = "executeFlow";
  private static final String API_RAMP = "ramp";
  private static final String API_UPDATE_PROP = "updateProp";

  private static final Logger logger = LoggerFactory.getLogger(ExecutorServlet.class.getName());
  private static final long serialVersionUID = 1L;
  private ProjectManager projectManager;
  private FlowTriggerService flowTriggerService;
  private ExecutorManagerAdapter executorManagerAdapter;
  private ContainerizedImpl containerizedImpl;
  private ScheduleManager scheduleManager;
  private UserManager userManager;

  private List<ExternalLink> flowLevelExternalLinks;
  private List<ExternalLink> jobLevelExternalLinks;
  private int externalLinksTimeoutMs;

  public ExecutorServlet() {
    super(createAPIEndpoints());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = getApplication();
    this.userManager = server.getUserManager();
    this.projectManager = server.getProjectManager();
    this.executorManagerAdapter = server.getExecutorManager();
    if (this.executorManagerAdapter instanceof ContainerizedDispatchManager) {
      this.containerizedImpl =
          ((ContainerizedDispatchManager) this.executorManagerAdapter).getContainerizedImpl();
    }
    this.scheduleManager = server.getScheduleManager();
    this.flowTriggerService = server.getFlowTriggerService();

    final Props azkProps = server.getServerProps();
    this.flowLevelExternalLinks = ExternalLinkUtils.parseExternalLinks(azkProps, ExternalLinkScope.FLOW);
    this.jobLevelExternalLinks = ExternalLinkUtils.parseExternalLinks(azkProps, ExternalLinkScope.JOB);
    this.externalLinksTimeoutMs =
        azkProps.getInt(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TIMEOUT_MS,
            Constants.DEFAULT_AZKABAN_SERVER_EXTERNAL_ANALYZER_TIMEOUT_MS);

  }

  private static List<AzkabanAPI> createAPIEndpoints() {
    final List<AzkabanAPI> apiEndpoints = new ArrayList<>();
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_EXEC_FLOW));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_EXEC_FLOW_UPDATE));
    apiEndpoints.add(new AzkabanAPI("ajax", API_CANCEL_FLOW));
    apiEndpoints.add(new AzkabanAPI("ajax", API_PAUSE_FLOW));
    apiEndpoints.add(new AzkabanAPI("ajax", API_RESUME_FLOW));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_EXEC_FLOW_LOGS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_EXEC_JOB_LOGS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_EXEC_JOB_STATS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_RETRY_FAILED_JOBS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FLOW_INFO));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_SCHEDULED_FLOW_GRAPH));
    apiEndpoints.add(new AzkabanAPI("ajax", API_RELOAD_EXECUTORS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_ENABLE_QUEUE_PROCESSOR));
    apiEndpoints.add(new AzkabanAPI("ajax", API_DISABLE_QUEUE_PROCESSOR));
    apiEndpoints.add(new AzkabanAPI("ajax", API_GET_RUNNING));
    apiEndpoints.add(new AzkabanAPI("ajax", API_EXECUTE_FLOW));
    apiEndpoints.add(new AzkabanAPI("ajax", API_RAMP));
    apiEndpoints.add(new AzkabanAPI("ajax", API_UPDATE_PROP));
    return apiEndpoints;
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

  private void handleAJAXAction(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");

    if (hasParam(req, "execid")) {
      final int execid = getIntParam(req, "execid");
      ExecutableFlow exFlow = null;

      try {
        exFlow = this.executorManagerAdapter.getExecutableFlow(execid);
      } catch (final ExecutorManagerException e) {
        ret.put("error",
            "Error fetching execution '" + execid + "': " + e.getMessage());
      }

      if (exFlow == null) {
        ret.put("error", "Cannot find execution '" + execid + "'");
      } else {
        if (API_FETCH_EXEC_FLOW.equals(ajaxName)) {
          ajaxFetchExecutableFlow(req, resp, ret, session.getUser(), exFlow);
        } else if (API_FETCH_EXEC_FLOW_UPDATE.equals(ajaxName)) {
          ajaxFetchExecutableFlowUpdate(req, resp, ret, session.getUser(),
              exFlow);
        } else if (API_CANCEL_FLOW.equals(ajaxName)) {
          ajaxCancelFlow(req, resp, ret, session.getUser(), exFlow);
        } else if (API_PAUSE_FLOW.equals(ajaxName)) {
          ajaxPauseFlow(req, resp, ret, session.getUser(), exFlow);
        } else if (API_RESUME_FLOW.equals(ajaxName)) {
          ajaxResumeFlow(req, resp, ret, session.getUser(), exFlow);
        } else if (API_FETCH_EXEC_FLOW_LOGS.equals(ajaxName)) {
          ajaxFetchExecFlowLogs(req, resp, ret, session.getUser(), exFlow);
        } else if (API_FETCH_EXEC_JOB_LOGS.equals(ajaxName)) {
          ajaxFetchJobLogs(req, resp, ret, session.getUser(), exFlow);
        } else if (API_FETCH_EXEC_JOB_STATS.equals(ajaxName)) {
          ajaxFetchJobStats(req, resp, ret, session.getUser(), exFlow);
        } else if (API_RETRY_FAILED_JOBS.equals(ajaxName)) {
          ajaxRestartFailed(req, resp, ret, session.getUser(), exFlow);
        } else if (API_FLOW_INFO.equals(ajaxName)) {
          ajaxFetchExecutableFlowInfo(req, resp, ret, session.getUser(), exFlow);
        }
      }
    } else if (API_UPDATE_PROP.equals(ajaxName)) {
      ajaxUpdateProperty(req, resp, ret, session.getUser());
    } else if (API_RAMP.equals(ajaxName)) {
      ajaxRampActions(req, resp, ret, session.getUser());
    } else if (API_FETCH_SCHEDULED_FLOW_GRAPH.equals(ajaxName)) {
      final String projectName = getParam(req, "project");
      final String flowName = getParam(req, "flow");
      ajaxFetchScheduledFlowGraph(projectName, flowName, ret, session.getUser());
    } else if (API_RELOAD_EXECUTORS.equals(ajaxName)) {
      ajaxReloadExecutors(req, resp, ret, session.getUser());
    } else if (API_ENABLE_QUEUE_PROCESSOR.equals(ajaxName)) {
      ajaxUpdateQueueProcessor(req, resp, ret, session.getUser(), true);
    } else if (API_DISABLE_QUEUE_PROCESSOR.equals(ajaxName)) {
      ajaxUpdateQueueProcessor(req, resp, ret, session.getUser(), false);
    } else if (API_GET_RUNNING.equals(ajaxName)) {
      final String projectName = getParam(req, "project");
      final String flowName = getParam(req, "flow");
      ajaxGetFlowRunning(req, resp, ret, session.getUser(), projectName, flowName);
    } else if (API_FLOW_INFO.equals(ajaxName)) {
      final String projectName = getParam(req, "project");
      final String flowName = getParam(req, "flow");
      ajaxFetchFlowInfo(req, resp, ret, session.getUser(), projectName, flowName);
    } else {
      final String projectName = getParam(req, "project");

      ret.put("project", projectName);
      if (API_EXECUTE_FLOW.equals(ajaxName)) {
        ajaxExecuteFlow(req, resp, ret, session.getUser());
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
          this.executorManagerAdapter.enableQueueProcessorThread();
        } else {
          this.executorManagerAdapter.disableQueueProcessorThread();
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
        this.executorManagerAdapter.setupExecutors();
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
    final Page page = newPage(req, resp, session,
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
    try {
      flow = this.executorManagerAdapter.getExecutableFlow(execId);
      if (flow == null) {
        page.add("errorMsg", "Error loading executing flow " + execId
            + ": not found.");
        page.render();
        return;
      }

      node = flow.getExecutableNodePath(jobId);
      if (node == null) {
        page.add("errorMsg", "Job " + jobId + " doesn't exist in " + flow.getExecutionId());
        return;
      }

      final List<ViewerPlugin> jobViewerPlugins = PluginRegistry.getRegistry()
          .getViewerPluginsForJobType(node.getType());
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

    final Map<String, String> jobLogUrlsByAppId = this.executorManagerAdapter
        .getExternalJobLogUrls(flow, jobId, attempt);
    page.add("jobLogUrlsByAppId", jobLogUrlsByAppId);

    addExternalLinks(this.jobLevelExternalLinks, page, req, execId, jobId);

    page.add("projectName", project.getName());
    page.add("flowid", flow.getId());
    page.add("flowlist", flow.getId().split(Constants.PATH_DELIMITER, 0));
    page.add("pathDelimiter", Constants.PATH_DELIMITER);
    page.add("parentflowid", node.getParentFlow().getFlowId());
    page.add("jobname", node.getId());
    page.add("jobType", node.getType());
    page.add("attemptStatus", attempt == node.getAttempt() ?
        node.getStatus() : node.getPastAttemptList().get(attempt).getStatus());
    page.add("pastAttempts", node.getAttempt() > 0 ?
        node.getPastAttemptList().size() : 0);
    page.add("jobFailed", node.getStatus() == Status.FAILED || node.getStatus() == Status.KILLED);

    page.render();
  }

  private void handleExecutionsPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/executionspage.vm");

    final List<Pair<ExecutableFlow, Optional<Executor>>> runningFlows =
        this.executorManagerAdapter.getActiveFlowsWithExecutor();
    page.add("runningFlows", runningFlows.isEmpty() ? null : runningFlows);

    final List<ExecutableFlow> finishedFlows =
        this.executorManagerAdapter.getRecentlyFinishedFlows();
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

    addExternalLinks(this.flowLevelExternalLinks, page, req, triggerInst.getFlowExecId(), null);

    page.add("projectId", project.getId());
    page.add("projectName", project.getName());
    page.add("flowid", triggerInst.getFlowId());
    page.add("flowlist", triggerInst.getFlowId().split(Constants.PATH_DELIMITER, 0));
    page.add("pathDelimiter", Constants.PATH_DELIMITER);

    page.render();
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
      flow = this.executorManagerAdapter.getExecutableFlow(execId);
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

    addExternalLinks(this.flowLevelExternalLinks, page, req, execId, null);

    page.add("projectId", project.getId());
    page.add("projectName", project.getName());
    page.add("flowid", flow.getFlowId());
    page.add("flowlist", flow.getFlowId().split(Constants.PATH_DELIMITER, 0));
    page.add("pathDelimiter", Constants.PATH_DELIMITER);

    // check the current flow definition to see if the flow is locked.
    final Flow currentFlow = project.getFlow(flow.getFlowId());
    boolean isCurrentFlowLocked = false;
    if (currentFlow != null) {
      isCurrentFlowLocked = currentFlow.isLocked();
    } else {
      logger.info("Flow {} not found in project {}.", flow.getFlowId(), project.getName());
    }
    page.add("isLocked", isCurrentFlowLocked);

    page.render();
  }

  private void addExternalLinks(final List<ExternalLink> extLinksTemplates, final Page page,
      final HttpServletRequest req, final int executionId, final String jobId) {
    List<ExternalLink> externalAnalyzers =
        ExternalLinkUtils.buildExternalLinksForRequest(extLinksTemplates,
            this.externalLinksTimeoutMs, req, executionId, jobId);
    page.add("externalAnalyzers", externalAnalyzers);
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
    return filterProjectByPermission(this.projectManager.getProject(projectName), user, type, ret);
  }

  protected Project getProjectAjaxByPermission(final Map<String, Object> ret,
      final int projectId, final User user, final Permission.Type type) {
    return filterProjectByPermission(this.projectManager.getProject(projectId), user, type, ret);
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
      this.executorManagerAdapter.retryFailures(exFlow, user.getUserId());
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
    final Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
    if (project == null) {
      return;
    }

    final int offset = this.getIntParam(req, "offset");
    final int length = this.getIntParam(req, "length");

    resp.setCharacterEncoding("utf-8");

    try {
      final LogData data = this.executorManagerAdapter.getExecutableFlowLog(exFlow, offset, length);
      ret.putAll(appendLogData(data, offset));

    } catch (final ExecutorManagerException e) {
      throw new ServletException(e);
    }
  }

  /**
   * Gets the logs through ajax plain text stream to reduce memory overhead.
   */
  private void ajaxFetchJobLogs(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user,
      final ExecutableFlow exFlow) throws ServletException {
    final Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
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
        ret.put("error", "Job " + jobId + " doesn't exist in " + exFlow.getExecutionId());
        return;
      }

      final int attempt = this.getIntParam(req, "attempt", node.getAttempt());
      final LogData data = this.executorManagerAdapter
          .getExecutionJobLog(exFlow, jobId, offset, length, attempt);
      ret.putAll(appendLogData(data, offset));

    } catch (final ExecutorManagerException e) {
      throw new ServletException(e);
    }
  }

  private Map<String, Object> appendLogData(final LogData data, final int defaultOffset) {
    final Map<String, Object> parameters = new HashMap<>();

    if (data == null) {
      parameters.put("length", 0);
      parameters.put("offset", defaultOffset);
      parameters.put("data", "");
    } else {
      parameters.put("length", data.getLength());
      parameters.put("offset", data.getOffset());
      parameters.put("data", StringEscapeUtils.escapeHtml(data.getData()));
    }

    return parameters;
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
          this.executorManagerAdapter
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
    ret.put("runtimeProperties", mergeRuntimeProperties(options));
    // For legacy support. This is not used by the Azkaban UI any more.
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

  /**
   * Copies flowOverrides under the key "ROOT".
   */
  private static Map<String, Map<String, String>> mergeRuntimeProperties(
      final ExecutionOptions options) {
    final Map<String, Map<String, String>> runtimeProperties = new HashMap<>();
    if (!options.getFlowParameters().isEmpty()) {
      runtimeProperties
          .put(Constants.ROOT_NODE_IDENTIFIER, new HashMap<>(options.getFlowParameters()));
    }
    for (final Entry<String, Map<String, String>> runtimeProp :
        options.getRuntimeProperties().entrySet()) {
      runtimeProperties.put(runtimeProp.getKey(), new HashMap<>(runtimeProp.getValue()));
    }
    return runtimeProperties;
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
      this.executorManagerAdapter.cancelFlow(exFlow, user.getUserId());
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
        this.executorManagerAdapter.getRunningFlowIds(project.getId(), flowId);
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
      this.executorManagerAdapter.pauseFlow(exFlow, user.getUserId());
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
      this.executorManagerAdapter.resumeFlow(exFlow, user.getUserId());
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
    if (node.getCondition() != null) {
      nodeObj.put("condition", node.getCondition());
    }
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
    } else {
      final ClusterInfo cluster = node.getClusterInfo();
      if (cluster != null && cluster.hadoopClusterURL != null) {
        nodeObj.put("cluster", cluster.hadoopClusterURL);
      }
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

    String autoRetryStatuses = exFlow.getExecutionOptions().getFlowParameters()
        .getOrDefault(FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "");
    if (!autoRetryStatuses.isEmpty()){
      Integer maxRetry = Integer.valueOf(exFlow.getExecutionOptions().getFlowParameters()
          .getOrDefault(FlowParameters.FLOW_PARAM_MAX_RETRIES, "1"));

      Map<String, Object> retriesInfo = new HashMap<>();
      retriesInfo.put("allowedStatuses", autoRetryStatuses);
      retriesInfo.put("userDefinedMax", maxRetry);
      retriesInfo.put("userDefinedCount", exFlow.getUserDefinedRetryCount());
      retriesInfo.put("systemDefinedCount", exFlow.getSystemDefinedRetryCount());
      retriesInfo.put("rootExecutionID", exFlow.getFlowRetryRootExecutionID());
      retriesInfo.put("parentExecutionID", exFlow.getFlowRetryParentExecutionID());
      retriesInfo.put("childExecutionID", exFlow.getFlowRetryChildExecutionID());

      ret.put("retries", (new JSONObject(retriesInfo)).toString());
    }

    final Map<String, Object> flowObj = getExecutableNodeInfo(exFlow);
    ret.putAll(flowObj);
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
      ret.put("error", "Flow '" + flowId + "' cannot be found in project " + project);
      return;
    }

    ExecutableFlow exflow = executorManagerAdapter.createExecutableFlow(project, flow);
    exflow.setUploadUser(project.getUploadUser());
    exflow.setSubmitUser(user.getUserId());
    exflow.setExecutionSource(Constants.EXECUTION_SOURCE_ADHOC);

    final ExecutionOptions options;
    final Props azProps = getApplication().getServerProps();
    try {
      options = HttpRequestUtils.parseFlowOptions(req, flowId);
      HttpRequestUtils.validatePreprocessFlowParameters(options, azProps);
    } catch (final ServletException e) {
      logger.info("parseFlowOptions failed", e);
      ret.put("error", "Error parsing flow options: " + e.getMessage());
      return;
    }
    if (!options.isFailureEmailsOverridden()) {
      options.setFailureEmails(flow.getFailureEmails());
    }
    if (!options.isSuccessEmailsOverridden()) {
      options.setSuccessEmails(flow.getSuccessEmails());
    }
    options.setMailCreator(flow.getMailCreator());

    exflow.getExecutionOptions().merge(options);

    /**
     * If the user has not explicitly overridden the failure action from the UI or
     * through API, we will consider the value specified in DSL/Yaml
     * By providing this override option, user can still choose to modify failure
     * option.
     */
    if (!options.isFailureActionOverridden() && flow.getFailureAction() != null) {
      options.setFailureAction(
          options.mapToFailureAction(flow.getFailureAction()));
    }
    try {
      HttpRequestUtils.filterAdminOnlyFlowParams(this.userManager, options, user);
      final String message =
          this.executorManagerAdapter.submitExecutableFlow(exflow, user.getUserId());
      ret.put("message", message);
    } catch (final Exception e) {
      e.printStackTrace();
      ret.put("error",
          "Error submitting flow " + exflow.getFlowId() + ". " + e.getMessage());
    }

    ret.put("execid", exflow.getExecutionId());
  }

  /**
   * This method is used to update the property. propType: Is the umbrella for properties for which
   * values need to be updated subType: Actual property for which values need to be updated val:
   * value to be updated
   * <p>
   * Example: propType=containerDispatch&subType=updateAllowList&val=spark,java
   * propType=containerDispatch&subType=updateDenyList&val=azktest
   */
  private void ajaxUpdateProperty(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user)
      throws ServletException {
    try {
      if (!HttpRequestUtils.hasPermission(this.userManager, user, Type.ADMIN)) {
        ret.put("error", String.format("User %s doesn't have ADMIN permission for updating "
            + "property", user));
        return;
      }
      String propType = getParam(req, "propType");
      if (propType.equals("containerDispatch")) {
        if (this.executorManagerAdapter instanceof ContainerizedDispatchManager) {
          updateContainerDispatchProps(req, ret);
        } else {
          ret.put("error",
              "ExecutorManagerAdapter is not of type: " + ContainerizedDispatchManager.class
                  .getName());
        }
      } else if (propType.equals("containerizedImpl")) {
        if (this.containerizedImpl != null) {
          updateContainerizedImplProps(req, ret);
        } else {
          ret.put("error", "ContainerizedImpl is null");
        }
      } else if (propType.equals("general")) {
        updateGeneralProps(req, ret);
      } else {
        ret.put("error", "Unsupported propType: " + propType);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      ret.put("error", "Error on update property. " + e.getMessage());
    }
  }

  private void updateContainerDispatchProps(final HttpServletRequest req,
      final HashMap<String, Object> ret)
      throws ServletException {
    ContainerizedDispatchManager containerizedDispatchManager = (ContainerizedDispatchManager) this.executorManagerAdapter;
    String subType = getParam(req, "subType");
    PropUpdate containerPropUpdate = PropUpdate.fromParam(subType);
    String val = getParam(req, "val");
    switch (containerPropUpdate) {
      case UPDATE_ALLOW_LIST:
        containerizedDispatchManager.getContainerJobTypeCriteria()
            .updateAllowList(ServletUtils.getSetFromString(val));
        break;
      case APPEND_ALLOW_LIST:
        containerizedDispatchManager.getContainerJobTypeCriteria()
            .appendAllowList(ServletUtils.getSetFromString(val));
        break;
      case REMOVE_FROM_ALLOW_LIST:
        containerizedDispatchManager.getContainerJobTypeCriteria()
            .removeFromAllowList(ServletUtils.getSetFromString(val));
        break;
      case UPDATE_RAMP_UP:
        containerizedDispatchManager.getContainerRampUpCriteria().setRampUp(Integer.parseInt(val));
        break;
      case APPEND_DENY_LIST:
        containerizedDispatchManager.getContainerProxyUserCriteria()
            .appendDenyList(ServletUtils.getSetFromString(val));
        break;
      case REMOVE_FROM_DENY_LIST:
        containerizedDispatchManager.getContainerProxyUserCriteria()
            .removeFromDenyList(ServletUtils.getSetFromString(val));
        break;
      case RELOAD_FLOW_FILTER:
        containerizedDispatchManager.getContainerFlowCriteria()
            .reloadFlowFilter();
        break;
      default:
        break;
    }
  }

  private void updateContainerizedImplProps(final HttpServletRequest req,
      final HashMap<String, Object> ret)
      throws ServletException {
    String subType = getParam(req, "subType");
    PropUpdate propUpdate = PropUpdate.fromParam(subType);
    String val = getParam(req, "val");
    switch (propUpdate) {
      case UPDATE_VPA_RAMP_UP:
        this.containerizedImpl.setVPARampUp(Integer.parseInt(val));
        break;
      case UPDATE_VPA_ENABLED:
        this.containerizedImpl.setVPAEnabled(Boolean.parseBoolean(val));
        break;
      case RELOAD_VPA_FLOW_FILTER:
        this.containerizedImpl.getVPAFlowCriteria().reloadFlowFilter();
        break;
      default:
        break;
    }
  }

  private void updateGeneralProps(final HttpServletRequest req,
      final HashMap<String, Object> ret)
      throws ServletException {
    String subType = getParam(req, "subType");
    PropUpdate propUpdate = PropUpdate.fromParam(subType);
    String val = getParam(req, "val");
    switch (propUpdate) {
      case ENABLE_OFFLINE_LOGS_LOADER:
        this.executorManagerAdapter.enableOfflineLogsLoader(Boolean.parseBoolean(val));
        break;
      default:
        break;
    }
  }

  private void ajaxRampActions(final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> ret, final User user)
      throws ServletException {

    try {
      final Object body = HttpRequestUtils.getJsonBody(req);
      if (HttpRequestUtils.hasPermission(this.userManager, user, Type.ADMIN)) {
        Map<String, String> result = new HashMap<>();
        if (body instanceof List) { // A list of actions
          final List<Map<String, Object>> rampActions = (List<Map<String, Object>>) body;
          result = this.executorManagerAdapter.doRampActions(rampActions);
        } else if (body instanceof Map) {
          final List<Map<String, Object>> rampActions = new ArrayList<>();
          rampActions.add((Map<String, Object>) body);
          result = this.executorManagerAdapter.doRampActions(rampActions);
        } else {
          result.put("error", "Invalid Body Format");
        }
        ret.putAll(result);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      ret.put("error", "Error on update Ramp. " + e.getMessage());
    }
  }
}

enum PropUpdate {
  UPDATE_ALLOW_LIST("updateAllowList"),
  APPEND_ALLOW_LIST("appendAllowList"),
  REMOVE_FROM_ALLOW_LIST("removeFromAllowList"),
  UPDATE_RAMP_UP("updateRampUp"),
  APPEND_DENY_LIST("appendDenyList"),
  REMOVE_FROM_DENY_LIST("removeFromDenyList"),
  RELOAD_FLOW_FILTER("reloadFlowFilter"),
  UPDATE_VPA_RAMP_UP("updateVPARampUp"),
  UPDATE_VPA_ENABLED("updateVPAEnabled"),
  RELOAD_VPA_FLOW_FILTER("reloadVPAFlowFilter"),
  ENABLE_OFFLINE_LOGS_LOADER("enableOfflineLogsLoader");

  private final String param;

  PropUpdate(String param) {
    this.param = param;
  }

  public String getParam() {
    return param;
  }

  public static PropUpdate fromParam(String param) {
    for (PropUpdate value : PropUpdate.values()) {
      if (value.getParam().equals(param)) {
        return value;
      }
    }
    throw new IllegalArgumentException(
        "No PropUpdate corresponding to param value " + param);
  }
}
