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

import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableJobInfo;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;
import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.project.Project;
import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLogEvent;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.project.ProjectWhitelist;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidatorConfigs;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.quartz.SchedulerException;

public class ProjectManagerServlet extends LoginAbstractAzkabanServlet {

  private static final String APPLICATION_ZIP_MIME_TYPE = "application/zip";
  private static final long serialVersionUID = 1;
  private static final Logger logger = Logger
      .getLogger(ProjectManagerServlet.class);
  private static final NodeLevelComparator NODE_LEVEL_COMPARATOR =
      new NodeLevelComparator();
  private static final String LOCKDOWN_CREATE_PROJECTS_KEY =
      "lockdown.create.projects";
  private static final String LOCKDOWN_UPLOAD_PROJECTS_KEY =
      "lockdown.upload.projects";

  private static final String PROJECT_DOWNLOAD_BUFFER_SIZE_IN_BYTES =
      "project.download.buffer.size";
  private static final Comparator<Flow> FLOW_ID_COMPARATOR = new Comparator<Flow>() {
    @Override
    public int compare(final Flow f1, final Flow f2) {
      return f1.getId().compareTo(f2.getId());
    }
  };
  private ProjectManager projectManager;
  private ExecutorManagerAdapter executorManager;
  private ScheduleManager scheduleManager;
  private UserManager userManager;
  private FlowTriggerScheduler scheduler;
  private int downloadBufferSize;
  private boolean lockdownCreateProjects = false;
  private boolean lockdownUploadProjects = false;
  private boolean enableQuartz = false;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);

    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.projectManager = server.getProjectManager();
    this.executorManager = server.getExecutorManager();
    this.scheduleManager = server.getScheduleManager();
    this.userManager = server.getUserManager();
    this.scheduler = server.getScheduler();
    this.lockdownCreateProjects =
        server.getServerProps().getBoolean(LOCKDOWN_CREATE_PROJECTS_KEY, false);
    this.enableQuartz = server.getServerProps().getBoolean(ConfigurationKeys.ENABLE_QUARTZ, false);
    if (this.lockdownCreateProjects) {
      logger.info("Creation of projects is locked down");
    }

    this.lockdownUploadProjects =
        server.getServerProps().getBoolean(LOCKDOWN_UPLOAD_PROJECTS_KEY, false);
    if (this.lockdownUploadProjects) {
      logger.info("Uploading of projects is locked down");
    }

    this.downloadBufferSize =
        server.getServerProps().getInt(PROJECT_DOWNLOAD_BUFFER_SIZE_IN_BYTES,
            8192);

    logger.info("downloadBufferSize: " + this.downloadBufferSize);
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "project")) {
      if (hasParam(req, "ajax")) {
        handleAJAXAction(req, resp, session);
      } else if (hasParam(req, "logs")) {
        handleProjectLogsPage(req, resp, session);
      } else if (hasParam(req, "permissions")) {
        handlePermissionPage(req, resp, session);
      } else if (hasParam(req, "prop")) {
        handlePropertyPage(req, resp, session);
      } else if (hasParam(req, "history")) {
        handleJobHistoryPage(req, resp, session);
      } else if (hasParam(req, "job")) {
        handleJobPage(req, resp, session);
      } else if (hasParam(req, "flow")) {
        handleFlowPage(req, resp, session);
      } else if (hasParam(req, "delete")) {
        handleRemoveProject(req, resp, session);
      } else if (hasParam(req, "purge")) {
        handlePurgeProject(req, resp, session);
      } else if (hasParam(req, "download")) {
        handleDownloadProject(req, resp, session);
      } else {
        handleProjectPage(req, resp, session);
      }
      return;
    } else if (hasParam(req, "reloadProjectWhitelist")) {
      handleReloadProjectWhitelist(req, resp, session);
    }

    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/projectpage.vm");
    page.add("errorMsg", "No project set.");
    page.render();
  }

  @Override
  protected void handleMultiformPost(final HttpServletRequest req,
      final HttpServletResponse resp, final Map<String, Object> params, final Session session)
      throws ServletException, IOException {
    // Looks like a duplicate, but this is a move away from the regular
    // multiform post + redirect
    // to a more ajax like command.
    if (params.containsKey("ajax")) {
      final String action = (String) params.get("ajax");
      final HashMap<String, String> ret = new HashMap<>();
      if (action.equals("upload")) {
        ajaxHandleUpload(req, resp, ret, params, session);
      }
      this.writeJSON(resp, ret);
    } else if (params.containsKey("action")) {
      final String action = (String) params.get("action");
      if (action.equals("upload")) {
        handleUpload(req, resp, params, session);
      }
    }
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "action")) {
      final String action = getParam(req, "action");
      if (action.equals("create")) {
        handleCreate(req, resp, session);
      }
    }
  }

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final String projectName = getParam(req, "project");
    final User user = session.getUser();

    final HashMap<String, Object> ret = new HashMap<>();
    ret.put("project", projectName);

    final Project project = this.projectManager.getProject(projectName);
    if (project == null) {
      ret.put("error", "Project " + projectName + " doesn't exist.");
    } else {
      ret.put("projectId", project.getId());
      final String ajaxName = getParam(req, "ajax");
      if (ajaxName.equals("getProjectId")) {
        // Do nothing, since projectId is added to all AJAX requests.
      } else if (ajaxName.equals("fetchProjectLogs")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchProjectLogEvents(project, req, ret);
        }
      } else if (ajaxName.equals("fetchflowjobs")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlow(project, ret, req);
        }
      } else if (ajaxName.equals("fetchflowdetails")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlowDetails(project, ret, req);
        }
      } else if (ajaxName.equals("fetchflowgraph")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlowGraph(project, ret, req);
        }
      } else if (ajaxName.equals("fetchflownodedata")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlowNodeData(project, ret, req);
        }
      } else if (ajaxName.equals("fetchprojectflows")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchProjectFlows(project, ret, req);
        }
      } else if (ajaxName.equals("changeDescription")) {
        if (handleAjaxPermission(project, user, Type.WRITE, ret)) {
          ajaxChangeDescription(project, ret, req, user);
        }
      } else if (ajaxName.equals("getPermissions")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxGetPermissions(project, ret);
        }
      } else if (ajaxName.equals("getGroupPermissions")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxGetGroupPermissions(project, ret);
        }
      } else if (ajaxName.equals("getProxyUsers")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxGetProxyUsers(project, ret);
        }
      } else if (ajaxName.equals("changePermission")) {
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxChangePermissions(project, ret, req, user);
        }
      } else if (ajaxName.equals("addPermission")) {
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxAddPermission(project, ret, req, user);
        }
      } else if (ajaxName.equals("addProxyUser")) {
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxAddProxyUser(project, ret, req, user);
        }
      } else if (ajaxName.equals("removeProxyUser")) {
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxRemoveProxyUser(project, ret, req, user);
        }
      } else if (ajaxName.equals("fetchFlowExecutions")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlowExecutions(project, ret, req);
        }
      } else if (ajaxName.equals("fetchLastSuccessfulFlowExecution")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchLastSuccessfulFlowExecution(project, ret, req);
        }
      } else if (ajaxName.equals("fetchJobInfo")) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchJobInfo(project, ret, req);
        }
      } else if (ajaxName.equals("setJobOverrideProperty")) {
        if (handleAjaxPermission(project, user, Type.WRITE, ret)) {
          ajaxSetJobOverrideProperty(project, ret, req, user);
        }
      } else {
        ret.put("error", "Cannot execute command " + ajaxName);
      }
    }

    this.writeJSON(resp, ret);
  }

  private boolean handleAjaxPermission(final Project project, final User user, final Type type,
      final Map<String, Object> ret) {
    if (hasPermission(project, user, type)) {
      return true;
    }

    ret.put("error", "Permission denied. Need " + type.toString() + " access.");
    return false;
  }

  private void ajaxFetchProjectLogEvents(final Project project,
      final HttpServletRequest req, final HashMap<String, Object> ret) throws ServletException {
    final int num = this.getIntParam(req, "size", 1000);
    final int skip = this.getIntParam(req, "skip", 0);

    final List<ProjectLogEvent> logEvents;
    try {
      logEvents = this.projectManager.getProjectEventLogs(project, num, skip);
    } catch (final ProjectManagerException e) {
      throw new ServletException(e);
    }

    final String[] columns = new String[]{"user", "time", "type", "message"};
    ret.put("columns", columns);

    final List<Object[]> eventData = new ArrayList<>();
    for (final ProjectLogEvent events : logEvents) {
      final Object[] entry = new Object[4];
      entry[0] = events.getUser();
      entry[1] = events.getTime();
      entry[2] = events.getType();
      entry[3] = events.getMessage();

      eventData.add(entry);
    }

    ret.put("logData", eventData);
  }

  private List<String> getFlowJobTypes(final Flow flow) {
    final Set<String> jobTypeSet = new HashSet<>();
    for (final Node node : flow.getNodes()) {
      jobTypeSet.add(node.getType());
    }
    final List<String> jobTypes = new ArrayList<>();
    jobTypes.addAll(jobTypeSet);
    return jobTypes;
  }

  private void ajaxFetchFlowDetails(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req)
      throws ServletException {
    final String flowName = getParam(req, "flow");

    Flow flow = null;
    try {
      flow = project.getFlow(flowName);
      if (flow == null) {
        ret.put("error", "Flow " + flowName + " not found.");
        return;
      }

      ret.put("jobTypes", getFlowJobTypes(flow));
    } catch (final AccessControlException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxFetchLastSuccessfulFlowExecution(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req)
      throws ServletException {
    final String flowId = getParam(req, "flow");
    List<ExecutableFlow> exFlows = null;
    try {
      exFlows =
          this.executorManager.getExecutableFlows(project.getId(), flowId, 0, 1,
              Status.SUCCEEDED);
    } catch (final ExecutorManagerException e) {
      ret.put("error", "Error retrieving executable flows");
      return;
    }

    if (exFlows.size() == 0) {
      ret.put("success", "false");
      ret.put("message", "This flow has no successful run.");
      return;
    }

    ret.put("success", "true");
    ret.put("message", "");
    ret.put("execId", exFlows.get(0).getExecutionId());
  }

  private void ajaxFetchFlowExecutions(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req)
      throws ServletException {
    final String flowId = getParam(req, "flow");
    final int from = Integer.valueOf(getParam(req, "start"));
    final int length = Integer.valueOf(getParam(req, "length"));

    final ArrayList<ExecutableFlow> exFlows = new ArrayList<>();
    int total = 0;
    try {
      total =
          this.executorManager.getExecutableFlows(project.getId(), flowId, from,
              length, exFlows);
    } catch (final ExecutorManagerException e) {
      ret.put("error", "Error retrieving executable flows");
    }

    ret.put("flow", flowId);
    ret.put("total", total);
    ret.put("from", from);
    ret.put("length", length);

    final ArrayList<Object> history = new ArrayList<>();
    for (final ExecutableFlow flow : exFlows) {
      final HashMap<String, Object> flowInfo = new HashMap<>();
      flowInfo.put("execId", flow.getExecutionId());
      flowInfo.put("flowId", flow.getFlowId());
      flowInfo.put("projectId", flow.getProjectId());
      flowInfo.put("status", flow.getStatus().toString());
      flowInfo.put("submitTime", flow.getSubmitTime());
      flowInfo.put("startTime", flow.getStartTime());
      flowInfo.put("endTime", flow.getEndTime());
      flowInfo.put("submitUser", flow.getSubmitUser());

      history.add(flowInfo);
    }

    ret.put("executions", history);
  }

  /**
   * Download project zip file from DB and send it back client.
   *
   * This method requires a project name and an optional project version.
   */
  private void handleDownloadProject(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {

    final User user = session.getUser();
    final String projectName = getParam(req, "project");
    logger.info(user.getUserId() + " is downloading project: " + projectName);

    final Project project = this.projectManager.getProject(projectName);
    if (project == null) {
      this.setErrorMessageInCookie(resp, "Project " + projectName
          + " doesn't exist.");
      resp.sendRedirect(req.getContextPath());
      return;
    }

    if (!hasPermission(project, user, Type.READ)) {
      this.setErrorMessageInCookie(resp, "No permission to download project " + projectName
          + ".");
      resp.sendRedirect(req.getContextPath());
      return;
    }

    int version = -1;
    if (hasParam(req, "version")) {
      version = getIntParam(req, "version");
    }

    ProjectFileHandler projectFileHandler = null;
    FileInputStream inStream = null;
    OutputStream outStream = null;
    try {
      projectFileHandler =
          this.projectManager.getProjectFileHandler(project, version);
      if (projectFileHandler == null) {
        this.setErrorMessageInCookie(resp, "Project " + projectName
            + " with version " + version + " doesn't exist");
        resp.sendRedirect(req.getContextPath());
        return;
      }
      final File projectZipFile = projectFileHandler.getLocalFile();
      final String logStr =
          String.format(
              "downloading project zip file for project \"%s\" at \"%s\""
                  + " size: %d type: %s  fileName: \"%s\"",
              projectFileHandler.getFileName(),
              projectZipFile.getAbsolutePath(), projectZipFile.length(),
              projectFileHandler.getFileType(),
              projectFileHandler.getFileName());
      logger.info(logStr);

      // now set up HTTP response for downloading file
      inStream = new FileInputStream(projectZipFile);

      resp.setContentType(APPLICATION_ZIP_MIME_TYPE);

      final String headerKey = "Content-Disposition";
      final String headerValue =
          String.format("attachment; filename=\"%s\"",
              projectFileHandler.getFileName());
      resp.setHeader(headerKey, headerValue);
      resp.setHeader("version",
          Integer.toString(projectFileHandler.getVersion()));
      resp.setHeader("projectId",
          Integer.toString(projectFileHandler.getProjectId()));

      outStream = resp.getOutputStream();

      final byte[] buffer = new byte[this.downloadBufferSize];
      int bytesRead = -1;

      while ((bytesRead = inStream.read(buffer)) != -1) {
        outStream.write(buffer, 0, bytesRead);
      }

    } catch (final Throwable e) {
      logger.error(
          "Encountered error while downloading project zip file for project: "
              + projectName + " by user: " + user.getUserId(), e);
      throw new ServletException(e);
    } finally {
      IOUtils.closeQuietly(inStream);
      IOUtils.closeQuietly(outStream);

      if (projectFileHandler != null) {
        projectFileHandler.deleteLocalFile();
      }
    }

  }

  /**
   * validate readiness of a project and user permission and use projectManager to purge the project
   * if things looks good
   **/
  private void handlePurgeProject(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final User user = session.getUser();
    final HashMap<String, Object> ret = new HashMap<>();
    boolean isOperationSuccessful = true;

    try {
      Project project = null;
      final String projectParam = getParam(req, "project");

      if (StringUtils.isNumeric(projectParam)) {
        project = this.projectManager.getProject(Integer.parseInt(projectParam)); // get
        // project
        // by
        // Id
      } else {
        project = this.projectManager.getProject(projectParam); // get project by
        // name (name cannot
        // start
        // from ints)
      }

      // invalid project
      if (project == null) {
        ret.put("error", "invalid project");
        isOperationSuccessful = false;
      }

      // project is already deleted
      if (isOperationSuccessful
          && this.projectManager.isActiveProject(project.getId())) {
        ret.put("error", "Project " + project.getName()
            + " should be deleted before purging");
        isOperationSuccessful = false;
      }

      // only eligible users can purge a project
      if (isOperationSuccessful && !hasPermission(project, user, Type.ADMIN)) {
        ret.put("error", "Cannot purge. User '" + user.getUserId()
            + "' is not an ADMIN.");
        isOperationSuccessful = false;
      }

      if (isOperationSuccessful) {
        this.projectManager.purgeProject(project, user);
      }
    } catch (final Exception e) {
      ret.put("error", e.getMessage());
      isOperationSuccessful = false;
    }

    ret.put("success", isOperationSuccessful);
    this.writeJSON(resp, ret);
  }

  private void removeAssociatedSchedules(final Project project) throws ServletException {
    // remove regular schedules
    try {
      for (final Schedule schedule : this.scheduleManager.getSchedules()) {
        if (schedule.getProjectId() == project.getId()) {
          logger.info("removing schedule " + schedule.getScheduleId());
          this.scheduleManager.removeSchedule(schedule);
        }
      }
    } catch (final ScheduleManagerException e) {
      throw new ServletException(e);
    }

    // remove flow trigger schedules
    try {
      if (this.enableQuartz) {
        this.scheduler.unscheduleAll(project);
      }
    } catch (final SchedulerException e) {
      throw new ServletException(e);
    }
  }

  private void handleRemoveProject(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final User user = session.getUser();
    final String projectName = getParam(req, "project");

    final Project project = this.projectManager.getProject(projectName);
    if (project == null) {
      this.setErrorMessageInCookie(resp, "Project " + projectName
          + " doesn't exist.");
      resp.sendRedirect(req.getContextPath());
      return;
    }

    if (!hasPermission(project, user, Type.ADMIN)) {
      this.setErrorMessageInCookie(resp,
          "Cannot delete. User '" + user.getUserId() + "' is not an ADMIN.");
      resp.sendRedirect(req.getRequestURI() + "?project=" + projectName);
      return;
    }

    removeAssociatedSchedules(project);

    try {
      this.projectManager.removeProject(project, user);
    } catch (final ProjectManagerException e) {
      this.setErrorMessageInCookie(resp, e.getMessage());
      resp.sendRedirect(req.getRequestURI() + "?project=" + projectName);
      return;
    }

    this.setSuccessMessageInCookie(resp, "Project '" + projectName
        + "' was successfully deleted and associated schedules are removed.");
    resp.sendRedirect(req.getContextPath());
  }

  private void ajaxChangeDescription(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req, final User user)
      throws ServletException {
    final String description = getParam(req, "description");
    project.setDescription(description);

    try {
      this.projectManager.updateProjectDescription(project, description, user);
    } catch (final ProjectManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxFetchJobInfo(final Project project, final HashMap<String, Object> ret,
      final HttpServletRequest req) throws ServletException {
    final String flowName = getParam(req, "flowName");
    final String jobName = getParam(req, "jobName");

    final Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put("error",
          "Flow " + flowName + " not found in project " + project.getName());
      return;
    }

    final Node node = flow.getNode(jobName);
    if (node == null) {
      ret.put("error", "Job " + jobName + " not found in flow " + flowName);
      return;
    }

    Props jobProp;
    try {
      jobProp = this.projectManager.getProperties(project, flow, jobName, node.getJobSource());
    } catch (final ProjectManagerException e) {
      ret.put("error", "Failed to retrieve job properties!");
      return;
    }

    if (jobProp == null) {
      jobProp = new Props();
    }

    Props overrideProp;
    try {
      overrideProp = this.projectManager
          .getJobOverrideProperty(project, flow, jobName, node.getJobSource());
    } catch (final ProjectManagerException e) {
      ret.put("error", "Failed to retrieve job override properties!");
      return;
    }

    ret.put("jobName", node.getId());
    ret.put("jobType", jobProp.get("type"));

    if (overrideProp == null) {
      overrideProp = new Props(jobProp);
    }

    final Map<String, String> generalParams = new HashMap<>();
    final Map<String, String> overrideParams = new HashMap<>();
    for (final String ps : jobProp.getKeySet()) {
      generalParams.put(ps, jobProp.getString(ps));
    }
    for (final String ops : overrideProp.getKeySet()) {
      overrideParams.put(ops, overrideProp.getString(ops));
    }
    ret.put("generalParams", generalParams);
    ret.put("overrideParams", overrideParams);
  }

  private void ajaxSetJobOverrideProperty(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req, final User user)
      throws ServletException {
    final String flowName = getParam(req, "flowName");
    final String jobName = getParam(req, "jobName");

    final Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put("error",
          "Flow " + flowName + " not found in project " + project.getName());
      return;
    }

    final Node node = flow.getNode(jobName);
    if (node == null) {
      ret.put("error", "Job " + jobName + " not found in flow " + flowName);
      return;
    }

    final Map<String, String> jobParamGroup = this.getParamGroup(req, "jobOverride");
    final Props overrideParams = new Props(null, jobParamGroup);
    try {
      this.projectManager
          .setJobOverrideProperty(project, flow, overrideParams, jobName, node.getJobSource(),
              user);
    } catch (final ProjectManagerException e) {
      ret.put("error", "Failed to upload job override property");
    }

  }

  private void ajaxFetchProjectFlows(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req)
      throws ServletException {
    final ArrayList<Map<String, Object>> flowList =
        new ArrayList<>();
    for (final Flow flow : project.getFlows()) {
      if (!flow.isEmbeddedFlow()) {
        final HashMap<String, Object> flowObj = new HashMap<>();
        flowObj.put("flowId", flow.getId());
        flowList.add(flowObj);
      }
    }

    ret.put("flows", flowList);
  }

  private void ajaxFetchFlowGraph(final Project project, final HashMap<String, Object> ret,
      final HttpServletRequest req) throws ServletException {
    final String flowId = getParam(req, "flow");

    fillFlowInfo(project, flowId, ret);
  }

  private void fillFlowInfo(final Project project, final String flowId,
      final HashMap<String, Object> ret) {
    final Flow flow = project.getFlow(flowId);
    if (flow == null) {
      ret.put("error",
          "Flow " + flowId + " not found in project " + project.getName());
      return;
    }

    final ArrayList<Map<String, Object>> nodeList =
        new ArrayList<>();
    for (final Node node : flow.getNodes()) {
      final HashMap<String, Object> nodeObj = new HashMap<>();
      nodeObj.put("id", node.getId());
      nodeObj.put("type", node.getType());
      if (node.getEmbeddedFlowId() != null) {
        nodeObj.put("flowId", node.getEmbeddedFlowId());
        fillFlowInfo(project, node.getEmbeddedFlowId(), nodeObj);
      }

      nodeList.add(nodeObj);
      final Set<Edge> inEdges = flow.getInEdges(node.getId());
      if (inEdges != null && !inEdges.isEmpty()) {
        final ArrayList<String> inEdgesList = new ArrayList<>();
        for (final Edge edge : inEdges) {
          inEdgesList.add(edge.getSourceId());
        }
        Collections.sort(inEdgesList);
        nodeObj.put("in", inEdgesList);
      }
    }

    Collections.sort(nodeList, new Comparator<Map<String, Object>>() {
      @Override
      public int compare(final Map<String, Object> o1, final Map<String, Object> o2) {
        final String id = (String) o1.get("id");
        return id.compareTo((String) o2.get("id"));
      }
    });

    ret.put("flow", flowId);
    ret.put("nodes", nodeList);
  }

  private void ajaxFetchFlowNodeData(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req)
      throws ServletException {
    final String flowId = getParam(req, "flow");
    final Flow flow = project.getFlow(flowId);

    final String nodeId = getParam(req, "node");
    final Node node = flow.getNode(nodeId);

    if (node == null) {
      ret.put("error", "Job " + nodeId + " doesn't exist.");
      return;
    }

    ret.put("id", nodeId);
    ret.put("flow", flowId);
    ret.put("type", node.getType());

    final Props jobProps;
    try {
      jobProps = this.projectManager.getProperties(project, flow, nodeId, node.getJobSource());
    } catch (final ProjectManagerException e) {
      ret.put("error", "Failed to upload job override property for " + nodeId);
      return;
    }

    if (jobProps == null) {
      ret.put("error", "Properties for " + nodeId + " isn't found.");
      return;
    }

    final Map<String, String> properties = PropsUtils.toStringMap(jobProps, true);
    ret.put("props", properties);

    if (node.getType().equals("flow")) {
      if (node.getEmbeddedFlowId() != null) {
        fillFlowInfo(project, node.getEmbeddedFlowId(), ret);
      }
    }
  }

  private void ajaxFetchFlow(final Project project, final HashMap<String, Object> ret,
      final HttpServletRequest req) throws ServletException {
    final String flowId = getParam(req, "flow");
    final Flow flow = project.getFlow(flowId);

    final ArrayList<Node> flowNodes = new ArrayList<>(flow.getNodes());
    Collections.sort(flowNodes, NODE_LEVEL_COMPARATOR);

    final ArrayList<Object> nodeList = new ArrayList<>();
    for (final Node node : flowNodes) {
      final HashMap<String, Object> nodeObj = new HashMap<>();
      nodeObj.put("id", node.getId());

      final ArrayList<String> dependencies = new ArrayList<>();
      Collection<Edge> collection = flow.getInEdges(node.getId());
      if (collection != null) {
        for (final Edge edge : collection) {
          dependencies.add(edge.getSourceId());
        }
      }

      final ArrayList<String> dependents = new ArrayList<>();
      collection = flow.getOutEdges(node.getId());
      if (collection != null) {
        for (final Edge edge : collection) {
          dependents.add(edge.getTargetId());
        }
      }

      nodeObj.put("dependencies", dependencies);
      nodeObj.put("dependents", dependents);
      nodeObj.put("level", node.getLevel());
      nodeList.add(nodeObj);
    }

    ret.put("flowId", flowId);
    ret.put("nodes", nodeList);
  }

  private void ajaxAddProxyUser(final Project project, final HashMap<String, Object> ret,
      final HttpServletRequest req, final User user) throws ServletException {
    final String name = getParam(req, "name");

    logger.info("Adding proxy user " + name + " by " + user.getUserId());
    if (this.userManager.validateProxyUser(name, user)) {
      try {
        this.projectManager.addProjectProxyUser(project, name, user);
      } catch (final ProjectManagerException e) {
        ret.put("error", e.getMessage());
      }
    } else {
      ret.put("error", "User " + user.getUserId()
          + " has no permission to add " + name + " as proxy user.");
      return;
    }
  }

  private void ajaxRemoveProxyUser(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req, final User user)
      throws ServletException {
    final String name = getParam(req, "name");

    logger.info("Removing proxy user " + name + " by " + user.getUserId());

    try {
      this.projectManager.removeProjectProxyUser(project, name, user);
    } catch (final ProjectManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxAddPermission(final Project project, final HashMap<String, Object> ret,
      final HttpServletRequest req, final User user) throws ServletException {
    final String name = getParam(req, "name");
    final boolean group = Boolean.parseBoolean(getParam(req, "group"));

    if (group) {
      if (project.getGroupPermission(name) != null) {
        ret.put("error", "Group permission already exists.");
        return;
      }
      if (!this.userManager.validateGroup(name)) {
        ret.put("error", "Group is invalid.");
        return;
      }
    } else {
      if (project.getUserPermission(name) != null) {
        ret.put("error", "User permission already exists.");
        return;
      }
      if (!this.userManager.validateUser(name)) {
        ret.put("error", "User is invalid.");
        return;
      }
    }

    final boolean admin = Boolean.parseBoolean(getParam(req, "permissions[admin]"));
    final boolean read = Boolean.parseBoolean(getParam(req, "permissions[read]"));
    final boolean write = Boolean.parseBoolean(getParam(req, "permissions[write]"));
    final boolean execute =
        Boolean.parseBoolean(getParam(req, "permissions[execute]"));
    final boolean schedule =
        Boolean.parseBoolean(getParam(req, "permissions[schedule]"));

    final Permission perm = new Permission();
    if (admin) {
      perm.setPermission(Type.ADMIN, true);
    } else {
      perm.setPermission(Type.READ, read);
      perm.setPermission(Type.WRITE, write);
      perm.setPermission(Type.EXECUTE, execute);
      perm.setPermission(Type.SCHEDULE, schedule);
    }

    try {
      this.projectManager.updateProjectPermission(project, name, perm, group, user);
    } catch (final ProjectManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxChangePermissions(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req, final User user)
      throws ServletException {
    final boolean admin = Boolean.parseBoolean(getParam(req, "permissions[admin]"));
    final boolean read = Boolean.parseBoolean(getParam(req, "permissions[read]"));
    final boolean write = Boolean.parseBoolean(getParam(req, "permissions[write]"));
    final boolean execute =
        Boolean.parseBoolean(getParam(req, "permissions[execute]"));
    final boolean schedule =
        Boolean.parseBoolean(getParam(req, "permissions[schedule]"));

    final boolean group = Boolean.parseBoolean(getParam(req, "group"));

    final String name = getParam(req, "name");
    final Permission perm;
    if (group) {
      perm = project.getGroupPermission(name);
    } else {
      perm = project.getUserPermission(name);
    }

    if (perm == null) {
      ret.put("error", "Permissions for " + name + " cannot be found.");
      return;
    }

    if (admin || read || write || execute || schedule) {
      if (admin) {
        perm.setPermission(Type.ADMIN, true);
        perm.setPermission(Type.READ, false);
        perm.setPermission(Type.WRITE, false);
        perm.setPermission(Type.EXECUTE, false);
        perm.setPermission(Type.SCHEDULE, false);
      } else {
        perm.setPermission(Type.ADMIN, false);
        perm.setPermission(Type.READ, read);
        perm.setPermission(Type.WRITE, write);
        perm.setPermission(Type.EXECUTE, execute);
        perm.setPermission(Type.SCHEDULE, schedule);
      }

      try {
        this.projectManager
            .updateProjectPermission(project, name, perm, group, user);
      } catch (final ProjectManagerException e) {
        ret.put("error", e.getMessage());
      }
    } else {
      try {
        this.projectManager.removeProjectPermission(project, name, group, user);
      } catch (final ProjectManagerException e) {
        ret.put("error", e.getMessage());
      }
    }
  }

  /**
   * this only returns user permissions, but not group permissions and proxy users
   */
  private void ajaxGetPermissions(final Project project, final HashMap<String, Object> ret) {
    final ArrayList<HashMap<String, Object>> permissions =
        new ArrayList<>();
    for (final Pair<String, Permission> perm : project.getUserPermissions()) {
      final HashMap<String, Object> permObj = new HashMap<>();
      final String userId = perm.getFirst();
      permObj.put("username", userId);
      permObj.put("permission", perm.getSecond().toStringArray());

      permissions.add(permObj);
    }

    ret.put("permissions", permissions);
  }

  private void ajaxGetGroupPermissions(final Project project,
      final HashMap<String, Object> ret) {
    final ArrayList<HashMap<String, Object>> permissions =
        new ArrayList<>();
    for (final Pair<String, Permission> perm : project.getGroupPermissions()) {
      final HashMap<String, Object> permObj = new HashMap<>();
      final String userId = perm.getFirst();
      permObj.put("username", userId);
      permObj.put("permission", perm.getSecond().toStringArray());

      permissions.add(permObj);
    }

    ret.put("permissions", permissions);
  }

  private void ajaxGetProxyUsers(final Project project, final HashMap<String, Object> ret) {
    final String[] proxyUsers = project.getProxyUsers().toArray(new String[0]);
    ret.put("proxyUsers", proxyUsers);
  }

  private void handleProjectLogsPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/projectlogpage.vm");
    final String projectName = getParam(req, "project");

    final User user = session.getUser();
    PageUtils
        .hideUploadButtonWhenNeeded(page, session, this.userManager, this.lockdownUploadProjects);
    Project project = null;
    try {
      project = this.projectManager.getProject(projectName);
      if (project == null) {
        page.add("errorMsg", "Project " + projectName + " doesn't exist.");
      } else {
        if (!hasPermission(project, user, Type.READ)) {
          throw new AccessControlException("No permission to view project "
              + projectName + ".");
        }

        page.add("project", project);
        page.add("admins", Utils.flattenToString(
            project.getUsersWithPermission(Type.ADMIN), ","));
        final Permission perm = this.getPermissionObject(project, user, Type.ADMIN);
        page.add("userpermission", perm);

        final boolean adminPerm = perm.isPermissionSet(Type.ADMIN);
        if (adminPerm) {
          page.add("admin", true);
        }
        // Set this so we can display execute buttons only to those who have
        // access.
        if (perm.isPermissionSet(Type.EXECUTE) || adminPerm) {
          page.add("exec", true);
        } else {
          page.add("exec", false);
        }
      }
    } catch (final AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    }

    final int numBytes = 1024;

    // Really sucks if we do a lot of these because it'll eat up memory fast.
    // But it's expected that this won't be a heavily used thing. If it is,
    // then we'll revisit it to make it more stream friendly.
    final StringBuffer buffer = new StringBuffer(numBytes);
    page.add("log", buffer.toString());

    page.render();
  }

  private void handleJobHistoryPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/jobhistorypage.vm");
    final String projectName = getParam(req, "project");
    final User user = session.getUser();

    final Project project = this.projectManager.getProject(projectName);
    if (project == null) {
      page.add("errorMsg", "Project " + projectName + " doesn't exist.");
      page.render();
      return;
    }
    if (!hasPermission(project, user, Type.READ)) {
      page.add("errorMsg", "No permission to view project " + projectName + ".");
      page.render();
      return;
    }

    final String jobId = getParam(req, "job");
    final int pageNum = getIntParam(req, "page", 1);
    final int pageSize = getIntParam(req, "size", 25);

    page.add("projectId", project.getId());
    page.add("projectName", project.getName());
    page.add("jobid", jobId);
    page.add("page", pageNum);

    final int skipPage = (pageNum - 1) * pageSize;

    int numResults = 0;
    try {
      numResults = this.executorManager.getNumberOfJobExecutions(project, jobId);
      final int maxPage = (numResults / pageSize) + 1;
      List<ExecutableJobInfo> jobInfo =
          this.executorManager.getExecutableJobs(project, jobId, skipPage, pageSize);

      if (jobInfo == null || jobInfo.isEmpty()) {
        jobInfo = null;
      }
      page.add("history", jobInfo);

      page.add("previous", new PageSelection("Previous", pageSize, true, false,
          Math.max(pageNum - 1, 1)));

      page.add(
          "next",
          new PageSelection("Next", pageSize, false, false, Math.min(
              pageNum + 1, maxPage)));

      if (jobInfo != null) {
        final ArrayList<Object> dataSeries = new ArrayList<>();
        for (final ExecutableJobInfo info : jobInfo) {
          final Map<String, Object> map = info.toObject();
          dataSeries.add(map);
        }
        page.add("dataSeries", JSONUtils.toJSON(dataSeries));
      } else {
        page.add("dataSeries", "[]");
      }
    } catch (final ExecutorManagerException e) {
      page.add("errorMsg", e.getMessage());
    }

    // Now for the 5 other values.
    int pageStartValue = 1;
    if (pageNum > 3) {
      pageStartValue = pageNum - 2;
    }
    final int maxPage = (numResults / pageSize) + 1;

    page.add(
        "page1",
        new PageSelection(String.valueOf(pageStartValue), pageSize,
            pageStartValue > maxPage, pageStartValue == pageNum, Math.min(
            pageStartValue, maxPage)));
    pageStartValue++;
    page.add(
        "page2",
        new PageSelection(String.valueOf(pageStartValue), pageSize,
            pageStartValue > maxPage, pageStartValue == pageNum, Math.min(
            pageStartValue, maxPage)));
    pageStartValue++;
    page.add(
        "page3",
        new PageSelection(String.valueOf(pageStartValue), pageSize,
            pageStartValue > maxPage, pageStartValue == pageNum, Math.min(
            pageStartValue, maxPage)));
    pageStartValue++;
    page.add(
        "page4",
        new PageSelection(String.valueOf(pageStartValue), pageSize,
            pageStartValue > maxPage, pageStartValue == pageNum, Math.min(
            pageStartValue, maxPage)));
    pageStartValue++;
    page.add(
        "page5",
        new PageSelection(String.valueOf(pageStartValue), pageSize,
            pageStartValue > maxPage, pageStartValue == pageNum, Math.min(
            pageStartValue, maxPage)));

    page.render();
  }

  private void handlePermissionPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/permissionspage.vm");
    final String projectName = getParam(req, "project");
    final User user = session.getUser();
    PageUtils
        .hideUploadButtonWhenNeeded(page, session, this.userManager, this.lockdownUploadProjects);
    Project project = null;
    try {
      project = this.projectManager.getProject(projectName);
      if (project == null) {
        page.add("errorMsg", "Project " + projectName + " not found.");
      } else {
        if (!hasPermission(project, user, Type.READ)) {
          throw new AccessControlException("No permission to view project "
              + projectName + ".");
        }

        page.add("project", project);
        page.add("username", user.getUserId());
        page.add("admins", Utils.flattenToString(
            project.getUsersWithPermission(Type.ADMIN), ","));
        final Permission perm = this.getPermissionObject(project, user, Type.ADMIN);
        page.add("userpermission", perm);

        if (perm.isPermissionSet(Type.ADMIN)) {
          page.add("admin", true);
        }

        final List<Pair<String, Permission>> userPermission =
            project.getUserPermissions();
        if (userPermission != null && !userPermission.isEmpty()) {
          page.add("permissions", userPermission);
        }

        final List<Pair<String, Permission>> groupPermission =
            project.getGroupPermissions();
        if (groupPermission != null && !groupPermission.isEmpty()) {
          page.add("groupPermissions", groupPermission);
        }

        final Set<String> proxyUsers = project.getProxyUsers();
        if (proxyUsers != null && !proxyUsers.isEmpty()) {
          page.add("proxyUsers", proxyUsers);
        }

        if (hasPermission(project, user, Type.ADMIN)) {
          page.add("isAdmin", true);
        }
      }
    } catch (final AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    }

    page.render();
  }

  private void handleJobPage(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/jobpage.vm");
    final String projectName = getParam(req, "project");
    final String flowName = getParam(req, "flow");
    final String jobName = getParam(req, "job");

    final User user = session.getUser();
    Project project = null;
    Flow flow = null;
    try {
      project = this.projectManager.getProject(projectName);
      logger.info("JobPage: project " + projectName + " version is " + project.getVersion()
          + ", reference is " + System.identityHashCode(project));
      if (project == null) {
        page.add("errorMsg", "Project " + projectName + " not found.");
        page.render();
        return;
      }
      if (!hasPermission(project, user, Type.READ)) {
        throw new AccessControlException("No permission to view project "
            + projectName + ".");
      }

      page.add("project", project);
      flow = project.getFlow(flowName);
      if (flow == null) {
        page.add("errorMsg", "Flow " + flowName + " not found.");
        page.render();
        return;
      }

      page.add("flowid", flow.getId());
      final Node node = flow.getNode(jobName);
      if (node == null) {
        page.add("errorMsg", "Job " + jobName + " not found.");
        page.render();
        return;
      }

      Props jobProp = this.projectManager
          .getJobOverrideProperty(project, flow, jobName, node.getJobSource());
      if (jobProp == null) {
        jobProp = this.projectManager.getProperties(project, flow, jobName, node.getJobSource());
      }

      page.add("jobid", node.getId());
      page.add("jobtype", node.getType());

      final ArrayList<String> dependencies = new ArrayList<>();
      final Set<Edge> inEdges = flow.getInEdges(node.getId());
      if (inEdges != null) {
        for (final Edge dependency : inEdges) {
          dependencies.add(dependency.getSourceId());
        }
      }
      if (!dependencies.isEmpty()) {
        page.add("dependencies", dependencies);
      }

      final ArrayList<String> dependents = new ArrayList<>();
      final Set<Edge> outEdges = flow.getOutEdges(node.getId());
      if (outEdges != null) {
        for (final Edge dependent : outEdges) {
          dependents.add(dependent.getTargetId());
        }
      }
      if (!dependents.isEmpty()) {
        page.add("dependents", dependents);
      }

      // Resolve property dependencies
      final ArrayList<String> source = new ArrayList<>();
      final String nodeSource = node.getPropsSource();
      if (nodeSource != null) {
        source.add(nodeSource);
        FlowProps parent = flow.getFlowProps(nodeSource);
        while (parent.getInheritedSource() != null) {
          source.add(parent.getInheritedSource());
          parent = flow.getFlowProps(parent.getInheritedSource());
        }
      }
      if (!source.isEmpty()) {
        page.add("properties", source);
      }

      final ArrayList<Pair<String, String>> parameters =
          new ArrayList<>();
      // Parameter
      for (final String key : jobProp.getKeySet()) {
        final String value = jobProp.get(key);
        parameters.add(new Pair<>(key, value));
      }

      page.add("parameters", parameters);
    } catch (final AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    } catch (final ProjectManagerException e) {
      page.add("errorMsg", e.getMessage());
    }
    page.render();
  }

  private void handlePropertyPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/propertypage.vm");
    final String projectName = getParam(req, "project");
    final String flowName = getParam(req, "flow");
    final String jobName = getParam(req, "job");
    final String propSource = getParam(req, "prop");

    final User user = session.getUser();
    Project project = null;
    Flow flow = null;
    try {
      project = this.projectManager.getProject(projectName);
      if (project == null) {
        page.add("errorMsg", "Project " + projectName + " not found.");
        logger.info("Display project property. Project " + projectName + " not found.");
        page.render();
        return;
      }

      if (!hasPermission(project, user, Type.READ)) {
        throw new AccessControlException("No permission to view project "
            + projectName + ".");
      }
      page.add("project", project);

      flow = project.getFlow(flowName);
      if (flow == null) {
        page.add("errorMsg", "Flow " + flowName + " not found.");
        logger.info("Display project property. Project " + projectName +
            " Flow " + flowName + " not found.");
        page.render();
        return;
      }

      page.add("flowid", flow.getId());
      final Node node = flow.getNode(jobName);
      if (node == null) {
        page.add("errorMsg", "Job " + jobName + " not found.");
        logger.info("Display project property. Project " + projectName +
            " Flow " + flowName + " Job " + jobName + " not found.");
        page.render();
        return;
      }

      final Props prop = this.projectManager.getProperties(project, flow, null, propSource);
      if (prop == null) {
        page.add("errorMsg", "Property " + propSource + " not found.");
        logger.info("Display project property. Project " + projectName +
            " Flow " + flowName + " Job " + jobName +
            " Property " + propSource + " not found.");
        page.render();
        return;

      }
      page.add("property", propSource);
      page.add("jobid", node.getId());

      // Resolve property dependencies
      final ArrayList<String> inheritProps = new ArrayList<>();
      FlowProps parent = flow.getFlowProps(propSource);
      while (parent.getInheritedSource() != null) {
        inheritProps.add(parent.getInheritedSource());
        parent = flow.getFlowProps(parent.getInheritedSource());
      }
      if (!inheritProps.isEmpty()) {
        page.add("inheritedproperties", inheritProps);
      }

      final ArrayList<String> dependingProps = new ArrayList<>();
      FlowProps child =
          flow.getFlowProps(flow.getNode(jobName).getPropsSource());
      while (!child.getSource().equals(propSource)) {
        dependingProps.add(child.getSource());
        child = flow.getFlowProps(child.getInheritedSource());
      }
      if (!dependingProps.isEmpty()) {
        page.add("dependingproperties", dependingProps);
      }

      final ArrayList<Pair<String, String>> parameters =
          new ArrayList<>();
      // Parameter
      for (final String key : prop.getKeySet()) {
        final String value = prop.get(key);
        parameters.add(new Pair<>(key, value));
      }

      page.add("parameters", parameters);
    } catch (final AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    } catch (final ProjectManagerException e) {
      page.add("errorMsg", e.getMessage());
    }

    page.render();
  }

  private void handleFlowPage(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/flowpage.vm");
    final String projectName = getParam(req, "project");
    final String flowName = getParam(req, "flow");

    final User user = session.getUser();
    Project project = null;
    Flow flow = null;
    try {
      project = this.projectManager.getProject(projectName);

      if (project == null) {
        page.add("errorMsg", "Project " + projectName + " not found.");
        page.render();
        return;
      }

      if (!hasPermission(project, user, Type.READ)) {
        throw new AccessControlException("No permission Project " + projectName
            + ".");
      }

      page.add("project", project);
      flow = project.getFlow(flowName);
      if (flow == null) {
        page.add("errorMsg", "Flow " + flowName + " not found.");
      } else {
        page.add("flowid", flow.getId());
      }
    } catch (final AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    }

    page.render();
  }

  private void handleProjectPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/projectpage.vm");
    final String projectName = getParam(req, "project");

    final User user = session.getUser();
    PageUtils
        .hideUploadButtonWhenNeeded(page, session, this.userManager, this.lockdownUploadProjects);
    Project project = null;
    try {
      project = this.projectManager.getProject(projectName);
      if (project == null) {
        page.add("errorMsg", "Project " + projectName + " not found.");
      } else {
        if (!hasPermission(project, user, Type.READ)) {
          throw new AccessControlException("No permission to view project "
              + projectName + ".");
        }

        page.add("project", project);
        page.add("admins", Utils.flattenToString(
            project.getUsersWithPermission(Type.ADMIN), ","));
        final Permission perm = this.getPermissionObject(project, user, Type.ADMIN);
        page.add("userpermission", perm);
        page.add(
            "validatorFixPrompt",
            this.projectManager.getProps().getBoolean(
                ValidatorConfigs.VALIDATOR_AUTO_FIX_PROMPT_FLAG_PARAM,
                ValidatorConfigs.DEFAULT_VALIDATOR_AUTO_FIX_PROMPT_FLAG));
        page.add(
            "validatorFixLabel",
            this.projectManager.getProps().get(
                ValidatorConfigs.VALIDATOR_AUTO_FIX_PROMPT_LABEL_PARAM));
        page.add(
            "validatorFixLink",
            this.projectManager.getProps().get(
                ValidatorConfigs.VALIDATOR_AUTO_FIX_PROMPT_LINK_PARAM));

        final boolean adminPerm = perm.isPermissionSet(Type.ADMIN);
        if (adminPerm) {
          page.add("admin", true);
        }
        // Set this so we can display execute buttons only to those who have
        // access.
        if (perm.isPermissionSet(Type.EXECUTE) || adminPerm) {
          page.add("exec", true);
        } else {
          page.add("exec", false);
        }

        final List<Flow> flows = project.getFlows().stream().filter(flow -> !flow.isEmbeddedFlow())
            .collect(Collectors.toList());

        if (!flows.isEmpty()) {
          Collections.sort(flows, FLOW_ID_COMPARATOR);
          page.add("flows", flows);
        }
      }
    } catch (final AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    }
    page.render();
  }

  private void handleCreate(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException {
    final String projectName = hasParam(req, "name") ? getParam(req, "name") : null;
    final String projectDescription =
        hasParam(req, "description") ? getParam(req, "description") : null;
    logger.info("Create project " + projectName);

    final User user = session.getUser();

    String status = null;
    String action = null;
    String message = null;
    HashMap<String, Object> params = null;

    if (this.lockdownCreateProjects &&
        !UserUtils.hasPermissionforAction(this.userManager, user, Type.CREATEPROJECTS)) {
      message =
          "User " + user.getUserId()
              + " doesn't have permission to create projects.";
      logger.info(message);
      status = "error";
    } else {
      try {
        this.projectManager.createProject(projectName, projectDescription, user);
        status = "success";
        action = "redirect";
        final String redirect = "manager?project=" + projectName;
        params = new HashMap<>();
        params.put("path", redirect);
      } catch (final ProjectManagerException e) {
        message = e.getMessage();
        status = "error";
      }
    }
    final String response = createJsonResponse(status, message, action, params);
    try {
      final Writer write = resp.getWriter();
      write.append(response);
      write.flush();
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }

  private void registerError(final Map<String, String> ret, final String error,
      final HttpServletResponse resp, final int returnCode) {
    ret.put("error", error);
    resp.setStatus(returnCode);
  }

  private void ajaxHandleUpload(final HttpServletRequest req, final HttpServletResponse resp,
      final Map<String, String> ret, final Map<String, Object> multipart, final Session session)
      throws ServletException, IOException {
    final User user = session.getUser();
    final String projectName = (String) multipart.get("project");
    final Project project = this.projectManager.getProject(projectName);
    if (!project.isActive()) {
      registerError(ret, "Installation Failed. Project '" + project.getName()
          + "' was already removed.", resp, 410);
      return;
    }
    logger.info(
        "Upload: reference of project " + projectName + " is " + System.identityHashCode(project));

    final String autoFix = (String) multipart.get("fix");

    final Props props = new Props();
    if (autoFix != null && autoFix.equals("off")) {
      props.put(ValidatorConfigs.CUSTOM_AUTO_FIX_FLAG_PARAM, "false");
    } else {
      props.put(ValidatorConfigs.CUSTOM_AUTO_FIX_FLAG_PARAM, "true");
    }

    if (this.lockdownUploadProjects && !UserUtils
        .hasPermissionforAction(this.userManager, user, Type.UPLOADPROJECTS)) {
      final String message =
          "Project uploading is locked out. Only admin users and users with special permissions can upload projects. "
              + "User " + user.getUserId() + " doesn't have permission to upload project.";
      logger.info(message);
      registerError(ret, message, resp, 403);
    } else if (projectName == null || projectName.isEmpty()) {
      registerError(ret, "No project name found.", resp, 400);
    } else if (project == null) {
      registerError(ret, "Installation Failed. Project '" + projectName
          + "' doesn't exist.", resp, 400);
    } else if (!hasPermission(project, user, Type.WRITE)) {
      registerError(ret, "Installation Failed. User '" + user.getUserId()
          + "' does not have write access.", resp, 400);
    } else {
      ret.put("projectId", String.valueOf(project.getId()));

      final FileItem item = (FileItem) multipart.get("file");
      final String name = item.getName();
      String type = null;

      final String contentType = item.getContentType();
      if (contentType != null
          && (contentType.startsWith(APPLICATION_ZIP_MIME_TYPE)
          || contentType.startsWith("application/x-zip-compressed") || contentType
          .startsWith("application/octet-stream"))) {
        type = "zip";
      } else {
        item.delete();
        registerError(ret, "File type " + contentType + " unrecognized.", resp, 400);

        return;
      }

      final File tempDir = Utils.createTempDir();
      OutputStream out = null;
      try {
        logger.info("Uploading file " + name);
        final File archiveFile = new File(tempDir, name);
        out = new BufferedOutputStream(new FileOutputStream(archiveFile));
        IOUtils.copy(item.getInputStream(), out);
        out.close();

        //unscheduleall/scheduleall should only work with flow which has defined flow trigger
        //unschedule all flows within the old project
        if (this.enableQuartz) {
          //todo chengren311: should maintain atomicity,
          // e.g, if uploadProject fails, associated schedule shouldn't be added.
          this.scheduler.unscheduleAll(project);
        }
        final Map<String, ValidationReport> reports =
            this.projectManager.uploadProject(project, archiveFile, type, user,
                props);

        if (this.enableQuartz) {
          //schedule the new project
          this.scheduler.scheduleAll(project, user.getUserId());
        }
        final StringBuffer errorMsgs = new StringBuffer();
        final StringBuffer warnMsgs = new StringBuffer();
        for (final Entry<String, ValidationReport> reportEntry : reports.entrySet()) {
          final ValidationReport report = reportEntry.getValue();
          if (!report.getInfoMsgs().isEmpty()) {
            for (final String msg : report.getInfoMsgs()) {
              switch (ValidationReport.getInfoMsgLevel(msg)) {
                case ERROR:
                  errorMsgs.append(ValidationReport.getInfoMsg(msg) + "<br/>");
                  break;
                case WARN:
                  warnMsgs.append(ValidationReport.getInfoMsg(msg) + "<br/>");
                  break;
                default:
                  break;
              }
            }
          }
          if (!report.getErrorMsgs().isEmpty()) {
            errorMsgs.append("Validator " + reportEntry.getKey()
                + " reports errors:<ul>");
            for (final String msg : report.getErrorMsgs()) {
              errorMsgs.append("<li>" + msg + "</li>");
            }
            errorMsgs.append("</ul>");
          }
          if (!report.getWarningMsgs().isEmpty()) {
            warnMsgs.append("Validator " + reportEntry.getKey()
                + " reports warnings:<ul>");
            for (final String msg : report.getWarningMsgs()) {
              warnMsgs.append("<li>" + msg + "</li>");
            }
            warnMsgs.append("</ul>");
          }
        }
        if (errorMsgs.length() > 0) {
          // If putting more than 4000 characters in the cookie, the entire
          // message
          // will somehow get discarded.
          registerError(ret, errorMsgs.length() > 4000 ? errorMsgs.substring(0, 4000)
              : errorMsgs.toString(), resp, 500);
        }
        if (warnMsgs.length() > 0) {
          ret.put(
              "warn",
              warnMsgs.length() > 4000 ? warnMsgs.substring(0, 4000) : warnMsgs
                  .toString());
        }
      } catch (final Exception e) {
        logger.info("Installation Failed.", e);
        String error = e.getMessage();
        if (error.length() > 512) {
          error =
              error.substring(0, 512) + "<br>Too many errors to display.<br>";
        }
        registerError(ret, "Installation Failed.<br>" + error, resp, 500);
      } finally {
        if (out != null) {
          out.close();
        }
        if (tempDir.exists()) {
          FileUtils.deleteDirectory(tempDir);
        }
      }

      logger.info("Upload: project " + projectName + " version is " + project.getVersion()
          + ", reference is " + System.identityHashCode(project));
      ret.put("version", String.valueOf(project.getVersion()));
    }
  }

  private void handleUpload(final HttpServletRequest req, final HttpServletResponse resp,
      final Map<String, Object> multipart, final Session session) throws ServletException,
      IOException {
    final HashMap<String, String> ret = new HashMap<>();
    final String projectName = (String) multipart.get("project");
    ajaxHandleUpload(req, resp, ret, multipart, session);

    if (ret.containsKey("error")) {
      setErrorMessageInCookie(resp, ret.get("error"));
    }

    if (ret.containsKey("warn")) {
      setWarnMessageInCookie(resp, ret.get("warn"));
    }

    resp.sendRedirect(req.getRequestURI() + "?project=" + projectName);
  }

  private Permission getPermissionObject(final Project project, final User user,
      final Permission.Type type) {
    final Permission perm = project.getCollectivePermission(user);

    for (final String roleName : user.getRoles()) {
      final Role role = this.userManager.getRole(roleName);
      perm.addPermissions(role.getPermission());
    }

    return perm;
  }

  private void handleReloadProjectWhitelist(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws IOException {
    final HashMap<String, Object> ret = new HashMap<>();

    if (hasPermission(session.getUser(), Permission.Type.ADMIN)) {
      try {
        if (this.projectManager.loadProjectWhiteList()) {
          ret.put("success", "Project whitelist re-loaded!");
        } else {
          ret.put("error", "azkaban.properties doesn't contain property "
              + ProjectWhitelist.XML_FILE_PARAM);
        }
      } catch (final Exception e) {
        ret.put("error",
            "Exception occurred while trying to re-load project whitelist: "
                + e);
      }
    } else {
      ret.put("error", "Provided session doesn't have admin privilege.");
    }

    this.writeJSON(resp, ret);
  }

  protected boolean hasPermission(final User user, final Permission.Type type) {
    for (final String roleName : user.getRoles()) {
      final Role role = this.userManager.getRole(roleName);
      if (role.getPermission().isPermissionSet(type)
          || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
        return true;
      }
    }

    return false;
  }

  private static class NodeLevelComparator implements Comparator<Node> {

    @Override
    public int compare(final Node node1, final Node node2) {
      return node1.getLevel() - node2.getLevel();
    }
  }

  public static class PageSelection {

    private final String page;
    private final int size;
    private final boolean disabled;
    private final int nextPage;
    private boolean selected;

    public PageSelection(final String pageName, final int size, final boolean disabled,
        final boolean selected, final int nextPage) {
      this.page = pageName;
      this.size = size;
      this.disabled = disabled;
      this.setSelected(selected);
      this.nextPage = nextPage;
    }

    public String getPage() {
      return this.page;
    }

    public int getSize() {
      return this.size;
    }

    public boolean getDisabled() {
      return this.disabled;
    }

    public boolean isSelected() {
      return this.selected;
    }

    public void setSelected(final boolean selected) {
      this.selected = selected;
    }

    public int getNextPage() {
      return this.nextPage;
    }
  }
}
