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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableJobInfo;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;
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
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;

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

  private ProjectManager projectManager;
  private ExecutorManagerAdapter executorManager;
  private ScheduleManager scheduleManager;
  private UserManager userManager;
  private int downloadBufferSize;

  private boolean lockdownCreateProjects = false;
  private boolean lockdownUploadProjects = false;

  private static Comparator<Flow> FLOW_ID_COMPARATOR = new Comparator<Flow>() {
    @Override
    public int compare(Flow f1, Flow f2) {
      return f1.getId().compareTo(f2.getId());
    }
  };

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    projectManager = server.getProjectManager();
    executorManager = server.getExecutorManager();
    scheduleManager = server.getScheduleManager();
    userManager = server.getUserManager();
    lockdownCreateProjects =
        server.getServerProps().getBoolean(LOCKDOWN_CREATE_PROJECTS_KEY, false);
    if (lockdownCreateProjects) {
      logger.info("Creation of projects is locked down");
    }
    
    lockdownUploadProjects =
        server.getServerProps().getBoolean(LOCKDOWN_UPLOAD_PROJECTS_KEY, false);
    if (lockdownUploadProjects) {
      logger.info("Uploading of projects is locked down");
    }
    
    downloadBufferSize =
        server.getServerProps().getInt(PROJECT_DOWNLOAD_BUFFER_SIZE_IN_BYTES,
            8192);

    logger.info("downloadBufferSize: " + downloadBufferSize);
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
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

    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/projectpage.vm");
    page.add("errorMsg", "No project set.");
    page.render();
  }

  @Override
  protected void handleMultiformPost(HttpServletRequest req,
      HttpServletResponse resp, Map<String, Object> params, Session session)
      throws ServletException, IOException {
    // Looks like a duplicate, but this is a move away from the regular
    // multiform post + redirect
    // to a more ajax like command.
    if (params.containsKey("ajax")) {
      String action = (String) params.get("ajax");
      HashMap<String, String> ret = new HashMap<String, String>();
      if (action.equals("upload")) {
        ajaxHandleUpload(req, ret, params, session);
      }
      this.writeJSON(resp, ret);
    } else if (params.containsKey("action")) {
      String action = (String) params.get("action");
      if (action.equals("upload")) {
        handleUpload(req, resp, params, session);
      }
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "action")) {
      String action = getParam(req, "action");
      if (action.equals("create")) {
        handleCreate(req, resp, session);
      }
    }
  }

  private void handleAJAXAction(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    String projectName = getParam(req, "project");
    User user = session.getUser();

    HashMap<String, Object> ret = new HashMap<>();
    ret.put("project", projectName);

    Project project = projectManager.getProject(projectName);
    if (project == null) {
      ret.put("error", "Project " + projectName + " doesn't exist.");
    } else {
      ret.put("projectId", project.getId());
      String ajaxName = getParam(req, "ajax");
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
          ajaxSetJobOverrideProperty(project, ret, req);
        }
      } else {
        ret.put("error", "Cannot execute command " + ajaxName);
      }
    }

    this.writeJSON(resp, ret);
  }

  private boolean handleAjaxPermission(Project project, User user, Type type,
      Map<String, Object> ret) {
    if (hasPermission(project, user, type)) {
      return true;
    }

    ret.put("error", "Permission denied. Need " + type.toString() + " access.");
    return false;
  }

  private void ajaxFetchProjectLogEvents(Project project,
      HttpServletRequest req, HashMap<String, Object> ret) throws ServletException {
    int num = this.getIntParam(req, "size", 1000);
    int skip = this.getIntParam(req, "skip", 0);

    List<ProjectLogEvent> logEvents;
    try {
      logEvents = projectManager.getProjectEventLogs(project, num, skip);
    } catch (ProjectManagerException e) {
      throw new ServletException(e);
    }

    String[] columns = new String[] { "user", "time", "type", "message" };
    ret.put("columns", columns);

    List<Object[]> eventData = new ArrayList<>();
    for (ProjectLogEvent events : logEvents) {
      Object[] entry = new Object[4];
      entry[0] = events.getUser();
      entry[1] = events.getTime();
      entry[2] = events.getType();
      entry[3] = events.getMessage();

      eventData.add(entry);
    }

    ret.put("logData", eventData);
  }

  private List<String> getFlowJobTypes(Flow flow) {
    Set<String> jobTypeSet = new HashSet<String>();
    for (Node node : flow.getNodes()) {
      jobTypeSet.add(node.getType());
    }
    List<String> jobTypes = new ArrayList<String>();
    jobTypes.addAll(jobTypeSet);
    return jobTypes;
  }

  private void ajaxFetchFlowDetails(Project project,
      HashMap<String, Object> ret, HttpServletRequest req)
      throws ServletException {
    String flowName = getParam(req, "flow");

    Flow flow = null;
    try {
      flow = project.getFlow(flowName);
      if (flow == null) {
        ret.put("error", "Flow " + flowName + " not found.");
        return;
      }

      ret.put("jobTypes", getFlowJobTypes(flow));
    } catch (AccessControlException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxFetchLastSuccessfulFlowExecution(Project project,
      HashMap<String, Object> ret, HttpServletRequest req)
      throws ServletException {
    String flowId = getParam(req, "flow");
    List<ExecutableFlow> exFlows = null;
    try {
      exFlows =
          executorManager.getExecutableFlows(project.getId(), flowId, 0, 1,
              Status.SUCCEEDED);
    } catch (ExecutorManagerException e) {
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

  private void ajaxFetchFlowExecutions(Project project,
      HashMap<String, Object> ret, HttpServletRequest req)
      throws ServletException {
    String flowId = getParam(req, "flow");
    int from = Integer.valueOf(getParam(req, "start"));
    int length = Integer.valueOf(getParam(req, "length"));

    ArrayList<ExecutableFlow> exFlows = new ArrayList<ExecutableFlow>();
    int total = 0;
    try {
      total =
          executorManager.getExecutableFlows(project.getId(), flowId, from,
              length, exFlows);
    } catch (ExecutorManagerException e) {
      ret.put("error", "Error retrieving executable flows");
    }

    ret.put("flow", flowId);
    ret.put("total", total);
    ret.put("from", from);
    ret.put("length", length);

    ArrayList<Object> history = new ArrayList<Object>();
    for (ExecutableFlow flow : exFlows) {
      HashMap<String, Object> flowInfo = new HashMap<String, Object>();
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
   *
   * @param req
   * @param resp
   * @param session
   * @throws ServletException
   * @throws IOException
   */
  private void handleDownloadProject(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {

    User user = session.getUser();
    String projectName = getParam(req, "project");
    logger.info(user.getUserId() + " is downloading project: " + projectName);

    Project project = projectManager.getProject(projectName);
    if (project == null) {
      this.setErrorMessageInCookie(resp, "Project " + projectName
          + " doesn't exist.");
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
          projectManager.getProjectFileHandler(project, version);
      if (projectFileHandler == null) {
        this.setErrorMessageInCookie(resp, "Project " + projectName
            + " with version " + version + " doesn't exist");
        resp.sendRedirect(req.getContextPath());
        return;
      }
      File projectZipFile = projectFileHandler.getLocalFile();
      String logStr =
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

      String headerKey = "Content-Disposition";
      String headerValue =
          String.format("attachment; filename=\"%s\"",
              projectFileHandler.getFileName());
      resp.setHeader(headerKey, headerValue);
      resp.setHeader("version",
          Integer.toString(projectFileHandler.getVersion()));
      resp.setHeader("projectId",
          Integer.toString(projectFileHandler.getProjectId()));

      outStream = resp.getOutputStream();

      byte[] buffer = new byte[downloadBufferSize];
      int bytesRead = -1;

      while ((bytesRead = inStream.read(buffer)) != -1) {
        outStream.write(buffer, 0, bytesRead);
      }

    } catch (Throwable e) {
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
   * validate readiness of a project and user permission and use projectManager
   * to purge the project if things looks good
   **/
  private void handlePurgeProject(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    User user = session.getUser();
    HashMap<String, Object> ret = new HashMap<String, Object>();
    boolean isOperationSuccessful = true;

    try {
      Project project = null;
      String projectParam = getParam(req, "project");

      if (StringUtils.isNumeric(projectParam)) {
        project = projectManager.getProject(Integer.parseInt(projectParam)); // get
                                                                             // project
                                                                             // by
                                                                             // Id
      } else {
        project = projectManager.getProject(projectParam); // get project by
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
          && projectManager.isActiveProject(project.getId())) {
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
        projectManager.purgeProject(project, user);
      }
    } catch (Exception e) {
      ret.put("error", e.getMessage());
      isOperationSuccessful = false;
    }

    ret.put("success", isOperationSuccessful);
    this.writeJSON(resp, ret);
  }

  private void handleRemoveProject(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    User user = session.getUser();
    String projectName = getParam(req, "project");

    Project project = projectManager.getProject(projectName);
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

    // Check if scheduled
    Schedule sflow = null;
    try {
      for (Schedule flow : scheduleManager.getSchedules()) {

        if (flow.getProjectId() == project.getId()) {
          sflow = flow;
          break;
        }
      }
    } catch (ScheduleManagerException e) {
      throw new ServletException(e);
    }

    if (sflow != null) {
      this.setErrorMessageInCookie(resp, "Cannot delete. Please unschedule "
          + sflow.getScheduleName() + ".");

      resp.sendRedirect(req.getRequestURI() + "?project=" + projectName);
      return;
    }

    // Check if executing
    ExecutableFlow exflow = null;
    for (ExecutableFlow flow : executorManager.getRunningFlows()) {
      if (flow.getProjectId() == project.getId()) {
        exflow = flow;
        break;
      }
    }
    if (exflow != null) {
      this.setErrorMessageInCookie(resp, "Cannot delete. Executable flow "
          + exflow.getExecutionId() + " is still running.");
      resp.sendRedirect(req.getRequestURI() + "?project=" + projectName);
      return;
    }

    try {
      projectManager.removeProject(project, user);
    } catch (ProjectManagerException e) {
      this.setErrorMessageInCookie(resp, e.getMessage());
      resp.sendRedirect(req.getRequestURI() + "?project=" + projectName);
      return;
    }

    this.setSuccessMessageInCookie(resp, "Project '" + projectName
        + "' was successfully deleted.");
    resp.sendRedirect(req.getContextPath());
  }

  private void ajaxChangeDescription(Project project,
      HashMap<String, Object> ret, HttpServletRequest req, User user)
      throws ServletException {
    String description = getParam(req, "description");
    project.setDescription(description);

    try {
      projectManager.updateProjectDescription(project, description, user);
    } catch (ProjectManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxFetchJobInfo(Project project, HashMap<String, Object> ret,
      HttpServletRequest req) throws ServletException {
    String flowName = getParam(req, "flowName");
    String jobName = getParam(req, "jobName");

    Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put("error",
          "Flow " + flowName + " not found in project " + project.getName());
      return;
    }

    Node node = flow.getNode(jobName);
    if (node == null) {
      ret.put("error", "Job " + jobName + " not found in flow " + flowName);
      return;
    }

    Props prop;
    try {
      prop = projectManager.getProperties(project, node.getJobSource());
    } catch (ProjectManagerException e) {
      ret.put("error", "Failed to retrieve job properties!");
      return;
    }

    Props overrideProp;
    try {
      overrideProp = projectManager.getJobOverrideProperty(project, jobName);
    } catch (ProjectManagerException e) {
      ret.put("error", "Failed to retrieve job override properties!");
      return;
    }

    ret.put("jobName", node.getId());
    ret.put("jobType", prop.get("type"));

    if (overrideProp == null) {
      overrideProp = new Props(prop);
    }

    Map<String, String> generalParams = new HashMap<String, String>();
    Map<String, String> overrideParams = new HashMap<String, String>();
    for (String ps : prop.getKeySet()) {
      generalParams.put(ps, prop.getString(ps));
    }
    for (String ops : overrideProp.getKeySet()) {
      overrideParams.put(ops, overrideProp.getString(ops));
    }
    ret.put("generalParams", generalParams);
    ret.put("overrideParams", overrideParams);
  }

  private void ajaxSetJobOverrideProperty(Project project,
      HashMap<String, Object> ret, HttpServletRequest req)
      throws ServletException {
    String flowName = getParam(req, "flowName");
    String jobName = getParam(req, "jobName");

    Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put("error",
          "Flow " + flowName + " not found in project " + project.getName());
      return;
    }

    Node node = flow.getNode(jobName);
    if (node == null) {
      ret.put("error", "Job " + jobName + " not found in flow " + flowName);
      return;
    }

    Map<String, String> jobParamGroup = this.getParamGroup(req, "jobOverride");
    @SuppressWarnings("unchecked")
    Props overrideParams = new Props(null, jobParamGroup);
    try {
      projectManager.setJobOverrideProperty(project, overrideParams, jobName);
    } catch (ProjectManagerException e) {
      ret.put("error", "Failed to upload job override property");
    }

  }

  private void ajaxFetchProjectFlows(Project project,
      HashMap<String, Object> ret, HttpServletRequest req)
      throws ServletException {
    ArrayList<Map<String, Object>> flowList =
        new ArrayList<Map<String, Object>>();
    for (Flow flow : project.getFlows()) {
      HashMap<String, Object> flowObj = new HashMap<String, Object>();
      flowObj.put("flowId", flow.getId());
      flowList.add(flowObj);
    }

    ret.put("flows", flowList);
  }

  private void ajaxFetchFlowGraph(Project project, HashMap<String, Object> ret,
      HttpServletRequest req) throws ServletException {
    String flowId = getParam(req, "flow");

    fillFlowInfo(project, flowId, ret);
  }

  private void fillFlowInfo(Project project, String flowId,
      HashMap<String, Object> ret) {
    Flow flow = project.getFlow(flowId);

    ArrayList<Map<String, Object>> nodeList =
        new ArrayList<Map<String, Object>>();
    for (Node node : flow.getNodes()) {
      HashMap<String, Object> nodeObj = new HashMap<String, Object>();
      nodeObj.put("id", node.getId());
      nodeObj.put("type", node.getType());
      if (node.getEmbeddedFlowId() != null) {
        nodeObj.put("flowId", node.getEmbeddedFlowId());
        fillFlowInfo(project, node.getEmbeddedFlowId(), nodeObj);
      }

      nodeList.add(nodeObj);
      Set<Edge> inEdges = flow.getInEdges(node.getId());
      if (inEdges != null && !inEdges.isEmpty()) {
        ArrayList<String> inEdgesList = new ArrayList<String>();
        for (Edge edge : inEdges) {
          inEdgesList.add(edge.getSourceId());
        }
        Collections.sort(inEdgesList);
        nodeObj.put("in", inEdgesList);
      }
    }

    Collections.sort(nodeList, new Comparator<Map<String, Object>>() {
      @Override
      public int compare(Map<String, Object> o1, Map<String, Object> o2) {
        String id = (String) o1.get("id");
        return id.compareTo((String) o2.get("id"));
      }
    });

    ret.put("flow", flowId);
    ret.put("nodes", nodeList);
  }

  private void ajaxFetchFlowNodeData(Project project,
      HashMap<String, Object> ret, HttpServletRequest req)
      throws ServletException {
    String flowId = getParam(req, "flow");
    Flow flow = project.getFlow(flowId);

    String nodeId = getParam(req, "node");
    Node node = flow.getNode(nodeId);

    if (node == null) {
      ret.put("error", "Job " + nodeId + " doesn't exist.");
      return;
    }

    ret.put("id", nodeId);
    ret.put("flow", flowId);
    ret.put("type", node.getType());

    Props props;
    try {
      props = projectManager.getProperties(project, node.getJobSource());
    } catch (ProjectManagerException e) {
      ret.put("error", "Failed to upload job override property for " + nodeId);
      return;
    }

    if (props == null) {
      ret.put("error", "Properties for " + nodeId + " isn't found.");
      return;
    }

    Map<String, String> properties = PropsUtils.toStringMap(props, true);
    ret.put("props", properties);

    if (node.getType().equals("flow")) {
      if (node.getEmbeddedFlowId() != null) {
        fillFlowInfo(project, node.getEmbeddedFlowId(), ret);
      }
    }
  }

  private void ajaxFetchFlow(Project project, HashMap<String, Object> ret,
      HttpServletRequest req) throws ServletException {
    String flowId = getParam(req, "flow");
    Flow flow = project.getFlow(flowId);

    ArrayList<Node> flowNodes = new ArrayList<Node>(flow.getNodes());
    Collections.sort(flowNodes, NODE_LEVEL_COMPARATOR);

    ArrayList<Object> nodeList = new ArrayList<Object>();
    for (Node node : flowNodes) {
      HashMap<String, Object> nodeObj = new HashMap<String, Object>();
      nodeObj.put("id", node.getId());

      ArrayList<String> dependencies = new ArrayList<String>();
      Collection<Edge> collection = flow.getInEdges(node.getId());
      if (collection != null) {
        for (Edge edge : collection) {
          dependencies.add(edge.getSourceId());
        }
      }

      ArrayList<String> dependents = new ArrayList<String>();
      collection = flow.getOutEdges(node.getId());
      if (collection != null) {
        for (Edge edge : collection) {
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

  private void ajaxAddProxyUser(Project project, HashMap<String, Object> ret,
      HttpServletRequest req, User user) throws ServletException {
    String name = getParam(req, "name");

    logger.info("Adding proxy user " + name + " by " + user.getUserId());
    if (userManager.validateProxyUser(name, user)) {
      try {
        projectManager.addProjectProxyUser(project, name, user);
      } catch (ProjectManagerException e) {
        ret.put("error", e.getMessage());
      }
    } else {
      ret.put("error", "User " + user.getUserId()
          + " has no permission to add " + name + " as proxy user.");
      return;
    }
  }

  private void ajaxRemoveProxyUser(Project project,
      HashMap<String, Object> ret, HttpServletRequest req, User user)
      throws ServletException {
    String name = getParam(req, "name");

    logger.info("Removing proxy user " + name + " by " + user.getUserId());

    try {
      projectManager.removeProjectProxyUser(project, name, user);
    } catch (ProjectManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxAddPermission(Project project, HashMap<String, Object> ret,
      HttpServletRequest req, User user) throws ServletException {
    String name = getParam(req, "name");
    boolean group = Boolean.parseBoolean(getParam(req, "group"));

    if (group) {
      if (project.getGroupPermission(name) != null) {
        ret.put("error", "Group permission already exists.");
        return;
      }
      if (!userManager.validateGroup(name)) {
        ret.put("error", "Group is invalid.");
        return;
      }
    } else {
      if (project.getUserPermission(name) != null) {
        ret.put("error", "User permission already exists.");
        return;
      }
      if (!userManager.validateUser(name)) {
        ret.put("error", "User is invalid.");
        return;
      }
    }

    boolean admin = Boolean.parseBoolean(getParam(req, "permissions[admin]"));
    boolean read = Boolean.parseBoolean(getParam(req, "permissions[read]"));
    boolean write = Boolean.parseBoolean(getParam(req, "permissions[write]"));
    boolean execute =
        Boolean.parseBoolean(getParam(req, "permissions[execute]"));
    boolean schedule =
        Boolean.parseBoolean(getParam(req, "permissions[schedule]"));

    Permission perm = new Permission();
    if (admin) {
      perm.setPermission(Type.ADMIN, true);
    } else {
      perm.setPermission(Type.READ, read);
      perm.setPermission(Type.WRITE, write);
      perm.setPermission(Type.EXECUTE, execute);
      perm.setPermission(Type.SCHEDULE, schedule);
    }

    try {
      projectManager.updateProjectPermission(project, name, perm, group, user);
    } catch (ProjectManagerException e) {
      ret.put("error", e.getMessage());
    }
  }

  private void ajaxChangePermissions(Project project,
      HashMap<String, Object> ret, HttpServletRequest req, User user)
      throws ServletException {
    boolean admin = Boolean.parseBoolean(getParam(req, "permissions[admin]"));
    boolean read = Boolean.parseBoolean(getParam(req, "permissions[read]"));
    boolean write = Boolean.parseBoolean(getParam(req, "permissions[write]"));
    boolean execute =
        Boolean.parseBoolean(getParam(req, "permissions[execute]"));
    boolean schedule =
        Boolean.parseBoolean(getParam(req, "permissions[schedule]"));

    boolean group = Boolean.parseBoolean(getParam(req, "group"));

    String name = getParam(req, "name");
    Permission perm;
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
        projectManager
            .updateProjectPermission(project, name, perm, group, user);
      } catch (ProjectManagerException e) {
        ret.put("error", e.getMessage());
      }
    } else {
      try {
        projectManager.removeProjectPermission(project, name, group, user);
      } catch (ProjectManagerException e) {
        ret.put("error", e.getMessage());
      }
    }
  }

  /**
   * this only returns user permissions, but not group permissions and proxy
   * users
   *
   * @param project
   * @param ret
   */
  private void ajaxGetPermissions(Project project, HashMap<String, Object> ret) {
    ArrayList<HashMap<String, Object>> permissions =
        new ArrayList<HashMap<String, Object>>();
    for (Pair<String, Permission> perm : project.getUserPermissions()) {
      HashMap<String, Object> permObj = new HashMap<String, Object>();
      String userId = perm.getFirst();
      permObj.put("username", userId);
      permObj.put("permission", perm.getSecond().toStringArray());

      permissions.add(permObj);
    }

    ret.put("permissions", permissions);
  }

  private void ajaxGetGroupPermissions(Project project,
      HashMap<String, Object> ret) {
    ArrayList<HashMap<String, Object>> permissions =
        new ArrayList<HashMap<String, Object>>();
    for (Pair<String, Permission> perm : project.getGroupPermissions()) {
      HashMap<String, Object> permObj = new HashMap<String, Object>();
      String userId = perm.getFirst();
      permObj.put("username", userId);
      permObj.put("permission", perm.getSecond().toStringArray());

      permissions.add(permObj);
    }

    ret.put("permissions", permissions);
  }

  private void ajaxGetProxyUsers(Project project, HashMap<String, Object> ret) {
    String[] proxyUsers = project.getProxyUsers().toArray(new String[0]);
    ret.put("proxyUsers", proxyUsers);
  }

  private void handleProjectLogsPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/projectlogpage.vm");
    String projectName = getParam(req, "project");

    User user = session.getUser();
    Project project = null;
    try {
      project = projectManager.getProject(projectName);
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
        Permission perm = this.getPermissionObject(project, user, Type.ADMIN);
        page.add("userpermission", perm);

        boolean adminPerm = perm.isPermissionSet(Type.ADMIN);
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
    } catch (AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    }

    int numBytes = 1024;

    // Really sucks if we do a lot of these because it'll eat up memory fast.
    // But it's expected that this won't be a heavily used thing. If it is,
    // then we'll revisit it to make it more stream friendly.
    StringBuffer buffer = new StringBuffer(numBytes);
    page.add("log", buffer.toString());

    page.render();
  }

  private void handleJobHistoryPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/jobhistorypage.vm");
    String projectName = getParam(req, "project");
    User user = session.getUser();

    Project project = projectManager.getProject(projectName);
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

    String jobId = getParam(req, "job");
    int pageNum = getIntParam(req, "page", 1);
    int pageSize = getIntParam(req, "size", 25);

    page.add("projectId", project.getId());
    page.add("projectName", project.getName());
    page.add("jobid", jobId);
    page.add("page", pageNum);

    int skipPage = (pageNum - 1) * pageSize;

    int numResults = 0;
    try {
      numResults = executorManager.getNumberOfJobExecutions(project, jobId);
      int maxPage = (numResults / pageSize) + 1;
      List<ExecutableJobInfo> jobInfo =
          executorManager.getExecutableJobs(project, jobId, skipPage, pageSize);

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
        ArrayList<Object> dataSeries = new ArrayList<Object>();
        for (ExecutableJobInfo info : jobInfo) {
          Map<String, Object> map = info.toObject();
          dataSeries.add(map);
        }
        page.add("dataSeries", JSONUtils.toJSON(dataSeries));
      } else {
        page.add("dataSeries", "[]");
      }
    } catch (ExecutorManagerException e) {
      page.add("errorMsg", e.getMessage());
    }

    // Now for the 5 other values.
    int pageStartValue = 1;
    if (pageNum > 3) {
      pageStartValue = pageNum - 2;
    }
    int maxPage = (numResults / pageSize) + 1;

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

  private void handlePermissionPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/permissionspage.vm");
    String projectName = getParam(req, "project");
    User user = session.getUser();

    Project project = null;
    try {
      project = projectManager.getProject(projectName);
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
        Permission perm = this.getPermissionObject(project, user, Type.ADMIN);
        page.add("userpermission", perm);

        if (perm.isPermissionSet(Type.ADMIN)) {
          page.add("admin", true);
        }

        List<Pair<String, Permission>> userPermission =
            project.getUserPermissions();
        if (userPermission != null && !userPermission.isEmpty()) {
          page.add("permissions", userPermission);
        }

        List<Pair<String, Permission>> groupPermission =
            project.getGroupPermissions();
        if (groupPermission != null && !groupPermission.isEmpty()) {
          page.add("groupPermissions", groupPermission);
        }

        Set<String> proxyUsers = project.getProxyUsers();
        if (proxyUsers != null && !proxyUsers.isEmpty()) {
          page.add("proxyUsers", proxyUsers);
        }

        if (hasPermission(project, user, Type.ADMIN)) {
          page.add("isAdmin", true);
        }
      }
    } catch (AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    }

    page.render();
  }

  private void handleJobPage(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/jobpage.vm");
    String projectName = getParam(req, "project");
    String flowName = getParam(req, "flow");
    String jobName = getParam(req, "job");

    User user = session.getUser();
    Project project = null;
    Flow flow = null;
    try {
      project = projectManager.getProject(projectName);
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
      Node node = flow.getNode(jobName);
      if (node == null) {
        page.add("errorMsg", "Job " + jobName + " not found.");
        page.render();
        return;
      }

      Props prop = projectManager.getProperties(project, node.getJobSource());
      Props overrideProp =
          projectManager.getJobOverrideProperty(project, jobName);
      if (overrideProp == null) {
        overrideProp = new Props();
      }
      Props comboProp = new Props(prop);
      for (String key : overrideProp.getKeySet()) {
        comboProp.put(key, overrideProp.get(key));
      }
      page.add("jobid", node.getId());
      page.add("jobtype", node.getType());

      ArrayList<String> dependencies = new ArrayList<String>();
      Set<Edge> inEdges = flow.getInEdges(node.getId());
      if (inEdges != null) {
        for (Edge dependency : inEdges) {
          dependencies.add(dependency.getSourceId());
        }
      }
      if (!dependencies.isEmpty()) {
        page.add("dependencies", dependencies);
      }

      ArrayList<String> dependents = new ArrayList<String>();
      Set<Edge> outEdges = flow.getOutEdges(node.getId());
      if (outEdges != null) {
        for (Edge dependent : outEdges) {
          dependents.add(dependent.getTargetId());
        }
      }
      if (!dependents.isEmpty()) {
        page.add("dependents", dependents);
      }

      // Resolve property dependencies
      ArrayList<String> source = new ArrayList<String>();
      String nodeSource = node.getPropsSource();
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

      ArrayList<Pair<String, String>> parameters =
          new ArrayList<Pair<String, String>>();
      // Parameter
      for (String key : comboProp.getKeySet()) {
        String value = comboProp.get(key);
        parameters.add(new Pair<String, String>(key, value));
      }

      page.add("parameters", parameters);
    } catch (AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    } catch (ProjectManagerException e) {
      page.add("errorMsg", e.getMessage());
    }
    page.render();
  }

  private void handlePropertyPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/propertypage.vm");
    String projectName = getParam(req, "project");
    String flowName = getParam(req, "flow");
    String jobName = getParam(req, "job");
    String propSource = getParam(req, "prop");

    User user = session.getUser();
    Project project = null;
    Flow flow = null;
    try {
      project = projectManager.getProject(projectName);
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
      Node node = flow.getNode(jobName);
      if (node == null) {
        page.add("errorMsg", "Job " + jobName + " not found.");
        page.render();
        return;
      }

      Props prop = projectManager.getProperties(project, propSource);
      page.add("property", propSource);
      page.add("jobid", node.getId());

      // Resolve property dependencies
      ArrayList<String> inheritProps = new ArrayList<String>();
      FlowProps parent = flow.getFlowProps(propSource);
      while (parent.getInheritedSource() != null) {
        inheritProps.add(parent.getInheritedSource());
        parent = flow.getFlowProps(parent.getInheritedSource());
      }
      if (!inheritProps.isEmpty()) {
        page.add("inheritedproperties", inheritProps);
      }

      ArrayList<String> dependingProps = new ArrayList<String>();
      FlowProps child =
          flow.getFlowProps(flow.getNode(jobName).getPropsSource());
      while (!child.getSource().equals(propSource)) {
        dependingProps.add(child.getSource());
        child = flow.getFlowProps(child.getInheritedSource());
      }
      if (!dependingProps.isEmpty()) {
        page.add("dependingproperties", dependingProps);
      }

      ArrayList<Pair<String, String>> parameters =
          new ArrayList<Pair<String, String>>();
      // Parameter
      for (String key : prop.getKeySet()) {
        String value = prop.get(key);
        parameters.add(new Pair<String, String>(key, value));
      }

      page.add("parameters", parameters);
    } catch (AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    } catch (ProjectManagerException e) {
      page.add("errorMsg", e.getMessage());
    }

    page.render();
  }

  private void handleFlowPage(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/flowpage.vm");
    String projectName = getParam(req, "project");
    String flowName = getParam(req, "flow");

    User user = session.getUser();
    Project project = null;
    Flow flow = null;
    try {
      project = projectManager.getProject(projectName);
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
    } catch (AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    }

    page.render();
  }

  private void handleProjectPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/projectpage.vm");
    String projectName = getParam(req, "project");

    User user = session.getUser();
    Project project = null;
    try {
      project = projectManager.getProject(projectName);
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
        Permission perm = this.getPermissionObject(project, user, Type.ADMIN);
        page.add("userpermission", perm);
        page.add(
            "validatorFixPrompt",
            projectManager.getProps().getBoolean(
                ValidatorConfigs.VALIDATOR_AUTO_FIX_PROMPT_FLAG_PARAM,
                ValidatorConfigs.DEFAULT_VALIDATOR_AUTO_FIX_PROMPT_FLAG));
        page.add(
            "validatorFixLabel",
            projectManager.getProps().get(
                ValidatorConfigs.VALIDATOR_AUTO_FIX_PROMPT_LABEL_PARAM));
        page.add(
            "validatorFixLink",
            projectManager.getProps().get(
                ValidatorConfigs.VALIDATOR_AUTO_FIX_PROMPT_LINK_PARAM));

        boolean adminPerm = perm.isPermissionSet(Type.ADMIN);
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

        List<Flow> flows = project.getFlows();
        if (!flows.isEmpty()) {
          Collections.sort(flows, FLOW_ID_COMPARATOR);
          page.add("flows", flows);
        }
      }
    } catch (AccessControlException e) {
      page.add("errorMsg", e.getMessage());
    }
    page.render();
  }

  private void handleCreate(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException {
    String projectName = hasParam(req, "name") ? getParam(req, "name") : null;
    String projectDescription =
        hasParam(req, "description") ? getParam(req, "description") : null;
    logger.info("Create project " + projectName);

    User user = session.getUser();

    String status = null;
    String action = null;
    String message = null;
    HashMap<String, Object> params = null;

    if (lockdownCreateProjects && !hasPermissionToCreateProject(user)) {
      message =
          "User " + user.getUserId()
              + " doesn't have permission to create projects.";
      logger.info(message);
      status = "error";
    } else {
      try {
        projectManager.createProject(projectName, projectDescription, user);
        status = "success";
        action = "redirect";
        String redirect = "manager?project=" + projectName;
        params = new HashMap<String, Object>();
        params.put("path", redirect);
      } catch (ProjectManagerException e) {
        message = e.getMessage();
        status = "error";
      }
    }
    String response = createJsonResponse(status, message, action, params);
    try {
      Writer write = resp.getWriter();
      write.append(response);
      write.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void ajaxHandleUpload(HttpServletRequest req,
      Map<String, String> ret, Map<String, Object> multipart, Session session)
      throws ServletException, IOException {
    User user = session.getUser();
    String projectName = (String) multipart.get("project");
    Project project = projectManager.getProject(projectName);
    String autoFix = (String) multipart.get("fix");
    Props props = new Props();
    if (autoFix != null && autoFix.equals("off")) {
      props.put(ValidatorConfigs.CUSTOM_AUTO_FIX_FLAG_PARAM, "false");
    } else {
      props.put(ValidatorConfigs.CUSTOM_AUTO_FIX_FLAG_PARAM, "true");
    }

    if (lockdownUploadProjects) {
      ret.put("error", "project uploading is locked out");
    } else if (projectName == null || projectName.isEmpty()) {
      ret.put("error", "No project name found.");
    } else if (project == null) {
      ret.put("error", "Installation Failed. Project '" + projectName
          + "' doesn't exist.");
    } else if (!hasPermission(project, user, Type.WRITE)) {
      ret.put("error", "Installation Failed. User '" + user.getUserId()
          + "' does not have write access.");
    } else {
      ret.put("projectId", String.valueOf(project.getId()));

      FileItem item = (FileItem) multipart.get("file");
      String name = item.getName();
      String type = null;

      final String contentType = item.getContentType();
      if (contentType != null
          && (contentType.startsWith(APPLICATION_ZIP_MIME_TYPE)
              || contentType.startsWith("application/x-zip-compressed") || contentType
                .startsWith("application/octet-stream"))) {
        type = "zip";
      } else {
        item.delete();
        ret.put("error", "File type " + contentType + " unrecognized.");

        return;
      }

      File tempDir = Utils.createTempDir();
      OutputStream out = null;
      try {
        logger.info("Uploading file " + name);
        File archiveFile = new File(tempDir, name);
        out = new BufferedOutputStream(new FileOutputStream(archiveFile));
        IOUtils.copy(item.getInputStream(), out);
        out.close();

        Map<String, ValidationReport> reports =
            projectManager.uploadProject(project, archiveFile, type, user,
                props);
        StringBuffer errorMsgs = new StringBuffer();
        StringBuffer warnMsgs = new StringBuffer();
        for (Entry<String, ValidationReport> reportEntry : reports.entrySet()) {
          ValidationReport report = reportEntry.getValue();
          if (!report.getInfoMsgs().isEmpty()) {
            for (String msg : report.getInfoMsgs()) {
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
            for (String msg : report.getErrorMsgs()) {
              errorMsgs.append("<li>" + msg + "</li>");
            }
            errorMsgs.append("</ul>");
          }
          if (!report.getWarningMsgs().isEmpty()) {
            warnMsgs.append("Validator " + reportEntry.getKey()
                + " reports warnings:<ul>");
            for (String msg : report.getWarningMsgs()) {
              warnMsgs.append("<li>" + msg + "</li>");
            }
            warnMsgs.append("</ul>");
          }
        }
        if (errorMsgs.length() > 0) {
          // If putting more than 4000 characters in the cookie, the entire
          // message
          // will somehow get discarded.
          ret.put("error",
              errorMsgs.length() > 4000 ? errorMsgs.substring(0, 4000)
                  : errorMsgs.toString());
        }
        if (warnMsgs.length() > 0) {
          ret.put(
              "warn",
              warnMsgs.length() > 4000 ? warnMsgs.substring(0, 4000) : warnMsgs
                  .toString());
        }
      } catch (Exception e) {
        logger.info("Installation Failed.", e);
        String error = e.getMessage();
        if (error.length() > 512) {
          error =
              error.substring(0, 512) + "<br>Too many errors to display.<br>";
        }
        ret.put("error", "Installation Failed.<br>" + error);
      } finally {
        if (out != null) {
          out.close();
        }
        if (tempDir.exists()) {
          FileUtils.deleteDirectory(tempDir);
        }
      }

      ret.put("version", String.valueOf(project.getVersion()));
    }
  }

  private void handleUpload(HttpServletRequest req, HttpServletResponse resp,
      Map<String, Object> multipart, Session session) throws ServletException,
      IOException {
    HashMap<String, String> ret = new HashMap<String, String>();
    String projectName = (String) multipart.get("project");
    ajaxHandleUpload(req, ret, multipart, session);

    if (ret.containsKey("error")) {
      setErrorMessageInCookie(resp, ret.get("error"));
    }

    if (ret.containsKey("warn")) {
      setWarnMessageInCookie(resp, ret.get("warn"));
    }

    resp.sendRedirect(req.getRequestURI() + "?project=" + projectName);
  }

  private static class NodeLevelComparator implements Comparator<Node> {
    @Override
    public int compare(Node node1, Node node2) {
      return node1.getLevel() - node2.getLevel();
    }
  }

  public class PageSelection {
    private String page;
    private int size;
    private boolean disabled;
    private boolean selected;
    private int nextPage;

    public PageSelection(String pageName, int size, boolean disabled,
        boolean selected, int nextPage) {
      this.page = pageName;
      this.size = size;
      this.disabled = disabled;
      this.setSelected(selected);
      this.nextPage = nextPage;
    }

    public String getPage() {
      return page;
    }

    public int getSize() {
      return size;
    }

    public boolean getDisabled() {
      return disabled;
    }

    public boolean isSelected() {
      return selected;
    }

    public int getNextPage() {
      return nextPage;
    }

    public void setSelected(boolean selected) {
      this.selected = selected;
    }
  }

  private Permission getPermissionObject(Project project, User user,
      Permission.Type type) {
    Permission perm = project.getCollectivePermission(user);

    for (String roleName : user.getRoles()) {
      Role role = userManager.getRole(roleName);
      perm.addPermissions(role.getPermission());
    }

    return perm;
  }

  private boolean hasPermissionToCreateProject(User user) {
    for (String roleName : user.getRoles()) {
      Role role = userManager.getRole(roleName);
      Permission perm = role.getPermission();
      if (perm.isPermissionSet(Permission.Type.ADMIN)
          || perm.isPermissionSet(Permission.Type.CREATEPROJECTS)) {
        return true;
      }
    }

    return false;
  }

  private void handleReloadProjectWhitelist(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();

    if (hasPermission(session.getUser(), Permission.Type.ADMIN)) {
      try {
        if (projectManager.loadProjectWhiteList()) {
          ret.put("success", "Project whitelist re-loaded!");
        } else {
          ret.put("error", "azkaban.properties doesn't contain property "
              + ProjectWhitelist.XML_FILE_PARAM);
        }
      } catch (Exception e) {
        ret.put("error",
            "Exception occurred while trying to re-load project whitelist: "
                + e);
      }
    } else {
      ret.put("error", "Provided session doesn't have admin privilege.");
    }

    this.writeJSON(resp, ret);
  }

  protected boolean hasPermission(User user, Permission.Type type) {
    for (String roleName : user.getRoles()) {
      Role role = userManager.getRole(roleName);
      if (role.getPermission().isPermissionSet(type)
          || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
        return true;
      }
    }

    return false;
  }
}
