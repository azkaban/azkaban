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
import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableJobInfo;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.ImmutableFlowProps;
import azkaban.flow.Node;
import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.project.Project;
import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLogEvent;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.project.ProjectWhitelist;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidatorConfigs;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.server.AzkabanAPI;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserUtils;
import azkaban.utils.HTMLFormElement;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.SecurityTag;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.Constants.*;
import static azkaban.Constants.ConfigurationKeys.ENABLE_SECURITY_CERT_MANAGEMENT;
import static azkaban.project.FeatureFlag.ENABLE_PROJECT_ADHOC_UPLOAD;


public class ProjectManagerServlet extends LoginAbstractAzkabanServlet {

  private static final String API_GET_PROJECT_ID = "getProjectId";
  private static final String API_FETCH_PROJECT_LOGS = "fetchProjectLogs";
  private static final String API_FETCH_FLOW_JOBS = "fetchflowjobs";
  private static final String API_FETCH_FLOW_DETAILS = "fetchflowdetails";
  private static final String API_FETCH_FLOW_GRAPH = "fetchflowgraph";
  private static final String API_FETCH_FLOW_NODE_DATA = "fetchflownodedata";
  private static final String API_FETCH_PROJECT_FLOWS = "fetchprojectflows";
  private static final String API_CHANGE_DESCRIPTION = "changeDescription";
  private static final String API_GET_PERMISSIONS = "getPermissions";
  private static final String API_GET_GROUP_PERMISSIONS = "getGroupPermissions";
  private static final String API_GET_PROXY_USERS = "getProxyUsers";
  private static final String API_CHANGE_PERMISSION = "changePermission";
  private static final String API_CHANGE_UPLOAD_SETTING = "changeUploadSetting";
  private static final String API_FETCH_UPLOAD_SETTING = "getUploadSetting";
  private static final String API_ADD_PERMISSION = "addPermission";
  private static final String API_ADD_PROXY_USER = "addProxyUser";
  private static final String API_REMOVE_PROXY_USER = "removeProxyUser";
  private static final String API_FETCH_FLOW_EXECUTIONS = "fetchFlowExecutions";
  private static final String API_FETCH_LAST_SUCCESSFUL_FLOW_EXECUTION =
      "fetchLastSuccessfulFlowExecution";
  private static final String API_FETCH_JOB_INFO = "fetchJobInfo";
  private static final String API_SET_JOB_OVERRIDE_PROPERTY = "setJobOverrideProperty";
  private static final String API_CHECK_FOR_WRITE_PERMISSION = "checkForWritePermission";
  private static final String API_SET_FLOW_LOCK = "setFlowLock";
  private static final String API_IS_FLOW_LOCKED = "isFlowLocked";
  public static final String API_UPLOAD = "upload";

  static final String FLOW_IS_LOCKED_PARAM = "isLocked";
  static final String FLOW_NAME_PARAM = "flowName";
  static final String FLOW_ID_PARAM = "flowId";
  public static final String ADHOC_UPLOAD = "adhocUpload";
  static final String ERROR_PARAM = "error";
  static final String FLOW_LOCK_ERROR_MESSAGE_PARAM = "flowLockErrorMessage";

  private static final String APPLICATION_ZIP_MIME_TYPE = "application/zip";
  private static final String PROJECT_DOWNLOAD_BUFFER_SIZE_IN_BYTES =
      "project.download.buffer.size";

  private static final long serialVersionUID = 1;
  private static final Logger logger = LoggerFactory.getLogger(ProjectManagerServlet.class);
  private static final NodeLevelComparator NODE_LEVEL_COMPARATOR = new NodeLevelComparator();
  private static final Comparator<Flow> FLOW_ID_COMPARATOR = new Comparator<Flow>() {
    @Override
    public int compare(final Flow f1, final Flow f2) {
      return f1.getId().compareTo(f2.getId());
    }
  };
  private ProjectManager projectManager;
  private ExecutorManagerAdapter executorManagerAdapter;
  private ScheduleManager scheduleManager;
  private UserManager userManager;
  private FlowTriggerScheduler scheduler;
  private int downloadBufferSize;
  private boolean lockdownCreateProjects = false;
  private boolean lockdownUploadProjects = false;
  private boolean enableQuartz = false;
  private boolean enableSecurityCertManagement = false;
  private boolean disableAdhocUploadWhenProjectUploadLocked = false;
  private boolean disableJobPropsOverrideWhenProjectUploadLocked = false;
  private String uploadPrivilegeUser;
  private Map<String, List<HTMLFormElement>> alerterPlugins;

  public ProjectManagerServlet() {
    super(createAPIEndpoints());
  }

  @VisibleForTesting
  void setProjectManager(final ProjectManager projectManager) {
    this.projectManager = projectManager;
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);

    final AzkabanWebServer server = getApplication();
    this.projectManager = server.getProjectManager();
    this.executorManagerAdapter = server.getExecutorManager();
    this.scheduleManager = server.getScheduleManager();
    this.userManager = server.getUserManager();
    this.scheduler = server.getFlowTriggerScheduler();
    this.enableSecurityCertManagement = server.getServerProps().getBoolean(ENABLE_SECURITY_CERT_MANAGEMENT, false);
    this.lockdownCreateProjects =
        server.getServerProps().getBoolean(ConfigurationKeys.LOCKDOWN_CREATE_PROJECTS_KEY, false);
    this.enableQuartz = server.getServerProps().getBoolean(ConfigurationKeys.ENABLE_QUARTZ, false);
    if (this.lockdownCreateProjects) {
      logger.info("Creation of projects is locked down");
    }

    this.lockdownUploadProjects =
        server.getServerProps().getBoolean(ConfigurationKeys.LOCKDOWN_UPLOAD_PROJECTS_KEY, false);
    if (this.lockdownUploadProjects) {
      logger.info("Uploading of projects is locked down");
    }

    this.downloadBufferSize =
        server.getServerProps().getInt(PROJECT_DOWNLOAD_BUFFER_SIZE_IN_BYTES, 8192);
    logger.info("downloadBufferSize: " + this.downloadBufferSize);

    // get upload privilege user, if not configured, treated upload as adhoc, no upload lock enabled
    // this feature flag is fundamental for project security feature enhanced by upload
    this.uploadPrivilegeUser = server.getServerProps().get(AZKABAN_UPLOAD_PRIVILEGE_USER);
    // a separate feature flag to disable adhoc upload when project upload lock is enabled
    this.disableAdhocUploadWhenProjectUploadLocked =
        server.getServerProps().getBoolean(AZKABAN_DISABLE_ADHOC_UPLOAD_ON_LOCKED, false);
    // a separate feature flag to disable job props override when project upload lock is enabled
    this.disableJobPropsOverrideWhenProjectUploadLocked =
        server.getServerProps().getBoolean(AZKABAN_DISABLE_JOB_PROPS_OVERRIDE_ON_LOCKED, false);

    final Map<String, List<HTMLFormElement>> alerterPlugins = new HashMap<>();
    server.getAlerterPlugins().forEach((name, alerter) -> alerterPlugins.put(name,
        (alerter.getViewParameters() != null ? alerter.getViewParameters()
            : Collections.emptyList())));
    this.alerterPlugins = alerterPlugins;
  }

  private static List<AzkabanAPI> createAPIEndpoints() {
    final List<AzkabanAPI> apiEndpoints = new ArrayList<>();
    apiEndpoints.add(new AzkabanAPI("ajax", API_GET_PROJECT_ID));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_PROJECT_LOGS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_FLOW_JOBS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_FLOW_DETAILS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_FLOW_GRAPH));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_FLOW_NODE_DATA));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_PROJECT_FLOWS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_CHANGE_DESCRIPTION));
    apiEndpoints.add(new AzkabanAPI("ajax", API_GET_PERMISSIONS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_GET_GROUP_PERMISSIONS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_GET_PROXY_USERS));
    apiEndpoints.add(new AzkabanAPI("ajax", API_CHANGE_PERMISSION));
    apiEndpoints.add(new AzkabanAPI("ajax", API_CHANGE_UPLOAD_SETTING));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_UPLOAD_SETTING));
    apiEndpoints.add(new AzkabanAPI("ajax", API_ADD_PERMISSION));
    apiEndpoints.add(new AzkabanAPI("ajax", API_ADD_PROXY_USER));
    apiEndpoints.add(new AzkabanAPI("ajax", API_REMOVE_PROXY_USER));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_FLOW_EXECUTIONS));
    apiEndpoints.add(new AzkabanAPI("ajax",
        API_FETCH_LAST_SUCCESSFUL_FLOW_EXECUTION));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_JOB_INFO));
    apiEndpoints.add(new AzkabanAPI("ajax", API_SET_JOB_OVERRIDE_PROPERTY));
    apiEndpoints.add(new AzkabanAPI("ajax", API_CHECK_FOR_WRITE_PERMISSION));
    apiEndpoints.add(new AzkabanAPI("ajax", API_SET_FLOW_LOCK));
    apiEndpoints.add(new AzkabanAPI("ajax", API_IS_FLOW_LOCKED));
    apiEndpoints.add(new AzkabanAPI("ajax", API_UPLOAD));

    apiEndpoints.add(new AzkabanAPI("action", API_UPLOAD));
    apiEndpoints.add(new AzkabanAPI("action", "create"));

    apiEndpoints.add(new AzkabanAPI("download", ""));
    apiEndpoints.add(new AzkabanAPI("delete", ""));
    apiEndpoints.add(new AzkabanAPI("purge", ""));
    apiEndpoints.add(new AzkabanAPI("reloadProjectWhitelist", ""));
    return apiEndpoints;
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
      return;
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
      if (API_UPLOAD.equals(action)) {
        ajaxHandleUpload(req, resp, ret, params, session);
      }
      this.writeJSON(resp, ret);
    } else if (params.containsKey("action")) {
      final String action = (String) params.get("action");
      if (API_UPLOAD.equals(action)) {
        handleUpload(req, resp, params, session);
      }
    }
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else if (hasParam(req, "action")) {
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
      ret.put(ERROR_PARAM, "Project " + projectName + " doesn't exist.");
    } else {
      ret.put("projectId", project.getId());
      final String ajaxName = getParam(req, "ajax");
      if (API_GET_PROJECT_ID.equals(ajaxName)) {
        // Do nothing, since projectId is added to all AJAX requests.
      } else if (API_FETCH_PROJECT_LOGS.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchProjectLogEvents(project, req, ret);
        }
      } else if (API_FETCH_FLOW_JOBS.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlow(project, ret, req);
        }
      } else if (API_FETCH_FLOW_DETAILS.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlowDetails(project, ret, req);
        }
      } else if (API_FETCH_FLOW_GRAPH.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlowGraph(project, ret, req);
        }
      } else if (API_FETCH_FLOW_NODE_DATA.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlowNodeData(project, ret, req);
        }
      } else if (API_FETCH_PROJECT_FLOWS.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchProjectFlows(project, ret, req);
        }
      } else if (API_CHANGE_DESCRIPTION.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.WRITE, ret)) {
          ajaxChangeDescription(project, ret, req, user);
        }
      } else if (API_GET_PERMISSIONS.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxGetPermissions(project, ret);
        }
      } else if (API_GET_GROUP_PERMISSIONS.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxGetGroupPermissions(project, ret);
        }
      } else if (API_GET_PROXY_USERS.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxGetProxyUsers(project, ret);
        }
      } else if (API_CHANGE_PERMISSION.equals(ajaxName)) {
        if (session != null && !validateCSRFToken(req)) {
          writeJSON(resp, ImmutableMap.of("error", "CSRF validation failed."));
          return;
        }
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxChangePermissions(project, ret, req, user);
        }
      } else if (API_ADD_PERMISSION.equals(ajaxName)) {
        if (session != null && !validateCSRFToken(req)) {
          writeJSON(resp, ImmutableMap.of("error", "CSRF validation failed."));
          return;
        }
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxAddPermission(project, ret, req, user);
        }
      } else if (API_CHANGE_UPLOAD_SETTING.equals(ajaxName)) {
        if (hasAzkabanAdminPermission(user)) {
          ajaxChangeUploadSetting(project, ret, req, user);
        } else {
          ret.put(ERROR_PARAM, "User " + user.getUserId() + " doesn't have permission to change "
              + "upload lock settings.");
        }
      } else if (API_FETCH_UPLOAD_SETTING.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxGetUploadSettings(project, ret);
        }
      } else if (API_ADD_PROXY_USER.equals(ajaxName)) {
        if (session != null && !validateCSRFToken(req)) {
          writeJSON(resp, ImmutableMap.of("error", "CSRF validation failed."));
          return;
        }
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxAddProxyUser(project, ret, req, user);
        }
      } else if (API_REMOVE_PROXY_USER.equals(ajaxName)) {
        if (session != null && !validateCSRFToken(req)) {
          writeJSON(resp, ImmutableMap.of("error", "CSRF validation failed."));
          return;
        }
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxRemoveProxyUser(project, ret, req, user);
        }
      } else if (API_FETCH_FLOW_EXECUTIONS.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchFlowExecutions(project, ret, req);
        }
      } else if (API_FETCH_LAST_SUCCESSFUL_FLOW_EXECUTION.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchLastSuccessfulFlowExecution(project, ret, req);
        }
      } else if (API_FETCH_JOB_INFO.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxFetchJobInfo(project, ret, req);
        }
      } else if (API_SET_JOB_OVERRIDE_PROPERTY.equals(ajaxName)) {
        if (uploadPrivilegeUser != null && disableJobPropsOverrideWhenProjectUploadLocked && project.isUploadLocked()) {
          ret.put(ERROR_PARAM, "Project " + projectName + "is locked for editing job property while upload privilege user "
              + "is set to " + uploadPrivilegeUser + ". If you really need to edit job property, "
              + "please contact oncall to remove this lock.");
          resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
          return;
        }
        if (handleAjaxPermission(project, user, Type.WRITE, ret)) {
          ajaxSetJobOverrideProperty(project, ret, req, user);
        }
      } else if (API_CHECK_FOR_WRITE_PERMISSION.equals(ajaxName)) {
        ajaxCheckForWritePermission(project, user, ret);
      } else if (API_SET_FLOW_LOCK.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.ADMIN, ret)) {
          ajaxSetFlowLock(project, ret, req);
        }
      } else if (API_IS_FLOW_LOCKED.equals(ajaxName)) {
        if (handleAjaxPermission(project, user, Type.READ, ret)) {
          ajaxIsFlowLocked(project, ret, req);
        }
      } else {
        ret.put(ERROR_PARAM, "Cannot execute command " + ajaxName);
      }
    }

    this.writeJSON(resp, ret);
  }

  private boolean handleAjaxPermission(final Project project, final User user, final Type type,
      final Map<String, Object> ret) {
    if (hasPermission(project, user, type)) {
      return true;
    }

    ret.put(ERROR_PARAM, "Permission denied. Need " + type.toString() + " access.");
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

    try {
      final Flow flow = project.getFlow(flowName);
      if (flow == null) {
        ret.put(ERROR_PARAM, "Flow " + flowName + " not found.");
        return;
      }

      ret.put("jobTypes", getFlowJobTypes(flow));
      if (flow.getCondition() != null) {
        ret.put("condition", flow.getCondition());
      }
    } catch (final AccessControlException e) {
      ret.put(ERROR_PARAM, e.getMessage());
    }
  }

  private void ajaxFetchLastSuccessfulFlowExecution(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req)
      throws ServletException {
    final String flowId = getParam(req, "flow");
    List<ExecutableFlow> exFlows = null;
    try {
      exFlows =
          this.executorManagerAdapter.getExecutableFlows(project.getId(), flowId, 0, 1,
              Status.SUCCEEDED);
    } catch (final ExecutorManagerException e) {
      ret.put(ERROR_PARAM, "Error retrieving executable flows");
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
          this.executorManagerAdapter.getExecutableFlows(project.getId(), flowId, from,
              length, exFlows);
    } catch (final ExecutorManagerException e) {
      ret.put(ERROR_PARAM, "Error retrieving executable flows");
    }

    ret.put("flow", flowId);
    ret.put("total", total);
    ret.put("from", from);
    ret.put("length", length);

    final ArrayList<Object> history = new ArrayList<>();
    for (final ExecutableFlow flow : exFlows) {
      final HashMap<String, Object> flowInfo = new HashMap<>();
      flowInfo.put("execId", flow.getExecutionId());
      flowInfo.put(FLOW_ID_PARAM, flow.getFlowId());
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
   * <p>
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
        ret.put(ERROR_PARAM, "invalid project");
        isOperationSuccessful = false;
      }

      // project is already deleted
      if (isOperationSuccessful
          && this.projectManager.isActiveProject(project.getId())) {
        ret.put(ERROR_PARAM, "Project " + project.getName()
            + " should be deleted before purging");
        isOperationSuccessful = false;
      }

      // only eligible users can purge a project
      if (isOperationSuccessful && !hasPermission(project, user, Type.ADMIN)) {
        ret.put(ERROR_PARAM, "Cannot purge. User '" + user.getUserId()
            + "' is not an ADMIN.");
        isOperationSuccessful = false;
      }

      if (isOperationSuccessful) {
        this.projectManager.purgeProject(project, user);
      }
    } catch (final Exception e) {
      ret.put(ERROR_PARAM, e.getMessage());
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
          logger.info("removing schedule {} for project {}", schedule.getScheduleId(), project.getName());
          this.scheduleManager.removeSchedule(schedule);
        }
      }
    } catch (final ScheduleManagerException e) {
      throw new ServletException(e);
    }

    // remove flow trigger schedules
    try {
      if (this.enableQuartz) {
        this.scheduler.unschedule(project);
      }
    } catch (final SchedulerException e) {
      logger.error("");
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
      ret.put(ERROR_PARAM, e.getMessage());
    }
  }

  private void ajaxChangeUploadSetting(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req, final User user) {
    if (req.getParameter(ADHOC_UPLOAD) == null) {
      ret.put(ERROR_PARAM, "adhocUpload parameter is not set");
      return;
    }
    try {
      final boolean enableAdhocProject = HttpRequestUtils.getBooleanParam(req, ADHOC_UPLOAD);
      if (enableAdhocProject) {
        project.addFeatureFlags(ENABLE_PROJECT_ADHOC_UPLOAD, true);
        project.setUploadLock(false);
      } else {
        project.addFeatureFlags(ENABLE_PROJECT_ADHOC_UPLOAD, false);
        if (project.getLastModifiedUser().equals(uploadPrivilegeUser)) {
          project.setUploadLock(true);
        }
      }
      project.setLastModifiedUser(user.getUserId());
      project.setLastModifiedTimestamp(System.currentTimeMillis());

      this.projectManager.updateProjectFeatureFlag(project, user);
    } catch (ServletException | ProjectManagerException e) {
      ret.put(ERROR_PARAM, e.getMessage());
    }
  }

  private void ajaxGetUploadSettings(final Project project, final HashMap<String, Object> ret) {
      ret.put("isUploadLocked", project.isUploadLocked());
      ret.put("adhocUpload", project.isAdhocUploadEnabled());
  }

  private void ajaxFetchJobInfo(final Project project, final HashMap<String, Object> ret,
      final HttpServletRequest req) throws ServletException {
    final String flowName = getParam(req, "flowName");
    final String jobName = getParam(req, "jobName");

    final Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put(ERROR_PARAM,
          "Flow " + flowName + " not found in project " + project.getName());
      return;
    }

    final Node node = flow.getNode(jobName);
    if (node == null) {
      ret.put(ERROR_PARAM, "Job " + jobName + " not found in flow " + flowName);
      return;
    }

    Props jobProp;
    try {
      jobProp = this.projectManager.getProperties(project, flow, jobName, node.getJobSource());
    } catch (final ProjectManagerException e) {
      ret.put(ERROR_PARAM, "Failed to retrieve job properties!");
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
      ret.put(ERROR_PARAM, "Failed to retrieve job override properties!");
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
      ret.put(ERROR_PARAM,
          "Flow " + flowName + " not found in project " + project.getName());
      return;
    }

    final Node node = flow.getNode(jobName);
    if (node == null) {
      ret.put(ERROR_PARAM, "Job " + jobName + " not found in flow " + flowName);
      return;
    }

    final Map<String, String> jobParamGroup = this.getParamGroup(req, "jobOverride");
    final Props overrideParams = new Props(null, jobParamGroup);
    try {
      this.projectManager
          .setJobOverrideProperty(project, flow, overrideParams, jobName, node.getJobSource(),
              user);
    } catch (final ProjectManagerException e) {
      ret.put(ERROR_PARAM, "Failed to upload job override property");
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
        flowObj.put(FLOW_ID_PARAM, flow.getId());
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
      ret.put(ERROR_PARAM,
          "Flow " + flowId + " not found in project " + project.getName());
      return;
    }

    final ArrayList<Map<String, Object>> nodeList =
        new ArrayList<>();
    for (final Node node : flow.getNodes()) {
      final HashMap<String, Object> nodeObj = new HashMap<>();
      nodeObj.put("id", node.getId());
      nodeObj.put("type", node.getType());
      if (node.getCondition() != null) {
        nodeObj.put("condition", node.getCondition());
      }
      if (node.getEmbeddedFlowId() != null) {
        nodeObj.put(FLOW_ID_PARAM, node.getEmbeddedFlowId());
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
      ret.put(ERROR_PARAM, "Job " + nodeId + " doesn't exist.");
      return;
    }

    ret.put("id", nodeId);
    ret.put("flow", flowId);
    ret.put("type", node.getType());

    final Props jobProps;
    try {
      jobProps = this.projectManager.getProperties(project, flow, nodeId, node.getJobSource());
    } catch (final ProjectManagerException e) {
      ret.put(ERROR_PARAM, "Failed to upload job override property for " + nodeId);
      return;
    }

    if (jobProps == null) {
      ret.put(ERROR_PARAM, "Properties for " + nodeId + " isn't found.");
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

    ret.put(FLOW_ID_PARAM, flowId);
    ret.put("nodes", nodeList);
    ret.put(FLOW_IS_LOCKED_PARAM, flow.isLocked());
  }

  private void ajaxAddProxyUser(final Project project, final HashMap<String, Object> ret,
      final HttpServletRequest req, final User user) throws ServletException {
    final String name = getParam(req, "name");

    logger.info("Adding proxy user " + name + " by " + user.getUserId());
    if (this.userManager.validateProxyUser(name, user)) {
      try {
        this.projectManager.addProjectProxyUser(project, name, user);
      } catch (final ProjectManagerException e) {
        ret.put(ERROR_PARAM, e.getMessage());
      }
    } else {
      ret.put(ERROR_PARAM, "User " + user.getUserId()
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
      ret.put(ERROR_PARAM, e.getMessage());
    }
  }

  private void ajaxAddPermission(final Project project, final HashMap<String, Object> ret,
      final HttpServletRequest req, final User user) throws ServletException {

    final String name = getParam(req, "name");
    final boolean group = Boolean.parseBoolean(getParam(req, "group"));

    if (group) {
      if (project.getGroupPermission(name) != null) {
        ret.put(ERROR_PARAM, "Group permission already exists.");
        return;
      }
      if (!this.userManager.validateGroup(name)) {
        ret.put(ERROR_PARAM, "Group is invalid.");
        return;
      }
    } else {
      if (project.getUserPermission(name) != null) {
        ret.put(ERROR_PARAM, "User permission already exists.");
        return;
      }
      if (!this.userManager.validateUser(name)) {
        ret.put(ERROR_PARAM, "User is invalid.");
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
      ret.put(ERROR_PARAM, e.getMessage());
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
      ret.put(ERROR_PARAM, "Permissions for " + name + " cannot be found.");
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
        ret.put(ERROR_PARAM, e.getMessage());
      }
    } else {
      try {
        this.projectManager.removeProjectPermission(project, name, group, user);
      } catch (final ProjectManagerException e) {
        ret.put(ERROR_PARAM, e.getMessage());
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

  private void ajaxCheckForWritePermission(final Project project, final User user,
      final HashMap<String, Object> ret) {
    ret.put("hasWritePermission", hasPermission(project, user, Type.WRITE));
  }

  /**
   * Set if a flow is locked.
   *
   * @param project the project for the flow.
   * @param ret     the return value.
   * @param req     the http request.
   */
  private void ajaxSetFlowLock(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req)
      throws ServletException {
    final String flowName = getParam(req, FLOW_NAME_PARAM);
    final Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put(ERROR_PARAM,
          "Flow " + flowName + " not found in project " + project.getName());
      return;
    }

    final boolean isLocked = Boolean.parseBoolean(getParam(req, FLOW_IS_LOCKED_PARAM));

    String flowLockErrorMessage = null;
    try {
      flowLockErrorMessage = getParam(req, FLOW_LOCK_ERROR_MESSAGE_PARAM);
    } catch (final Exception e) {
      logger.info("Unable to get flow lock error message");
    }

    // if there is a change in the locked value, then check to see if the project has a flow trigger
    // that needs to be paused/resumed.
    if (isLocked != flow.isLocked()) {
      try {
        if (this.projectManager.hasFlowTrigger(project, flow)) {
          if (isLocked) {
            if (this.scheduler.pauseFlowTriggerIfPresent(project.getId(), flow.getId())) {
              logger.info("Flow trigger for flow " + project.getName() + "." + flow.getId() +
                  " is paused");
            } else {
              logger.warn("Flow trigger for flow " + project.getName() + "." + flow.getId() +
                  " doesn't exist");
            }
          } else {
            if (this.scheduler.resumeFlowTriggerIfPresent(project.getId(), flow.getId())) {
              logger.info("Flow trigger for flow " + project.getName() + "." + flow.getId() +
                  " is resumed");
            } else {
              logger.warn("Flow trigger for flow " + project.getName() + "." + flow.getId() +
                  " doesn't exist");
            }
          }
        }
      } catch (final Exception e) {
        ret.put(ERROR_PARAM, e);
      }
    }

    flow.setLocked(isLocked);
    flow.setFlowLockErrorMessage(isLocked ? flowLockErrorMessage : null);

    ret.put(FLOW_IS_LOCKED_PARAM, flow.isLocked());
    ret.put(FLOW_ID_PARAM, flow.getId());
    ret.put(FLOW_LOCK_ERROR_MESSAGE_PARAM, flow.getFlowLockErrorMessage());
    this.projectManager.updateFlow(project, flow);
  }

  /**
   * Returns true if the flow is locked, false if it is unlocked.
   *
   * @param project the project containing the flow.
   * @param ret     the return value.
   * @param req     the http request.
   */
  private void ajaxIsFlowLocked(final Project project,
      final HashMap<String, Object> ret, final HttpServletRequest req)
      throws ServletException {
    final String flowName = getParam(req, FLOW_NAME_PARAM);

    final Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put(ERROR_PARAM,
          "Flow " + flowName + " not found in project " + project.getName());
      return;
    }

    ret.put(FLOW_ID_PARAM, flow.getId());
    ret.put(FLOW_IS_LOCKED_PARAM, flow.isLocked());
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

        page.add("projectName", project.getName());
        page.add("projectId", project.getId());
        //params for projectsidebar
        addProjectSidebarProperties(page, project);

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

    page.render();
  }

  private void handleJobHistoryPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/jobhistorypage.vm");

    final String jobId = getParam(req, "job");
    page.add("jobId", jobId);

    int pageNum = Math.max(1, getIntParam(req, "page", 1));
    page.add("page", pageNum);

    final int pageSize = Math.max(1, getIntParam(req, "size", 25));
    page.add("pageSize", pageSize);

    page.add("recordCount", 0);
    page.add("projectId", "");
    page.add("projectName", "");
    page.add("dataSeries", "[]");
    page.add("history", null);

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

    page.add("projectId", project.getId());
    page.add("projectName", project.getName());

    try {
      final int numResults = this.executorManagerAdapter.getNumberOfJobExecutions(project, jobId);
      page.add("recordCount", numResults);

      final int totalPages = ((numResults - 1) / pageSize) + 1;
      if (pageNum > totalPages) {
        pageNum = totalPages;
        page.add("page", pageNum);
      }
      final int elementsToSkip = (pageNum - 1) * pageSize;
      final List<ExecutableJobInfo> jobInfo =
          this.executorManagerAdapter.getExecutableJobs(project, jobId, elementsToSkip, pageSize);

      if (CollectionUtils.isNotEmpty(jobInfo)) {
        page.add("history", jobInfo);

        final ArrayList<Object> dataSeries = new ArrayList<>();
        for (final ExecutableJobInfo info : jobInfo) {
          final Map<String, Object> map = info.toObject();
          dataSeries.add(map);
        }
        page.add("dataSeries", JSONUtils.toJSON(dataSeries));
      }
    } catch (final ExecutorManagerException e) {
      page.add("errorMsg", e.getMessage());
    }

    page.render();
  }

  private void handlePermissionPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException, IOException {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/permissionspage.vm");
    if(!addCSRFTokenToPage(page, session)) {
      writeJSON(resp, ImmutableMap.of("error", "Unable to load the page."));
      return;
    }
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

        page.add("projectName", project.getName());
        addProjectSidebarProperties(page, project);

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

  /**
   * Project sidebar properties shared by multiple web pages are added here.
   *
   * @param page the page to add properties to
   * @param project the current project to add properties from
   * */
  private void addProjectSidebarProperties(Page page, Project project) {
    // basic project properties
    page.add("description", project.getDescription());
    page.add("createTimestamp", project.getCreateTimestamp());
    page.add("lastModifiedTimestamp", project.getLastModifiedTimestamp());
    page.add("lastModifiedUser", project.getLastModifiedUser());

    // params for project upload
    // show if a project has prod identifier
    page.add("projectUploadLock", uploadPrivilegeUser != null && project.isUploadLocked());
    page.add("adhocUpload", project.isAdhocUploadEnabled());
    page.add("showUploadLockPanel", uploadPrivilegeUser != null);
    // only show adhocUpload changeable button when this feature is enabled
    page.add("showAdhocUploadFeature",
        uploadPrivilegeUser != null && disableAdhocUploadWhenProjectUploadLocked);
    // hide upload project button when project prod identifier is set
    page.add("hideUploadProjectButton",
        uploadPrivilegeUser != null && disableAdhocUploadWhenProjectUploadLocked && project.isUploadLocked());
  }

  private void handleJobPage(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException {
    final Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/jobpage.vm");
    final String projectName = getParam(req, "project");
    final String flowNodePath = getParam(req, "flow");
    final String jobName = getParam(req, "job");

    final User user = session.getUser();
    try {
      final Project project = this.projectManager.getProject(projectName);
      logger.info("JobPage: project " + projectName + " version is " + project.getVersion()
          + ", reference is " + System.identityHashCode(project));
      if (project == null) {
        page.add("errorMsg", "Project " + projectName + " not found.");
        page.render();
        return;
      }
      if (!hasPermission(project, user, Type.READ)) {
        throw new AccessControlException("No permission to view project " + projectName + ".");
      }
      page.add("projectName", project.getName());
      page.add("hideJobPropsEdit",
          uploadPrivilegeUser != null && disableJobPropsOverrideWhenProjectUploadLocked && project.isUploadLocked());

      final Flow flow = project.getFlow(flowNodePath);
      if (flow == null) {
        page.add("errorMsg", "Flow " + flowNodePath + " not found.");
        page.render();
        return;
      }
      final String flowId = flow.getId();
      page.add("flowid", flowId);
      page.add("flowlist", flowId.split(Constants.PATH_DELIMITER, 0));
      page.add("pathDelimiter", Constants.PATH_DELIMITER);

      final Node node = flow.getNode(jobName);
      if (node == null) {
        page.add("errorMsg", "Job " + jobName + " not found.");
        page.render();
        return;
      }
      page.add("jobid", node.getId());
      page.add("jobtype", node.getType());
      if (node.getCondition() != null) {
        page.add("condition", node.getCondition());
      }

      Props jobProp = this.projectManager
          .getJobOverrideProperty(project, flow, jobName, node.getJobSource());
      if (jobProp == null) {
        jobProp = this.projectManager.getProperties(project, flow, jobName, node.getJobSource());
      }
      final List<Pair<String, String>> jobProperties = new ArrayList<>();
      for (final String key : jobProp.getKeySet()) {
        final String value = jobProp.get(key);
        jobProperties.add(new Pair<>(key, value));
      }
      page.add("jobProperties", jobProperties);

      final List<String> dependencies = new ArrayList<>();
      final Set<Edge> inEdges = flow.getInEdges(node.getId());
      if (inEdges != null) {
        for (final Edge dependency : inEdges) {
          dependencies.add(dependency.getSourceId());
        }
      }
      page.add("dependencies", dependencies);

      final List<String> dependents = new ArrayList<>();
      final Set<Edge> outEdges = flow.getOutEdges(node.getId());
      if (outEdges != null) {
        for (final Edge dependent : outEdges) {
          dependents.add(dependent.getTargetId());
        }
      }
      page.add("dependents", dependents);

      // Resolve inherited properties
      final List<Pair<String, String>> allParentFlows = flow.getParents();
      // List of triplets of NAME and NODE PATH of flows from which properties are
      // inherited as well as the FILE NAME where they are to be found.
      final List<Triple<String, String, String>> inheritedProperties = new ArrayList<>();
      final String nodePropsSource = node.getPropsSource();
      if (nodePropsSource != null) {
        if (flow.getAzkabanFlowVersion() == Constants.AZKABAN_FLOW_VERSION_2_0) {
          allParentFlows.stream().forEach(p -> inheritedProperties
              .add(Triple.of(p.getFirst(), p.getSecond(), nodePropsSource)));
        } else {
          inheritedProperties.add(Triple.of(nodePropsSource, flowId, nodePropsSource));
          ImmutableFlowProps parent = flow.getFlowProps(nodePropsSource);
          while (parent.getInheritedSource() != null) {
            final String inheritedSource = parent.getInheritedSource();
            inheritedProperties.add(Triple.of(inheritedSource, flowId, inheritedSource));
            parent = flow.getFlowProps(parent.getInheritedSource());
          }
        }
      }
      page.add("inheritedProperties", inheritedProperties);
    } catch (final AccessControlException | ProjectManagerException e) {
      page.add("errorMsg", e.getMessage());
    }
    page.render();
  }

  private void handlePropertyPage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException {
    final Page page =
        newPage(req, resp, session, "azkaban/webapp/servlet/velocity/propertypage.vm");
    final String projectName = getParam(req, "project");

    // flow and job parameters are mainly used to build the breadcrumb.
    final String flowNodePath = getParam(req, "flow");
    final String jobName = getParam(req, "job");

    // The name of the file where properties are located
    final String propsSource = getParam(req, "prop");
    // The properties that should be retrieved:
    // In Flow 1.0 is the entire .properties file. Flow and proNode parameters have same value
    // In Flow 2.0 is just the properties of provided node.
    final String propsNodePath = getParam(req, "propNode");

    final User user = session.getUser();
    try {
      final Project project = this.projectManager.getProject(projectName);
      if (project == null) {
        page.add("errorMsg", "Project " + projectName + " not found.");
        logger
            .info("Display inherited job properties. Project " + projectName + " not found.");
        page.render();
        return;
      }

      if (!hasPermission(project, user, Type.READ)) {
        throw new AccessControlException("No permission to view project " + projectName + ".");
      }
      page.add("projectName", project.getName());

      final Flow flow = project.getFlow(flowNodePath);
      if (flow == null) {
        page.add("errorMsg", "Flow " + flowNodePath + " not found.");
        logger.info("Display inherited job properties. Flow " + flowNodePath +
            " not found in Project " + projectName + ".");
        page.render();
        return;
      }
      page.add("flowid", flow.getId());
      page.add("flowlist", flow.getId().split(Constants.PATH_DELIMITER, 0));
      page.add("pathDelimiter", Constants.PATH_DELIMITER);

      final Node job = flow.getNode(jobName);
      if (job == null) {
        page.add("errorMsg", "Job " + jobName + " not found.");
        logger.info("Display inherited job properties. Job " + jobName +
            " not found in Flow " + flowNodePath + " and Project " + projectName + ".");
        page.render();
        return;
      }
      page.add("jobid", job.getId());

      final Flow propsNode = project.getFlow(propsNodePath);
      if (propsNode == null) {
        page.add("errorMsg",
            "Nested Flow " + propsNodePath + " not found in Flow " + flowNodePath + ".");
        logger.info("Display inherited job properties. Nested Flow " + propsNodePath +
            " not found in Flow " + flowNodePath + " and Project " + projectName + ".");
        page.render();
        return;
      }

      final Props propertiesProps = this.projectManager.getProperties(project, propsNode, null,
          propsSource);
      if (propertiesProps == null) {
        page.add("errorMsg", "Property file " + propsSource + " not found.");
        logger.info("Display inherited job properties. Property file " + propsSource
                + " not found in Project " + projectName + " and Flow " + flowNodePath + ".");
        page.render();
        return;

      }
      final List<Pair<String, String>> propertiesPair = new ArrayList<>();
      for (final String key : propertiesProps.getKeySet()) {
        final String value = propertiesProps.get(key);
        propertiesPair.add(new Pair<>(key, value));
      }
      page.add("properties", propertiesPair);

      String propsSourceLabel = propsSource;
      if (flow.getAzkabanFlowVersion() == Constants.AZKABAN_FLOW_VERSION_2_0) {
        propsSourceLabel = propsNodePath;
      }
      page.add("propsSourceLabel", propsSourceLabel);
      page.add("propsSource", propsSource);
      page.add("propsNodePath", propsNodePath);

      // Resolve property dependencies
      final List<String> inheritProps = new ArrayList<>();
      ImmutableFlowProps parent = flow.getFlowProps(propsSource);
      while (parent.getInheritedSource() != null) {
        inheritProps.add(parent.getInheritedSource());
        parent = flow.getFlowProps(parent.getInheritedSource());
      }
      page.add("inheritedproperties", inheritProps);

      final List<String> dependingProps = new ArrayList<>();
      ImmutableFlowProps child = flow.getFlowProps(flow.getNode(jobName).getPropsSource());
      while (!child.getSource().equals(propsSource)) {
        dependingProps.add(child.getSource());
        child = flow.getFlowProps(child.getInheritedSource());
      }
      page.add("dependingproperties", dependingProps);
    } catch (final AccessControlException | ProjectManagerException e) {
      page.add("errorMsg", e.getMessage());
    }

    page.render();
  }

  private void handleFlowPage(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException {
    final Page page =
        newPage(req, resp, session, "azkaban/webapp/servlet/velocity/flowpage.vm");
    final String projectName = getParam(req, "project");
    final String flowName = getParam(req, "flow");

    final User user = session.getUser();
    Project project;
    Flow flow;
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

      page.add("projectName", project.getName());
      page.add("projectId", project.getId());
      flow = project.getFlow(flowName);
      if (flow == null) {
        page.add("errorMsg", "Flow " + flowName + " not found.");
      } else {
        page.add("flowid", flow.getId());
        page.add("flowlist", flow.getId().split(Constants.PATH_DELIMITER, 0));
        page.add("pathDelimiter", Constants.PATH_DELIMITER);
        page.add("isLocked", flow.isLocked());
        if (flow.isLocked()) {
          final Props props = this.projectManager.getProps();
          final String flowLockErrorMessage = flow.getFlowLockErrorMessage();
          final String lockedFlowMsg = flowLockErrorMessage != null ? flowLockErrorMessage :
              String.format(props.getString(ConfigurationKeys.AZKABAN_LOCKED_FLOW_ERROR_MESSAGE,
                  Constants.DEFAULT_LOCKED_FLOW_ERROR_MESSAGE), flow.getId(), projectName);
          page.add("error_message", lockedFlowMsg);
        }
        page.add("alerterPlugins", this.alerterPlugins);
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

        page.add("projectName", project.getName());
        page.add("projectId", project.getId());
        //params for projectsidebar
        addProjectSidebarProperties(page, project);

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
      status = ERROR_PARAM;
    } else {
      try {
        this.projectManager.createProject(projectName, projectDescription, user,
            enableSecurityCertManagement ? SecurityTag.NEW_PROJECT : null);
        status = "success";
        action = "redirect";
        final String redirect = "manager?project=" + projectName;
        params = new HashMap<>();
        params.put("path", redirect);
      } catch (final ProjectManagerException e) {
        message = e.getMessage();
        status = ERROR_PARAM;
      }
    }
    final String response = AbstractAzkabanServlet
        .createJsonResponse(status, message, action, params);
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
    ret.put(ERROR_PARAM, error);
    resp.setStatus(returnCode);
  }


  private void ajaxHandleUpload(final HttpServletRequest req, final HttpServletResponse resp,
      final Map<String, String> ret, final Map<String, Object> multipart, final Session session)
      throws ServletException, IOException {
    final User user = session.getUser();
    final String projectName = (String) multipart.get("project");

    // Fetch the uploader's IP
    final String uploaderIPAddr = WebUtils.getRealClientIpAddr(req);

    final Project project = validateUploadAndGetProject(resp, ret, user, projectName);
    if (project == null) {
      return;
    }

    // fail the upload if the project is UPLOAD locked and the user is not the upload privilege user
    // and the cluster-wide disableAdhocUpload flag is set
    if (uploadPrivilegeUser != null && disableAdhocUploadWhenProjectUploadLocked
        && project.isUploadLocked() && !user.getUserId().equals(uploadPrivilegeUser)) {
      registerError(ret, "Installation Failed. Project '" + projectName + " has UPLOAD LOCK on. \n"
          + "Create a new project to use adhoc upload feature, as crt deployed project would be automatically locked.\n"
          + "If you really need enable adhoc upload on the current project, please contact oncall to remove this lock.",
          resp, HttpServletResponse.SC_FORBIDDEN);
      return;
    }
    final FileItem item = (FileItem) multipart.get("file");
    final String name = item.getName();
    final String lowercaseExtension = FilenameUtils.getExtension(name).toLowerCase();
    final Boolean hasZipExtension = lowercaseExtension.equals("zip");
    final String contentType = item.getContentType();
    if (contentType == null || !hasZipExtension ||
        (!contentType.startsWith(APPLICATION_ZIP_MIME_TYPE) &&
            !contentType.startsWith("application/x-zip-compressed") &&
            !contentType.startsWith("application/octet-stream"))) {
      item.delete();
      if (!hasZipExtension) {
        registerError(ret, "File extension '" + lowercaseExtension + "' unrecognized.", resp,
            HttpServletResponse.SC_BAD_REQUEST);
      } else {
        registerError(ret, "Content type '" + contentType + "' does not match extension '" +
            lowercaseExtension + "'", resp, HttpServletResponse.SC_BAD_REQUEST);
      }
      return;
    }

    final String autoFix = (String) multipart.get("fix");

    final Props props = new Props();
    if (autoFix != null && autoFix.equals("off")) {
      props.put(ValidatorConfigs.CUSTOM_AUTO_FIX_FLAG_PARAM, "false");
    } else {
      props.put(ValidatorConfigs.CUSTOM_AUTO_FIX_FLAG_PARAM, "true");
    }
    ret.put("projectId", String.valueOf(project.getId()));

    final File tempDir = Utils.createTempDir();
    OutputStream out = null;
    try {
      logger.info("Uploading file to web server " + name);
      final File archiveFile = new File(tempDir, name);
      out = new BufferedOutputStream(new FileOutputStream(archiveFile));
      IOUtils.copy(item.getInputStream(), out);
      out.close();

      // get the locked flows for the project, so that they can be locked again after upload
      final List<Pair<String, String>> lockedFlows = getLockedFlows(project);
      // record the existing project flows before upload
      final List<Flow> existingFlows = project.getFlows();

      // validate project zip's dependencies and persist the new project metadata into DB
      final Map<String, ValidationReport> reports = this.projectManager
          .uploadProject(project, archiveFile, lowercaseExtension, user, props, uploaderIPAddr);

      // Post-processing after upload
      // reschedule data triggers if quartz is enabled
      if (this.enableQuartz) {
        this.scheduler.unschedule(existingFlows, projectName);
        this.scheduler.schedule(project, user.getUserId());
      }

      // reset locks for flows as needed
      lockFlowsForProject(project, lockedFlows);

      // remove schedule of renamed/deleted flows
      removeScheduleOfDeletedFlows(project, this.scheduleManager, (schedule) -> {
        logger.info("Removed schedule with id {} of renamed/deleted flow: {} from project: {}.",
                schedule.getScheduleId(), schedule.getFlowName(), schedule.getProjectName());
        this.projectManager.postProjectEvent(project, EventType.SCHEDULE, "azkaban",
                "Schedule " + schedule.toString() + " has been removed.");
      });

      // uploader is upload privilege user and the project is not protected by feature flag enable.project.adhoc.upload
      // mark the project with UPLOAD LOCK if it is not already, and persist in DB
      if (!project.isAdhocUploadEnabled() && user.getUserId().equals(uploadPrivilegeUser) && !project.isUploadLocked()) {
        project.setUploadLock(true);
        this.projectManager.updateProjectSetting(project);
        logger.info("Project {} is PROD", project.getName());
      } else if (uploadPrivilegeUser != null && !user.getUserId().equals(uploadPrivilegeUser)) {
        // when project security lock feature is turned on (only prod project flows would be allowed to push to prod cluster)
        // and uploader not the upload privileged user, we want to reset prod lock status to false
        // so that we remain the same restriction "prod project flows would be allowed to push to prod cluster"
        // regardless we enable/disable AdhocUploadWhenProjectUploadLocked completely
        project.setUploadLock(false);
        this.projectManager.updateProjectSetting(project);
        logger.info("Project {} is non PROD due to uploader {} is not uploadPrivilegeUser {}",
            project.getName(), user.getUserId(), uploadPrivilegeUser);
      }

      registerErrorsAndWarningsFromValidationReport(resp, ret, reports);
      // Reload the flow_filter to ensure if this project is added to the file, then the filter is current.
      if (this.executorManagerAdapter instanceof ContainerizedDispatchManager) {
        ContainerizedDispatchManager containerizedDispatchManager = (ContainerizedDispatchManager) this.executorManagerAdapter;
        containerizedDispatchManager.getContainerFlowCriteria().reloadFlowFilter();
      }
    } catch (final Exception e) {
      logger.error("Installation failed for project {}", projectName, e);
      String error = e.getMessage();
      if (error.length() > 512) {
        error = error.substring(0, 512) + "<br>Too many errors to display.<br>";
      }
      registerError(ret, "Installation Failed.<br>" + error, resp,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (final Throwable e) {
      logger.error("Severe Error: unable to upload for project {}", projectName, e);
      registerError(ret, "Server Encounter an unknown Error. <br>", resp,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      // rethrow the error as for now we don't know how to recover from it
      // usually nonExceptionError is severe one.
      // TODO: if it is outofmemory error, we should restart the server directly.
      throw e;
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

  /**
   * @return project. Null if invalid upload params or not enough permissions to proceed.
   */
  private Project validateUploadAndGetProject(final HttpServletResponse resp,
      final Map<String, String> ret, final User user, final String projectName) {
    if (projectName == null || projectName.isEmpty()) {
      registerError(ret, "No project name found.", resp, HttpServletResponse.SC_BAD_REQUEST);
      return null;
    }
    final Project project = this.projectManager.getProject(projectName);
    if (project == null || !project.isActive()) {
      final String failureCause = (project == null) ? "doesn't exist." : "was already removed.";
      registerError(ret, "Installation Failed. Project '" + projectName + " "
          + failureCause, resp, HttpServletResponse.SC_GONE);
      return null;
    }

    logger.info(
        "Upload: reference of project " + projectName + " is " + System.identityHashCode(project));

    if (this.lockdownUploadProjects && !UserUtils
        .hasPermissionforAction(this.userManager, user, Type.UPLOADPROJECTS)) {
      final String message =
          "Project uploading is locked out. Only admin users and users with special permissions can upload projects. "
              + "User " + user.getUserId() + " doesn't have permission to upload project.";
      logger.info(message);
      registerError(ret, message, resp, HttpServletResponse.SC_FORBIDDEN);
      return null;
    }
    if (!hasPermission(project, user, Type.WRITE)) {
      registerError(ret,
          "Installation Failed. User '" + user.getUserId() + "' does not have write access.",
          resp, HttpServletResponse.SC_BAD_REQUEST);
      return null;
    }
    return project;
  }

  /**
   * Remove schedule of renamed/deleted flows
   *
   * @param project           project from which old flows will be unscheduled
   * @param scheduleManager   the schedule manager
   * @param onDeletedSchedule a callback function to execute with every deleted schedule
   */
  static void removeScheduleOfDeletedFlows(final Project project,
      final ScheduleManager scheduleManager, final Consumer<Schedule> onDeletedSchedule) {
    final Set<String> flowNameList = project.getFlows().stream().map(f -> f.getId()).collect(
        Collectors.toSet());

    for (final Schedule schedule : scheduleManager.getAllSchedules()) {
      if (schedule.getProjectId() == project.getId() &&
          !flowNameList.contains(schedule.getFlowName())) {
        scheduleManager.removeSchedule(schedule);
        onDeletedSchedule.accept(schedule);
      }
    }
  }

  private void registerErrorsAndWarningsFromValidationReport(final HttpServletResponse resp,
      final Map<String, String> ret, final Map<String, ValidationReport> reports) {
    final StringBuffer errorMsgs = new StringBuffer();
    final StringBuffer warnMsgs = new StringBuffer();
    for (final Entry<String, ValidationReport> reportEntry : reports.entrySet()) {
      final ValidationReport report = reportEntry.getValue();
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
      if (!report.getErrorMsgs().isEmpty()) {
        errorMsgs.append("Validator " + reportEntry.getKey() + " reports errors:<br><br>");
        for (final String msg : report.getErrorMsgs()) {
          errorMsgs.append(msg + "<br>");
        }
      }
      if (!report.getWarningMsgs().isEmpty()) {
        warnMsgs.append("Validator " + reportEntry.getKey() + " reports warnings:<br><br>");
        for (final String msg : report.getWarningMsgs()) {
          warnMsgs.append(msg + "<br>");
        }
      }
    }
    if (errorMsgs.length() > 0) {
      // If putting more than 4000 characters in the cookie, the entire message will somehow
      // get discarded.
      registerError(ret,
          errorMsgs.length() > 4000 ? errorMsgs.substring(0, 4000) : errorMsgs.toString(), resp,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
    if (warnMsgs.length() > 0) {
      ret.put("warn", warnMsgs.length() > 4000 ? warnMsgs.substring(0, 4000) : warnMsgs.toString());
    }
  }

  /**
   * @return the list of locked flows and corresponding error messages for the specified project.
   */
  private List<Pair<String, String>> getLockedFlows(final Project project) {
    final List<Flow> flows = project.getFlows();
    return flows.stream()
        .filter(flow -> flow.isLocked())
        .map(flow -> new Pair<>(flow.getId(), flow.getFlowLockErrorMessage()))
        .collect(Collectors.toList());
  }

  /**
   * Lock the specified flows for the project.
   *
   * @param project     the project
   * @param lockedFlows list of IDs of flows to lock and corresponding lock error messages
   */
  private void lockFlowsForProject(final Project project,
      final List<Pair<String, String>> lockedFlows) {
    for (final Pair<String, String> idMsgPair : lockedFlows) {
      final Flow flow = project.getFlow(idMsgPair.getFirst());
      if (flow != null) {
        flow.setLocked(true);
        flow.setFlowLockErrorMessage(idMsgPair.getSecond());
      }
    }
  }

  private void handleUpload(final HttpServletRequest req, final HttpServletResponse resp,
      final Map<String, Object> multipart, final Session session) throws ServletException,
      IOException {
    final HashMap<String, String> ret = new HashMap<>();
    final String projectName = (String) multipart.get("project");
    ajaxHandleUpload(req, resp, ret, multipart, session);
    if (ret.containsKey(ERROR_PARAM)) {
      setErrorMessageInCookie(resp, ret.get(ERROR_PARAM));
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
          ret.put(ERROR_PARAM, "azkaban.properties doesn't contain property "
              + ProjectWhitelist.XML_FILE_PARAM);
        }
      } catch (final Exception e) {
        ret.put(ERROR_PARAM,
            "Exception occurred while trying to re-load project whitelist: "
                + e);
      }
    } else {
      ret.put(ERROR_PARAM, "Provided session doesn't have admin privilege.");
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
