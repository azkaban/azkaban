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

package azkaban.viewer.reportal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.tools.generic.EscapeTool;
import org.joda.time.DateTime;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.reportal.util.IStreamProvider;
import azkaban.reportal.util.Reportal;
import azkaban.reportal.util.Reportal.Query;
import azkaban.reportal.util.Reportal.Variable;
import azkaban.reportal.util.ReportalHelper;
import azkaban.reportal.util.ReportalUtil;
import azkaban.reportal.util.StreamProviderHDFS;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;

public class ReportalServlet extends LoginAbstractAzkabanServlet {
  private static final String REPORTAL_VARIABLE_PREFIX = "reportal.variable.";
  private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM =
      "hadoop.security.manager.class";
  private static final long serialVersionUID = 1L;
  private static Logger logger = Logger.getLogger(ReportalServlet.class);

  private CleanerThread cleanerThread;

  private File reportalMailTempDirectory;

  /**
   * A whitelist of allowed email domains (e.g.: example.com). If null, all
   * email domains are allowed.
   */
  private Set<String> allowedEmailDomains = null;

  private AzkabanWebServer server;
  private Props props;
  private boolean shouldProxy;

  private String viewerName;
  private String reportalStorageUser;
  private File webResourcesFolder;
  private int itemsPerPage = 20;
  private int max_allowed_schedule_dates;
  private int default_schedule_dates;
  private boolean showNav;

  private HadoopSecurityManager hadoopSecurityManager;

  public ReportalServlet(Props props) {
    this.props = props;

    viewerName = props.getString("viewer.name");
    reportalStorageUser = props.getString("reportal.storage.user", "reportal");
    itemsPerPage = props.getInt("reportal.items_per_page", 20);
    showNav = props.getBoolean("reportal.show.navigation", false);

    max_allowed_schedule_dates = props.getInt("reportal.max.allowed.schedule.dates", 180);
    default_schedule_dates = props.getInt("reportal.default.schedule.dates", 30);

    reportalMailTempDirectory =
        new File(props.getString("reportal.mail.temp.dir", "/tmp/reportal"));
    reportalMailTempDirectory.mkdirs();
    ReportalMailCreator.reportalMailTempDirectory = reportalMailTempDirectory;

    List<String> allowedDomains =
        props.getStringList("reportal.allowed.email.domains",
            (List<String>) null);
    if (allowedDomains != null) {
      allowedEmailDomains = new HashSet<String>(allowedDomains);
    }

    ReportalMailCreator.outputLocation =
        props.getString("reportal.output.dir", "/tmp/reportal");
    ReportalMailCreator.outputFileSystem =
        props.getString("reportal.output.filesystem", "local");
    ReportalMailCreator.reportalStorageUser = reportalStorageUser;

    webResourcesFolder =
        new File(new File(props.getSource()).getParentFile().getParentFile(),
            "web");
    webResourcesFolder.mkdirs();
    setResourceDirectory(webResourcesFolder);
    System.out.println("Reportal web resources: "
        + webResourcesFolder.getAbsolutePath());
  }

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    server = (AzkabanWebServer) getApplication();
    ReportalMailCreator.azkaban = server;

    shouldProxy = props.getBoolean("azkaban.should.proxy", false);
    logger.info("Hdfs browser should proxy: " + shouldProxy);
    try {
      hadoopSecurityManager = loadHadoopSecurityManager(props, logger);
      ReportalMailCreator.hadoopSecurityManager = hadoopSecurityManager;
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to get hadoop security manager!"
          + e.getCause());
    }

    cleanerThread = new CleanerThread();
    cleanerThread.start();
  }

  private HadoopSecurityManager loadHadoopSecurityManager(Props props,
      Logger logger) throws RuntimeException {

    Class<?> hadoopSecurityManagerClass =
        props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
            ReportalServlet.class.getClassLoader());
    logger.info("Initializing hadoop security manager "
        + hadoopSecurityManagerClass.getName());
    HadoopSecurityManager hadoopSecurityManager = null;

    try {
      Method getInstanceMethod =
          hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
      hadoopSecurityManager =
          (HadoopSecurityManager) getInstanceMethod.invoke(
              hadoopSecurityManagerClass, props);
    } catch (InvocationTargetException e) {
      logger.error("Could not instantiate Hadoop Security Manager "
          + hadoopSecurityManagerClass.getName() + e.getCause());
      throw new RuntimeException(e.getCause());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e.getCause());
    }

    return hadoopSecurityManager;
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      if (hasParam(req, "view")) {
        try {
          handleViewReportal(req, resp, session);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else if (hasParam(req, "new")) {
        handleNewReportal(req, resp, session);
      } else if (hasParam(req, "edit")) {
        handleEditReportal(req, resp, session);
      } else if (hasParam(req, "run")) {
        handleRunReportal(req, resp, session);
      } else {
        handleListReportal(req, resp, session);
      }
    }
  }

  private void handleAJAXAction(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    String ajaxName = getParam(req, "ajax");
    User user = session.getUser();
    int id = getIntParam(req, "id");
    ProjectManager projectManager = server.getProjectManager();
    Project project = projectManager.getProject(id);
    Reportal reportal = Reportal.loadFromProject(project);

    // Delete report
    if (ajaxName.equals("delete")) {
      if (!project.hasPermission(user, Type.ADMIN)) {
        ret.put("error", "You do not have permissions to delete this reportal.");
      } else {
        try {
          ScheduleManager scheduleManager = server.getScheduleManager();
          reportal.removeSchedules(scheduleManager);
          projectManager.removeProject(project, user);
        } catch (Exception e) {
          e.printStackTrace();
          ret.put("error", "An exception occured while deleting this reportal.");
        }
        ret.put("result", "success");
      }
    }
    // Bookmark report
    else if (ajaxName.equals("bookmark")) {
      boolean wasBookmarked = ReportalHelper.isBookmarkProject(project, user);
      try {
        if (wasBookmarked) {
          ReportalHelper.unBookmarkProject(server, project, user);
          ret.put("result", "success");
          ret.put("bookmark", false);
        } else {
          ReportalHelper.bookmarkProject(server, project, user);
          ret.put("result", "success");
          ret.put("bookmark", true);
        }
      } catch (ProjectManagerException e) {
        e.printStackTrace();
        ret.put("error", "Error bookmarking reportal. " + e.getMessage());
      }
    }
    // Subscribe to report
    else if (ajaxName.equals("subscribe")) {
      boolean wasSubscribed = ReportalHelper.isSubscribeProject(project, user);
      if (!wasSubscribed && reportal.getAccessViewers().size() > 0
          && !hasPermission(project, user, Type.READ)) {
        ret.put("error", "You do not have permissions to view this reportal.");
      } else {
        try {
          if (wasSubscribed) {
            ReportalHelper.unSubscribeProject(server, project, user);
            ret.put("result", "success");
            ret.put("subscribe", false);
          } else {
            ReportalHelper.subscribeProject(server, project, user,
                user.getEmail());
            ret.put("result", "success");
            ret.put("subscribe", true);
          }
        } catch (ProjectManagerException e) {
          e.printStackTrace();
          ret.put("error", "Error subscribing to reportal. " + e.getMessage());
        }
      }
    }
    // Get a portion of logs
    else if (ajaxName.equals("log")) {
      int execId = getIntParam(req, "execId");
      String jobId = getParam(req, "jobId");
      int offset = getIntParam(req, "offset");
      int length = getIntParam(req, "length");
      ExecutableFlow exec;
      ExecutorManagerAdapter executorManager = server.getExecutorManager();
      try {
        exec = executorManager.getExecutableFlow(execId);
      } catch (Exception e) {
        ret.put("error", "Log does not exist or isn't created yet.");
        return;
      }

      LogData data;
      try {
        data =
            executorManager.getExecutionJobLog(exec, jobId, offset, length,
                exec.getExecutableNode(jobId).getAttempt());
      } catch (Exception e) {
        e.printStackTrace();
        ret.put("error", "Log does not exist or isn't created yet.");
        return;
      }
      if (data != null) {
        ret.put("result", "success");
        ret.put("log", data.getData());
        ret.put("offset", data.getOffset());
        ret.put("length", data.getLength());
        ret.put("completed", exec.getEndTime() != -1);
      } else {
        // Return an empty result to indicate the end
        ret.put("result", "success");
        ret.put("log", "");
        ret.put("offset", offset);
        ret.put("length", 0);
        ret.put("completed", exec.getEndTime() != -1);
      }
    }

    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void handleListReportal(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {

    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportallistpage.vm");
    preparePage(page, session);

    List<Project> projects = ReportalHelper.getReportalProjects(server);
    page.add("ReportalHelper", ReportalHelper.class);
    page.add("user", session.getUser());

    String startDate = DateTime.now().minusWeeks(1).toString("yyyy-MM-dd");
    String endDate = DateTime.now().toString("yyyy-MM-dd");
    page.add("startDate", startDate);
    page.add("endDate", endDate);

    if (!projects.isEmpty()) {
      page.add("projects", projects);
    } else {
      page.add("projects", false);
    }

    page.render();
  }

  private void handleViewReportal(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      Exception {
    int id = getIntParam(req, "id");
    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportaldatapage.vm");
    preparePage(page, session);

    ProjectManager projectManager = server.getProjectManager();
    ExecutorManagerAdapter executorManager = server.getExecutorManager();

    Project project = projectManager.getProject(id);
    Reportal reportal = Reportal.loadFromProject(project);

    if (reportal == null) {
      page.add("errorMsg", "Report not found.");
      page.render();
      return;
    }

    if (reportal.getAccessViewers().size() > 0
        && !hasPermission(project, session.getUser(), Type.READ)) {
      page.add("errorMsg", "You are not allowed to view this report.");
      page.render();
      return;
    }

    page.add("project", project);
    page.add("title", project.getMetadata().get("title"));

    if (hasParam(req, "execid")) {
      int execId = getIntParam(req, "execid");
      page.add("execid", execId);
      // Show logs
      if (hasParam(req, "logs")) {
        ExecutableFlow exec;
        try {
          exec = executorManager.getExecutableFlow(execId);
        } catch (ExecutorManagerException e) {
          e.printStackTrace();
          page.add("errorMsg", "ExecutableFlow not found. " + e.getMessage());
          page.render();
          return;
        }
        // View single log
        if (hasParam(req, "log")) {
          page.add("view-log", true);
          String jobId = getParam(req, "log");
          page.add("execid", execId);
          page.add("jobId", jobId);
        }
        // List files
        else {
          page.add("view-logs", true);
          List<ExecutableNode> jobLogs = ReportalUtil.sortExecutableNodes(exec);

          boolean showDataCollector = hasParam(req, "debug");
          if (!showDataCollector) {
            jobLogs.remove(jobLogs.size() - 1);
          }

          if (jobLogs.size() == 1) {
            resp.sendRedirect("/reportal?view&logs&id=" + project.getId()
                + "&execid=" + execId + "&log=" + jobLogs.get(0).getId());
          }
          page.add("logs", jobLogs);
        }
      }
      // Show data files
      else {
        String outputFileSystem = ReportalMailCreator.outputFileSystem;
        String outputBase = ReportalMailCreator.outputLocation;

        String locationFull = (outputBase + "/" + execId).replace("//", "/");

        IStreamProvider streamProvider =
            ReportalUtil.getStreamProvider(outputFileSystem);

        if (streamProvider instanceof StreamProviderHDFS) {
          StreamProviderHDFS hdfsStreamProvider =
              (StreamProviderHDFS) streamProvider;
          hdfsStreamProvider.setHadoopSecurityManager(hadoopSecurityManager);
          hdfsStreamProvider.setUser(reportalStorageUser);
        }

        try {
          if (hasParam(req, "download")) {
            String fileName = getParam(req, "download");
            String filePath = locationFull + "/" + fileName;
            InputStream csvInputStream = null;
            OutputStream out = null;
            try {
              csvInputStream = streamProvider.getFileInputStream(filePath);
              resp.setContentType("application/octet-stream");

              out = resp.getOutputStream();
              IOUtils.copy(csvInputStream, out);
            } finally {
              IOUtils.closeQuietly(out);
              IOUtils.closeQuietly(csvInputStream);
            }
            return;
          }
          // Show file previews
          else {
            page.add("view-preview", true);

            try {
              String[] fileList = streamProvider.getFileList(locationFull);
              fileList = ReportalHelper.filterCSVFile(fileList);
              Arrays.sort(fileList);

              List<Object> files =
                  getFilePreviews(fileList, locationFull, streamProvider,
                      reportal.renderResultsAsHtml);

              page.add("files", files);
            } catch (Exception e) {
              logger.error("Error encountered while processing files in "
                  + locationFull, e);
            }
          }
        } finally {
          try {
            streamProvider.cleanUp();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
    // List executions and their data
    else {
      page.add("view-executions", true);
      ArrayList<ExecutableFlow> exFlows = new ArrayList<ExecutableFlow>();

      int pageNumber = 0;
      boolean hasNextPage = false;
      if (hasParam(req, "page")) {
        pageNumber = getIntParam(req, "page") - 1;
      }
      if (pageNumber < 0) {
        pageNumber = 0;
      }
      try {
        Flow flow = project.getFlows().get(0);
        executorManager.getExecutableFlows(project.getId(), flow.getId(),
            pageNumber * itemsPerPage, itemsPerPage, exFlows);
        ArrayList<ExecutableFlow> tmp = new ArrayList<ExecutableFlow>();
        executorManager.getExecutableFlows(project.getId(), flow.getId(),
            (pageNumber + 1) * itemsPerPage, 1, tmp);
        if (!tmp.isEmpty()) {
          hasNextPage = true;
        }
      } catch (ExecutorManagerException e) {
        page.add("error", "Error retrieving executable flows");
      }

      if (!exFlows.isEmpty()) {
        ArrayList<Object> history = new ArrayList<Object>();
        for (ExecutableFlow exFlow : exFlows) {
          HashMap<String, Object> flowInfo = new HashMap<String, Object>();
          flowInfo.put("execId", exFlow.getExecutionId());
          flowInfo.put("status", exFlow.getStatus().toString());
          flowInfo.put("startTime", exFlow.getStartTime());

          history.add(flowInfo);
        }
        page.add("executions", history);
      }
      if (pageNumber > 0) {
        page.add("pagePrev", pageNumber);
      }
      page.add("page", pageNumber + 1);
      if (hasNextPage) {
        page.add("pageNext", pageNumber + 2);
      }
    }

    page.render();
  }

  /**
   * Returns a list of file Objects that contain a "name" property with the file
   * name, a "content" property with the lines in the file, and a "hasMore"
   * property if the file contains more than NUM_PREVIEW_ROWS lines.
   *
   * @param fileList
   * @param locationFull
   * @param streamProvider
   * @return
   */
  private List<Object> getFilePreviews(String[] fileList, String locationFull,
      IStreamProvider streamProvider, boolean renderResultsAsHtml) {
    List<Object> files = new ArrayList<Object>();
    InputStream csvInputStream = null;

    try {
      for (String fileName : fileList) {
        Map<String, Object> file = new HashMap<String, Object>();
        file.put("name", fileName);

        String filePath = locationFull + "/" + fileName;
        csvInputStream = streamProvider.getFileInputStream(filePath);
        Scanner rowScanner = new Scanner(csvInputStream, StandardCharsets.UTF_8.toString());

        List<Object> lines = new ArrayList<Object>();
        int lineNumber = 0;
        while (rowScanner.hasNextLine()
            && lineNumber < ReportalMailCreator.NUM_PREVIEW_ROWS) {
          String csvLine = rowScanner.nextLine();
          String[] data = csvLine.split("\",\"");
          List<String> line = new ArrayList<String>();
          for (String item : data) {
            String column = item.replace("\"", "");
            if (!renderResultsAsHtml) {
              column = StringEscapeUtils.escapeHtml(column);
            }
            line.add(column);
          }
          lines.add(line);
          lineNumber++;
        }

        file.put("content", lines);

        if (rowScanner.hasNextLine()) {
          file.put("hasMore", true);
        }

        files.add(file);
        rowScanner.close();
      }
    } catch (Exception e) {
      logger.debug("Error encountered while processing files in "
          + locationFull, e);
    } finally {
      IOUtils.closeQuietly(csvInputStream);
    }

    return files;
  }

  private void handleRunReportal(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    int id = getIntParam(req, "id");
    ProjectManager projectManager = server.getProjectManager();
    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportalrunpage.vm");
    preparePage(page, session);

    Project project = projectManager.getProject(id);
    Reportal reportal = Reportal.loadFromProject(project);

    if (reportal == null) {
      page.add("errorMsg", "Report not found");
      page.render();
      return;
    }

    if (reportal.getAccessExecutors().size() > 0
        && !hasPermission(project, session.getUser(), Type.EXECUTE)) {
      page.add("errorMsg", "You are not allowed to run this report.");
      page.render();
      return;
    }

    page.add("projectId", id);
    page.add("title", reportal.title);
    page.add("description", reportal.description);

    List<Variable> runtimeVariables =
      ReportalUtil.getRunTimeVariables(reportal.variables);
    if (runtimeVariables.size() > 0) {
      page.add("variableNumber", runtimeVariables.size());
      page.add("variables", runtimeVariables);
    }

    page.render();
  }

  private void handleNewReportal(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {

    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportaleditpage.vm");
    preparePage(page, session);

    page.add("title", "");
    page.add("description", "");

    page.add("queryNumber", 1);

    List<Map<String, Object>> queryList = new ArrayList<Map<String, Object>>();
    page.add("queries", queryList);

    Map<String, Object> query = new HashMap<String, Object>();
    queryList.add(query);
    query.put("title", "");
    query.put("type", "");
    query.put("script", "");

    page.add("accessViewer", "");
    page.add("accessExecutor", "");
    page.add("accessOwner", "");
    page.add("notifications", "");
    page.add("failureNotifications", "");

    page.add("max_allowed_schedule_dates", max_allowed_schedule_dates);
    page.add("default_schedule_dates", default_schedule_dates);

    page.render();
  }

  private void handleEditReportal(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    int id = getIntParam(req, "id");
    ProjectManager projectManager = server.getProjectManager();

    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportaleditpage.vm");
    preparePage(page, session);
    page.add("ReportalHelper", ReportalHelper.class);

    Project project = projectManager.getProject(id);
    Reportal reportal = Reportal.loadFromProject(project);

    List<String> errors = new ArrayList<String>();

    if (reportal == null) {
      errors.add("Report not found");
      page.add("errorMsgs", errors);
      page.render();
      return;
    }

    if (!hasPermission(project, session.getUser(), Type.ADMIN)) {
      errors.add("You are not allowed to edit this report.");
      page.add("errorMsgs", errors);
      page.render();
      return;
    }

    page.add("projectId", id);
    page.add("title", reportal.title);
    page.add("description", reportal.description);
    page.add("queryNumber", reportal.queries.size());
    page.add("queries", reportal.queries);
    page.add("variableNumber", reportal.variables.size());
    page.add("variables", reportal.variables);
    page.add("schedule", reportal.schedule);
    page.add("scheduleHour", reportal.scheduleHour);
    page.add("scheduleMinute", reportal.scheduleMinute);
    page.add("scheduleAmPm", reportal.scheduleAmPm);
    page.add("scheduleTimeZone", reportal.scheduleTimeZone);
    page.add("scheduleDate", reportal.scheduleDate);
    page.add("endScheduleDate", reportal.endSchedule);
    page.add("scheduleRepeat", reportal.scheduleRepeat);
    page.add("scheduleIntervalQuantity", reportal.scheduleIntervalQuantity);
    page.add("scheduleInterval", reportal.scheduleInterval);
    page.add("renderResultsAsHtml", reportal.renderResultsAsHtml);
    page.add("notifications", reportal.notifications);
    page.add("failureNotifications", reportal.failureNotifications);
    page.add("accessViewer", reportal.accessViewer);
    page.add("accessExecutor", reportal.accessExecutor);
    page.add("accessOwner", reportal.accessOwner);

    page.add("max_allowed_schedule_dates", max_allowed_schedule_dates);
    page.add("default_schedule_dates", default_schedule_dates);
    page.render();
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      HashMap<String, Object> ret = new HashMap<String, Object>();

      handleRunReportalWithVariables(req, ret, session);

      if (ret != null) {
        this.writeJSON(resp, ret);
      }
    } else {
      handleSaveReportal(req, resp, session);
    }
  }

  private void handleSaveReportal(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    String projectId = validateAndSaveReport(req, resp, session);

    if (projectId != null) {
      this.setSuccessMessageInCookie(resp, "Report Saved.");

      String submitType = getParam(req, "submit");
      if (submitType.equals("Save")) {
        resp.sendRedirect(req.getRequestURI() + "?edit&id=" + projectId);
      } else {
        resp.sendRedirect(req.getRequestURI() + "?run&id=" + projectId);
      }
    }
  }

  /**
   * Validates and saves a report, returning the project id of the saved report
   * if successful, and null otherwise.
   *
   * @param req
   * @param resp
   * @param session
   * @return The project id of the saved report if successful, and null
   *         otherwise
   * @throws ServletException
   * @throws IOException
   */
  private String validateAndSaveReport(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {

    ProjectManager projectManager = server.getProjectManager();
    User user = session.getUser();

    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportaleditpage.vm");
    preparePage(page, session);
    page.add("ReportalHelper", ReportalHelper.class);

    boolean isEdit = hasParam(req, "id");
    if (isEdit) {
      page.add("projectId", getIntParam(req, "id"));
    }

    Project project = null;
    Reportal report = new Reportal();

    report.title = getParam(req, "title");
    report.description = getParam(req, "description");
    page.add("title", report.title);
    page.add("description", report.description);

    report.schedule = hasParam(req, "schedule");
    report.scheduleHour = getParam(req, "schedule-hour");
    report.scheduleMinute = getParam(req, "schedule-minute");
    report.scheduleAmPm = getParam(req, "schedule-am_pm");
    report.scheduleTimeZone = getParam(req, "schedule-timezone");
    report.scheduleDate = getParam(req, "schedule-date");
    report.scheduleRepeat = hasParam(req, "schedule-repeat");
    report.scheduleIntervalQuantity =
        getParam(req, "schedule-interval-quantity");
    report.scheduleInterval = getParam(req, "schedule-interval");
    report.renderResultsAsHtml = hasParam(req, "render-results-as-html");

    boolean isEndSchedule = hasParam(req, "end-schedule-date");
    if (isEndSchedule) {
      report.endSchedule = getParam(req, "end-schedule-date");
    }

    page.add("schedule", report.schedule);
    page.add("scheduleHour", report.scheduleHour);
    page.add("scheduleMinute", report.scheduleMinute);
    page.add("scheduleAmPm", report.scheduleAmPm);
    page.add("scheduleTimeZone", report.scheduleTimeZone);
    page.add("scheduleDate", report.scheduleDate);
    page.add("scheduleRepeat", report.scheduleRepeat);
    page.add("scheduleIntervalQuantity", report.scheduleIntervalQuantity);
    page.add("scheduleInterval", report.scheduleInterval);
    page.add("renderResultsAsHtml", report.renderResultsAsHtml);
    page.add("endSchedule", report.endSchedule);
    page.add("max_allowed_schedule_dates", max_allowed_schedule_dates);
    page.add("default_schedule_dates", default_schedule_dates);

    report.accessViewer = getParam(req, "access-viewer");
    report.accessExecutor = getParam(req, "access-executor");
    report.accessOwner = getParam(req, "access-owner");
    page.add("accessViewer", report.accessViewer);
    page.add("accessExecutor", report.accessExecutor);

    // Adding report creator as explicit owner, if not present already
    if (report.accessOwner == null || report.accessOwner.isEmpty()) {
      report.accessOwner = user.getUserId();
    } else {
      String[] splittedOwners = report.accessOwner.toLowerCase()
              .split(Reportal.ACCESS_LIST_SPLIT_REGEX);
      if (!Arrays.asList(splittedOwners).contains(user.getUserId())) {
        report.accessOwner = String.format("%s,%s", user.getUserId(),
                StringUtils.join(splittedOwners, ','));
      } else {
        report.accessOwner = StringUtils.join(splittedOwners, ',');
      }
    }

    page.add("accessOwner", report.accessOwner);

    report.notifications = getParam(req, "notifications");
    report.failureNotifications = getParam(req, "failure-notifications");
    page.add("notifications", report.notifications);
    page.add("failureNotifications", report.failureNotifications);

    int numQueries = getIntParam(req, "queryNumber");
    page.add("queryNumber", numQueries);
    List<Query> queryList = new ArrayList<Query>(numQueries);
    page.add("queries", queryList);
    report.queries = queryList;

    List<String> errors = new ArrayList<String>();
    for (int i = 0; i < numQueries; i++) {
      Query query = new Query();

      query.title = getParam(req, "query" + i + "title");
      query.type = getParam(req, "query" + i + "type");
      query.script = getParam(req, "query" + i + "script");

      // Type check
      ReportalType type = ReportalType.getTypeByName(query.type);
      if (type == null) {
        errors.add("Type " + query.type + " is invalid.");
      }

      if (!type.checkPermission(user) && report.schedule) {
        errors.add("You do not have permission to schedule Type " + query.type + ".");
      }

      queryList.add(query);
    }

    int variables = getIntParam(req, "variableNumber");
    page.add("variableNumber", variables);
    List<Variable> variableList = new ArrayList<Variable>(variables);
    page.add("variables", variableList);
    report.variables = variableList;

    String proxyUser = null;

    for (int i = 0; i < variables; i++) {
      Variable variable =
        new Variable(getParam(req, "variable" + i + "title"), getParam(req,
          "variable" + i + "name"));

      if (variable.title.isEmpty() || variable.name.isEmpty()) {
        errors.add("Variable title and name cannot be empty.");
      }

      if (variable.title.equals("reportal.config.reportal.execution.user")) {
        proxyUser = variable.name;
      }

      variableList.add(variable);
    }

    // Make sure title isn't empty
    if (report.title.isEmpty()) {
      errors.add("Title must not be empty.");
    }

    // Make sure description isn't empty
    if (report.description.isEmpty()) {
      errors.add("Description must not be empty.");
    }

    // Verify schedule and repeat
    if (report.schedule) {
      // Verify schedule time
      if (!NumberUtils.isDigits(report.scheduleHour)
          || !NumberUtils.isDigits(report.scheduleMinute)) {
        errors.add("Schedule time is invalid.");
      }

      // Verify schedule date is not empty
      if (report.scheduleDate.isEmpty()) {
        errors.add("Schedule date must not be empty.");
      }

      if (report.scheduleRepeat) {
        // Verify repeat interval
        if (!NumberUtils.isDigits(report.scheduleIntervalQuantity)) {
          errors.add("Repeat interval quantity is invalid.");
        }
      }
    }

    // Empty query check
    if (numQueries <= 0) {
      errors.add("There needs to have at least one query.");
    }

    // Validate access users
    UserManager userManager = getApplication().getUserManager();
    String[] accessLists =
        new String[] { report.accessViewer, report.accessExecutor,
            report.accessOwner };
    for (String accessList : accessLists) {
      if (accessList == null) {
        continue;
      }

      accessList = accessList.trim();
      if (!accessList.isEmpty()) {
        String[] users = accessList.split(Reportal.ACCESS_LIST_SPLIT_REGEX);
        for (String accessUser : users) {
          if (!userManager.validateUser(accessUser)) {
            errors.add("User " + accessUser + " in access list is invalid.");
          }
        }
      }
    }

    // Validate proxy user
    if (proxyUser != null) {
      if (!userManager.validateProxyUser(proxyUser,user)){
        errors.add("User " + user.getUserId() + " has no permission to add " + proxyUser + " as proxy user.");
      }
      proxyUser = null;
    }

    // Validate email addresses
    Set<String> emails =
        ReportalHelper.parseUniqueEmails(report.notifications + ","
            + report.failureNotifications, Reportal.ACCESS_LIST_SPLIT_REGEX);
    for (String email : emails) {
      if (!ReportalHelper.isValidEmailAddress(email)) {
        errors.add("Invalid email address: " + email);
        continue;
      }

      String domain = ReportalHelper.getEmailDomain(email);
      if (allowedEmailDomains != null && !allowedEmailDomains.contains(domain)) {
        errors.add("Email address '" + email + "' has an invalid domain '"
            + domain + "'. " + "Valid domains are: " + allowedEmailDomains);
      }
    }

    if (errors.size() > 0) {
      page.add("errorMsgs", errors);
      page.render();
      return null;
    }

    // Attempt to get a project object
    if (isEdit) {
      // Editing mode, load project
      int projectId = getIntParam(req, "id");
      project = projectManager.getProject(projectId);
      report.loadImmutableFromProject(project);
    } else {
      // Creation mode, create project
      try {
        project =
            ReportalHelper.createReportalProject(server, report.title,
                report.description, user);
        report.reportalUser = user.getUserId();
        report.ownerEmail = user.getEmail();
      } catch (Exception e) {
        e.printStackTrace();
        errors.add("Error while creating report. " + e.getMessage());
        page.add("errorMsgs", errors);
        page.render();
        return null;
      }

      // Project already exists
      if (project == null) {
        errors.add("A Report with the same name already exists.");
        page.add("errorMsgs", errors);
        page.render();
        return null;
      }
    }

    if (project == null) {
      errors.add("Internal Error: Report not found");
      page.add("errorMsgs", errors);
      page.render();
      return null;
    }

    report.project = project;
    page.add("projectId", project.getId());

    try {
      report.createZipAndUpload(projectManager, user, reportalStorageUser);
    } catch (Exception e) {
      e.printStackTrace();
      errors.add("Error while creating Azkaban jobs. " + e.getMessage());
      page.add("errorMsgs", errors);
      page.render();
      if (!isEdit) {
        try {
          projectManager.removeProject(project, user);
        } catch (ProjectManagerException e1) {
          e1.printStackTrace();
        }
      }
      return null;
    }

    // Prepare flow
    Flow flow = project.getFlows().get(0);
    project.getMetadata().put("flowName", flow.getId());

    // Set Reportal mailer
    flow.setMailCreator(ReportalMailCreator.REPORTAL_MAIL_CREATOR);

    // Create/Save schedule
    ScheduleManager scheduleManager = server.getScheduleManager();
    try {
      report.updateSchedules(report, scheduleManager, user, flow);
    } catch (ScheduleManagerException e) {
      e.printStackTrace();
      errors.add(e.getMessage());
      page.add("errorMsgs", errors);
      page.render();
      return null;
    }

    report.saveToProject(project);

    try {
      ReportalHelper.updateProjectNotifications(project, projectManager);
      projectManager.updateProjectSetting(project);
      projectManager
          .updateProjectDescription(project, report.description, user);
      updateProjectPermissions(project, projectManager, report, user);
      projectManager.updateFlow(project, flow);
    } catch (ProjectManagerException e) {
      e.printStackTrace();
      errors.add("Error while updating report. " + e.getMessage());
      page.add("errorMsgs", errors);
      page.render();
      if (!isEdit) {
        try {
          projectManager.removeProject(project, user);
        } catch (ProjectManagerException e1) {
          e1.printStackTrace();
        }
      }
      return null;
    }

    return Integer.toString(project.getId());
  }

  private void updateProjectPermissions(Project project,
      ProjectManager projectManager, Reportal report, User currentUser)
      throws ProjectManagerException {
    // Old permissions and users
    List<Pair<String, Permission>> oldPermissions =
        project.getUserPermissions();
    Set<String> oldUsers = new HashSet<String>();
    for (Pair<String, Permission> userPermission : oldPermissions) {
      oldUsers.add(userPermission.getFirst());
    }

    // Update permissions
    report.updatePermissions();

    // New permissions and users
    List<Pair<String, Permission>> newPermissions =
        project.getUserPermissions();
    Set<String> newUsers = new HashSet<String>();
    for (Pair<String, Permission> userPermission : newPermissions) {
      newUsers.add(userPermission.getFirst());
    }

    // Save all new permissions
    for (Pair<String, Permission> userPermission : newPermissions) {
      if (!oldPermissions.contains(userPermission)) {
        projectManager.updateProjectPermission(project,
            userPermission.getFirst(), userPermission.getSecond(), false,
            currentUser);
      }
    }

    // Remove permissions for any old users no longer in the new users
    for (String oldUser : oldUsers) {
      if (!newUsers.contains(oldUser)) {
        projectManager.removeProjectPermission(project, oldUser, false,
            currentUser);
      }
    }
  }

  private void handleRunReportalWithVariables(HttpServletRequest req,
      HashMap<String, Object> ret, Session session) throws ServletException,
      IOException {
    boolean isTestRun = hasParam(req, "testRun");

    int id = getIntParam(req, "id");
    ProjectManager projectManager = server.getProjectManager();
    Project project = projectManager.getProject(id);
    Reportal report = Reportal.loadFromProject(project);
    User user = session.getUser();

    if (report.getAccessExecutors().size() > 0
        && !hasPermission(project, user, Type.EXECUTE)) {
      ret.put("error", "You are not allowed to run this report.");
      return;
    }

    for (Query query : report.queries) {
      String jobType = query.type;
      ReportalType type = ReportalType.getTypeByName(jobType);
      if (!type.checkPermission(user)) {
        ret.put(
            "error",
            "You are not allowed to run this report as you don't have permission to run job type "
                + type.toString() + ".");
        return;
      }
    }

    Flow flow = project.getFlows().get(0);

    ExecutableFlow exflow = new ExecutableFlow(project, flow);
    exflow.setSubmitUser(user.getUserId());
    exflow.addAllProxyUsers(project.getProxyUsers());

    ExecutionOptions options = exflow.getExecutionOptions();

    int i = 0;
    for (Variable variable : ReportalUtil.getRunTimeVariables(report.variables)) {
      options.getFlowParameters().put(REPORTAL_VARIABLE_PREFIX + i + ".from",
          variable.name);
      options.getFlowParameters().put(REPORTAL_VARIABLE_PREFIX + i + ".to",
          getParam(req, "variable" + i));
      i++;
    }

    options.getFlowParameters()
        .put("reportal.execution.user", user.getUserId());

    // Add the execution user's email to the list of success and failure emails.
    String email = user.getEmail();

    if (email != null && !email.isEmpty()) {
      if (isTestRun) { // Only email the executor
        List<String> emails = new ArrayList<String>();
        emails.add(email);
        options.setSuccessEmails(emails);
        options.setFailureEmails(emails);
      } else {
        options.getSuccessEmails().add(email);
        options.getFailureEmails().add(email);
      }
    }

    options.getFlowParameters().put("reportal.title", report.title);
    options.getFlowParameters().put("reportal.render.results.as.html",
        report.renderResultsAsHtml ? "true" : "false");
    options.getFlowParameters().put("reportal.unscheduled.run", "true");

    try {
      String message =
          server.getExecutorManager().submitExecutableFlow(exflow,
              session.getUser().getUserId())
              + ".";
      ret.put("message", message);
      ret.put("result", "success");
      ret.put("redirect", "/reportal?view&logs&id=" + project.getId()
          + "&execid=" + exflow.getExecutionId());
    } catch (ExecutorManagerException e) {
      e.printStackTrace();
      ret.put("error",
          "Error running report " + report.title + ". " + e.getMessage());
    }
  }

  private void preparePage(Page page, Session session) {
    page.add("viewerName", viewerName);
    page.add("hideNavigation", !showNav);
    page.add("userid", session.getUser().getUserId());
    page.add("esc", new EscapeTool());
  }

  private class CleanerThread extends Thread {
    // The frequency, in milliseconds, that the Reportal output
    // and mail temp directories should be cleaned
    private final long CLEAN_INTERVAL_MS;
    private static final long DEFAULT_CLEAN_INTERVAL_MS = 24 * 60 * 60 * 1000;

    // The duration, in milliseconds, that Reportal output should be retained
    // for
    private final long OUTPUT_DIR_RETENTION_MS;
    private static final long DEFAULT_OUTPUT_DIR_RETENTION_MS = 7 * 24 * 60
        * 60 * 1000;

    // The duration, in milliseconds, that Reportal mail temp files should be
    // retained for
    private final long MAIL_TEMP_DIR_RETENTION_MS;
    private static final long DEFAULT_MAIL_TEMP_DIR_RETENTION_MS =
        24 * 60 * 60 * 1000;

    private boolean shutdown = false;

    public CleanerThread() {
      this.setName("Reportal-Cleaner-Thread");
      CLEAN_INTERVAL_MS =
          props
              .getLong("reportal.clean.interval.ms", DEFAULT_CLEAN_INTERVAL_MS);
      OUTPUT_DIR_RETENTION_MS =
          props.getLong("reportal.output.dir.retention.ms",
              DEFAULT_OUTPUT_DIR_RETENTION_MS);
      MAIL_TEMP_DIR_RETENTION_MS =
          props.getLong("reportal.mail.temp.dir.retention.ms",
              DEFAULT_MAIL_TEMP_DIR_RETENTION_MS);
    }

    @SuppressWarnings("unused")
    public void shutdown() {
      shutdown = true;
      this.interrupt();
    }

    @Override
    public void run() {
      while (!shutdown) {
        synchronized (this) {
          logger.info("Cleaning old execution output dirs");
          cleanOldReportalOutputDirs();

          logger.info("Cleaning Reportal mail temp directory");
          cleanReportalMailTempDir();
        }

        try {
          Thread.sleep(CLEAN_INTERVAL_MS);
        } catch (InterruptedException e) {
          logger.error("CleanerThread's sleep was interrupted.", e);
        }
      }
    }

    private void cleanOldReportalOutputDirs() {
      IStreamProvider streamProvider =
          ReportalUtil.getStreamProvider(ReportalMailCreator.outputFileSystem);

      if (streamProvider instanceof StreamProviderHDFS) {
        StreamProviderHDFS hdfsStreamProvider =
            (StreamProviderHDFS) streamProvider;
        hdfsStreamProvider.setHadoopSecurityManager(hadoopSecurityManager);
        hdfsStreamProvider.setUser(reportalStorageUser);
      }

      final long pastTimeThreshold =
          System.currentTimeMillis() - OUTPUT_DIR_RETENTION_MS;

      String[] oldFiles = null;
      try {
        oldFiles =
            streamProvider.getOldFiles(ReportalMailCreator.outputLocation,
                pastTimeThreshold);
      } catch (Exception e) {
        logger.error("Error getting old files from "
            + ReportalMailCreator.outputLocation + " on "
            + ReportalMailCreator.outputFileSystem + " file system.", e);
      }

      if (oldFiles != null) {
        for (String file : oldFiles) {
          String filePath = ReportalMailCreator.outputLocation + "/" + file;
          try {
            streamProvider.deleteFile(filePath);
          } catch (Exception e) {
            logger.error("Error deleting file " + filePath + " from "
                + ReportalMailCreator.outputFileSystem + " file system.", e);
          }
        }
      }
    }

    private void cleanReportalMailTempDir() {
      File dir = reportalMailTempDirectory;
      final long pastTimeThreshold =
          System.currentTimeMillis() - MAIL_TEMP_DIR_RETENTION_MS;

      File[] oldMailTempDirs = dir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File path) {
          if (path.isDirectory() && path.lastModified() < pastTimeThreshold) {
            return true;
          }
          return false;
        }
      });

      for (File tempDir : oldMailTempDirs) {
        try {
          FileUtils.deleteDirectory(tempDir);
        } catch (IOException e) {
          logger.error(
              "Error cleaning Reportal mail temp dir " + tempDir.getPath(), e);
        }
      }
    }
  }
}
