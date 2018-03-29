/*
 * Copyright 2018 LinkedIn Corp.
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

public class ReportalServlet extends LoginAbstractAzkabanServlet {

  private static final String REPORTAL_VARIABLE_PREFIX = "reportal.variable.";
  private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM =
      "hadoop.security.manager.class";
  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger.getLogger(ReportalServlet.class);
  private final File reportalMailTempDirectory;
  private final Props props;
  private final String viewerName;
  private final String reportalStorageUser;
  private final File webResourcesFolder;
  private final int max_allowed_schedule_dates;
  private final int default_schedule_dates;
  private final boolean showNav;
  private CleanerThread cleanerThread;
  /**
   * A whitelist of allowed email domains (e.g.: example.com). If null, all
   * email domains are allowed.
   */
  private Set<String> allowedEmailDomains = null;
  private AzkabanWebServer server;
  private boolean shouldProxy;
  private int itemsPerPage = 20;
  private HadoopSecurityManager hadoopSecurityManager;

  public ReportalServlet(final Props props) {
    this.props = props;

    this.viewerName = props.getString("viewer.name");
    this.reportalStorageUser = props.getString("reportal.storage.user", "reportal");
    this.itemsPerPage = props.getInt("reportal.items_per_page", 20);
    this.showNav = props.getBoolean("reportal.show.navigation", false);

    this.max_allowed_schedule_dates = props.getInt("reportal.max.allowed.schedule.dates", 180);
    this.default_schedule_dates = props.getInt("reportal.default.schedule.dates", 30);

    this.reportalMailTempDirectory =
        new File(props.getString("reportal.mail.temp.dir", "/tmp/reportal"));
    this.reportalMailTempDirectory.mkdirs();
    ReportalMailCreator.reportalMailTempDirectory = this.reportalMailTempDirectory;

    final List<String> allowedDomains =
        props.getStringList("reportal.allowed.email.domains",
            (List<String>) null);
    if (allowedDomains != null) {
      this.allowedEmailDomains = new HashSet<>(allowedDomains);
    }

    ReportalMailCreator.outputLocation =
        props.getString("reportal.output.dir", "/tmp/reportal");
    ReportalMailCreator.outputFileSystem =
        props.getString("reportal.output.filesystem", "local");
    ReportalMailCreator.reportalStorageUser = this.reportalStorageUser;

    this.webResourcesFolder =
        new File(new File(props.getSource()).getParentFile().getParentFile(),
            "web");
    this.webResourcesFolder.mkdirs();
    setResourceDirectory(this.webResourcesFolder);
    System.out.println("Reportal web resources: "
        + this.webResourcesFolder.getAbsolutePath());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    this.server = (AzkabanWebServer) getApplication();
    ReportalMailCreator.azkaban = this.server;

    this.shouldProxy = this.props.getBoolean("azkaban.should.proxy", false);
    logger.info("Hdfs browser should proxy: " + this.shouldProxy);
    try {
      this.hadoopSecurityManager = loadHadoopSecurityManager(this.props, logger);
      ReportalMailCreator.hadoopSecurityManager = this.hadoopSecurityManager;
    } catch (final RuntimeException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to get hadoop security manager!"
          + e.getCause());
    }

    this.cleanerThread = new CleanerThread();
    this.cleanerThread.start();
  }

  private HadoopSecurityManager loadHadoopSecurityManager(final Props props,
      final Logger logger) throws RuntimeException {

    final Class<?> hadoopSecurityManagerClass =
        props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
            ReportalServlet.class.getClassLoader());
    logger.info("Initializing hadoop security manager "
        + hadoopSecurityManagerClass.getName());
    HadoopSecurityManager hadoopSecurityManager = null;

    try {
      final Method getInstanceMethod =
          hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
      hadoopSecurityManager =
          (HadoopSecurityManager) getInstanceMethod.invoke(
              hadoopSecurityManagerClass, props);
    } catch (final InvocationTargetException e) {
      logger.error("Could not instantiate Hadoop Security Manager "
          + hadoopSecurityManagerClass.getName() + e.getCause());
      throw new RuntimeException(e.getCause());
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e.getCause());
    }

    return hadoopSecurityManager;
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      if (hasParam(req, "view")) {
        try {
          handleViewReportal(req, resp, session);
        } catch (final Exception e) {
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

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");
    final User user = session.getUser();
    final int id = getIntParam(req, "id");
    final ProjectManager projectManager = this.server.getProjectManager();
    final Project project = projectManager.getProject(id);
    final Reportal reportal = Reportal.loadFromProject(project);

    // Delete report
    if (ajaxName.equals("delete")) {
      if (!project.hasPermission(user, Type.ADMIN)) {
        ret.put("error", "You do not have permissions to delete this reportal.");
      } else {
        try {
          final ScheduleManager scheduleManager = this.server.getScheduleManager();
          reportal.removeSchedules(scheduleManager);
          projectManager.removeProject(project, user);
        } catch (final Exception e) {
          e.printStackTrace();
          ret.put("error", "An exception occured while deleting this reportal.");
        }
        ret.put("result", "success");
      }
    }
    // Bookmark report
    else if (ajaxName.equals("bookmark")) {
      final boolean wasBookmarked = ReportalHelper.isBookmarkProject(project, user);
      try {
        if (wasBookmarked) {
          ReportalHelper.unBookmarkProject(this.server, project, user);
          ret.put("result", "success");
          ret.put("bookmark", false);
        } else {
          ReportalHelper.bookmarkProject(this.server, project, user);
          ret.put("result", "success");
          ret.put("bookmark", true);
        }
      } catch (final ProjectManagerException e) {
        e.printStackTrace();
        ret.put("error", "Error bookmarking reportal. " + e.getMessage());
      }
    }
    // Subscribe to report
    else if (ajaxName.equals("subscribe")) {
      final boolean wasSubscribed = ReportalHelper.isSubscribeProject(project, user);
      if (!wasSubscribed && reportal.getAccessViewers().size() > 0
          && !hasPermission(project, user, Type.READ)) {
        ret.put("error", "You do not have permissions to view this reportal.");
      } else {
        try {
          if (wasSubscribed) {
            ReportalHelper.unSubscribeProject(this.server, project, user);
            ret.put("result", "success");
            ret.put("subscribe", false);
          } else {
            ReportalHelper.subscribeProject(this.server, project, user,
                user.getEmail());
            ret.put("result", "success");
            ret.put("subscribe", true);
          }
        } catch (final ProjectManagerException e) {
          e.printStackTrace();
          ret.put("error", "Error subscribing to reportal. " + e.getMessage());
        }
      }
    }
    // Get a portion of logs
    else if (ajaxName.equals("log")) {
      final int execId = getIntParam(req, "execId");
      final String jobId = getParam(req, "jobId");
      final int offset = getIntParam(req, "offset");
      final int length = getIntParam(req, "length");
      final ExecutableFlow exec;
      final ExecutorManagerAdapter executorManager = this.server.getExecutorManager();
      try {
        exec = executorManager.getExecutableFlow(execId);
      } catch (final Exception e) {
        ret.put("error", "Log does not exist or isn't created yet.");
        return;
      }

      final LogData data;
      try {
        data =
            executorManager.getExecutionJobLog(exec, jobId, offset, length,
                exec.getExecutableNode(jobId).getAttempt());
      } catch (final Exception e) {
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

  private void handleListReportal(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {

    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportallistpage.vm");
    preparePage(page, session);

    final List<Project> projects = ReportalHelper.getReportalProjects(this.server);
    page.add("ReportalHelper", ReportalHelper.class);
    page.add("user", session.getUser());

    final String startDate = DateTime.now().minusWeeks(1).toString("yyyy-MM-dd");
    final String endDate = DateTime.now().toString("yyyy-MM-dd");
    page.add("startDate", startDate);
    page.add("endDate", endDate);

    if (!projects.isEmpty()) {
      page.add("projects", projects);
    } else {
      page.add("projects", false);
    }

    page.render();
  }

  private void handleViewReportal(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      Exception {
    final int id = getIntParam(req, "id");
    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportaldatapage.vm");
    preparePage(page, session);

    final ProjectManager projectManager = this.server.getProjectManager();
    final ExecutorManagerAdapter executorManager = this.server.getExecutorManager();

    final Project project = projectManager.getProject(id);
    final Reportal reportal = Reportal.loadFromProject(project);

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
      final int execId = getIntParam(req, "execid");
      page.add("execid", execId);
      // Show logs
      if (hasParam(req, "logs")) {
        final ExecutableFlow exec;
        try {
          exec = executorManager.getExecutableFlow(execId);
        } catch (final ExecutorManagerException e) {
          e.printStackTrace();
          page.add("errorMsg", "ExecutableFlow not found. " + e.getMessage());
          page.render();
          return;
        }
        // View single log
        if (hasParam(req, "log")) {
          page.add("view-log", true);
          final String jobId = getParam(req, "log");
          page.add("execid", execId);
          page.add("jobId", jobId);
        }
        // List files
        else {
          page.add("view-logs", true);
          final List<ExecutableNode> jobLogs = ReportalUtil.sortExecutableNodes(exec);

          final boolean showDataCollector = hasParam(req, "debug");
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
        final String outputFileSystem = ReportalMailCreator.outputFileSystem;
        final String outputBase = ReportalMailCreator.outputLocation;

        final String locationFull = (outputBase + "/" + execId).replace("//", "/");

        final IStreamProvider streamProvider =
            ReportalUtil.getStreamProvider(outputFileSystem);

        if (streamProvider instanceof StreamProviderHDFS) {
          final StreamProviderHDFS hdfsStreamProvider =
              (StreamProviderHDFS) streamProvider;
          hdfsStreamProvider.setHadoopSecurityManager(this.hadoopSecurityManager);
          hdfsStreamProvider.setUser(this.reportalStorageUser);
        }

        try {
          if (hasParam(req, "download")) {
            final String fileName = getParam(req, "download");
            final String filePath = locationFull + "/" + fileName;
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

              final List<Object> files =
                  getFilePreviews(fileList, locationFull, streamProvider,
                      reportal.renderResultsAsHtml);

              page.add("files", files);
            } catch (final Exception e) {
              logger.error("Error encountered while processing files in "
                  + locationFull, e);
            }
          }
        } finally {
          try {
            streamProvider.cleanUp();
          } catch (final IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
    // List executions and their data
    else {
      page.add("view-executions", true);
      final ArrayList<ExecutableFlow> exFlows = new ArrayList<>();

      int pageNumber = 0;
      boolean hasNextPage = false;
      if (hasParam(req, "page")) {
        pageNumber = getIntParam(req, "page") - 1;
      }
      if (pageNumber < 0) {
        pageNumber = 0;
      }
      try {
        final Flow flow = project.getFlows().get(0);
        executorManager.getExecutableFlows(project.getId(), flow.getId(),
            pageNumber * this.itemsPerPage, this.itemsPerPage, exFlows);
        final ArrayList<ExecutableFlow> tmp = new ArrayList<>();
        executorManager.getExecutableFlows(project.getId(), flow.getId(),
            (pageNumber + 1) * this.itemsPerPage, 1, tmp);
        if (!tmp.isEmpty()) {
          hasNextPage = true;
        }
      } catch (final ExecutorManagerException e) {
        page.add("error", "Error retrieving executable flows");
      }

      if (!exFlows.isEmpty()) {
        final ArrayList<Object> history = new ArrayList<>();
        for (final ExecutableFlow exFlow : exFlows) {
          final HashMap<String, Object> flowInfo = new HashMap<>();
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
   */
  private List<Object> getFilePreviews(final String[] fileList, final String locationFull,
      final IStreamProvider streamProvider, final boolean renderResultsAsHtml) {
    final List<Object> files = new ArrayList<>();
    InputStream csvInputStream = null;

    try {
      for (final String fileName : fileList) {
        final Map<String, Object> file = new HashMap<>();
        file.put("name", fileName);

        final String filePath = locationFull + "/" + fileName;
        csvInputStream = streamProvider.getFileInputStream(filePath);
        final Scanner rowScanner = new Scanner(csvInputStream, StandardCharsets.UTF_8.toString());

        final List<Object> lines = new ArrayList<>();
        int lineNumber = 0;
        while (rowScanner.hasNextLine()
            && lineNumber < ReportalMailCreator.NUM_PREVIEW_ROWS) {
          final String csvLine = rowScanner.nextLine();
          final String[] data = csvLine.split("\",\"");
          final List<String> line = new ArrayList<>();
          for (final String item : data) {
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
    } catch (final Exception e) {
      logger.debug("Error encountered while processing files in "
          + locationFull, e);
    } finally {
      IOUtils.closeQuietly(csvInputStream);
    }

    return files;
  }

  private void handleRunReportal(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final int id = getIntParam(req, "id");
    final ProjectManager projectManager = this.server.getProjectManager();
    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportalrunpage.vm");
    preparePage(page, session);

    final Project project = projectManager.getProject(id);
    final Reportal reportal = Reportal.loadFromProject(project);

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

    final List<Variable> runtimeVariables =
        ReportalUtil.getRunTimeVariables(reportal.variables);
    if (runtimeVariables.size() > 0) {
      page.add("variableNumber", runtimeVariables.size());
      page.add("variables", runtimeVariables);
    }

    page.render();
  }

  private void handleNewReportal(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {

    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportaleditpage.vm");
    preparePage(page, session);

    page.add("title", "");
    page.add("description", "");

    page.add("queryNumber", 1);

    final List<Map<String, Object>> queryList = new ArrayList<>();
    page.add("queries", queryList);

    final Map<String, Object> query = new HashMap<>();
    queryList.add(query);
    query.put("title", "");
    query.put("type", "");
    query.put("script", "");

    page.add("accessViewer", "");
    page.add("accessExecutor", "");
    page.add("accessOwner", "");
    page.add("notifications", "");
    page.add("failureNotifications", "");

    page.add("max_allowed_schedule_dates", this.max_allowed_schedule_dates);
    page.add("default_schedule_dates", this.default_schedule_dates);

    page.render();
  }

  private void handleEditReportal(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final int id = getIntParam(req, "id");
    final ProjectManager projectManager = this.server.getProjectManager();

    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportaleditpage.vm");
    preparePage(page, session);
    page.add("ReportalHelper", ReportalHelper.class);

    final Project project = projectManager.getProject(id);
    final Reportal reportal = Reportal.loadFromProject(project);

    final List<String> errors = new ArrayList<>();

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

    page.add("max_allowed_schedule_dates", this.max_allowed_schedule_dates);
    page.add("default_schedule_dates", this.default_schedule_dates);
    page.render();
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      final HashMap<String, Object> ret = new HashMap<>();

      handleRunReportalWithVariables(req, ret, session);

      if (ret != null) {
        this.writeJSON(resp, ret);
      }
    } else {
      handleSaveReportal(req, resp, session);
    }
  }

  private void handleSaveReportal(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final String projectId = validateAndSaveReport(req, resp, session);

    if (projectId != null) {
      this.setSuccessMessageInCookie(resp, "Report Saved.");

      final String submitType = getParam(req, "submit");
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
   * @return The project id of the saved report if successful, and null
   * otherwise
   */
  private String validateAndSaveReport(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {

    final ProjectManager projectManager = this.server.getProjectManager();
    final User user = session.getUser();

    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/reportal/reportaleditpage.vm");
    preparePage(page, session);
    page.add("ReportalHelper", ReportalHelper.class);

    final boolean isEdit = hasParam(req, "id");
    if (isEdit) {
      page.add("projectId", getIntParam(req, "id"));
    }

    Project project = null;
    final Reportal report = new Reportal();

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

    final boolean isEndSchedule = hasParam(req, "end-schedule-date");
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
    page.add("max_allowed_schedule_dates", this.max_allowed_schedule_dates);
    page.add("default_schedule_dates", this.default_schedule_dates);

    report.accessViewer = getParam(req, "access-viewer");
    report.accessExecutor = getParam(req, "access-executor");
    report.accessOwner = getParam(req, "access-owner");
    page.add("accessViewer", report.accessViewer);
    page.add("accessExecutor", report.accessExecutor);

    // Adding report creator as explicit owner, if not present already
    if (report.accessOwner == null || report.accessOwner.isEmpty()) {
      report.accessOwner = user.getUserId();
    } else {
      final String[] splittedOwners = report.accessOwner.toLowerCase()
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

    final int numQueries = getIntParam(req, "queryNumber");
    page.add("queryNumber", numQueries);
    final List<Query> queryList = new ArrayList<>(numQueries);
    page.add("queries", queryList);
    report.queries = queryList;

    final List<String> errors = new ArrayList<>();
    for (int i = 0; i < numQueries; i++) {
      final Query query = new Query();

      query.title = getParam(req, "query" + i + "title");
      query.type = getParam(req, "query" + i + "type");
      query.script = getParam(req, "query" + i + "script");

      // Type check
      final ReportalType type = ReportalType.getTypeByName(query.type);
      if (type == null) {
        errors.add("Type " + query.type + " is invalid.");
      }

      if (!type.checkPermission(user) && report.schedule) {
        errors.add("You do not have permission to schedule Type " + query.type + ".");
      }

      queryList.add(query);
    }

    final int variables = getIntParam(req, "variableNumber");
    page.add("variableNumber", variables);
    final List<Variable> variableList = new ArrayList<>(variables);
    page.add("variables", variableList);
    report.variables = variableList;

    String proxyUser = null;

    for (int i = 0; i < variables; i++) {
      final Variable variable =
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
    final UserManager userManager = getApplication().getUserManager();
    final String[] accessLists =
        new String[]{report.accessViewer, report.accessExecutor,
            report.accessOwner};
    for (String accessList : accessLists) {
      if (accessList == null) {
        continue;
      }

      accessList = accessList.trim();
      if (!accessList.isEmpty()) {
        final String[] users = accessList.split(Reportal.ACCESS_LIST_SPLIT_REGEX);
        for (final String accessUser : users) {
          if (!userManager.validateUser(accessUser)) {
            errors.add("User " + accessUser + " in access list is invalid.");
          }
        }
      }
    }

    // Validate proxy user
    if (proxyUser != null) {
      if (!userManager.validateProxyUser(proxyUser, user)) {
        errors.add("User " + user.getUserId() + " has no permission to add " + proxyUser
            + " as proxy user.");
      }
      proxyUser = null;
    }

    // Validate email addresses
    final Set<String> emails =
        ReportalHelper.parseUniqueEmails(report.notifications + ","
            + report.failureNotifications, Reportal.ACCESS_LIST_SPLIT_REGEX);
    for (final String email : emails) {
      if (!ReportalHelper.isValidEmailAddress(email)) {
        errors.add("Invalid email address: " + email);
        continue;
      }

      final String domain = ReportalHelper.getEmailDomain(email);
      if (this.allowedEmailDomains != null && !this.allowedEmailDomains.contains(domain)) {
        errors.add("Email address '" + email + "' has an invalid domain '"
            + domain + "'. " + "Valid domains are: " + this.allowedEmailDomains);
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
      final int projectId = getIntParam(req, "id");
      project = projectManager.getProject(projectId);
      report.loadImmutableFromProject(project);
    } else {
      // Creation mode, create project
      try {
        project =
            ReportalHelper.createReportalProject(this.server, report.title,
                report.description, user);
        report.reportalUser = user.getUserId();
        report.ownerEmail = user.getEmail();
      } catch (final Exception e) {
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
      report.createZipAndUpload(projectManager, user, this.reportalStorageUser);
    } catch (final Exception e) {
      e.printStackTrace();
      errors.add("Error while creating Azkaban jobs. " + e.getMessage());
      page.add("errorMsgs", errors);
      page.render();
      if (!isEdit) {
        try {
          projectManager.removeProject(project, user);
        } catch (final ProjectManagerException e1) {
          e1.printStackTrace();
        }
      }
      return null;
    }

    // Prepare flow
    final Flow flow = project.getFlows().get(0);
    project.getMetadata().put("flowName", flow.getId());

    // Set Reportal mailer
    flow.setMailCreator(ReportalMailCreator.REPORTAL_MAIL_CREATOR);

    // Create/Save schedule
    final ScheduleManager scheduleManager = this.server.getScheduleManager();
    try {
      report.updateSchedules(report, scheduleManager, user, flow);
    } catch (final ScheduleManagerException e) {
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
    } catch (final ProjectManagerException e) {
      e.printStackTrace();
      errors.add("Error while updating report. " + e.getMessage());
      page.add("errorMsgs", errors);
      page.render();
      if (!isEdit) {
        try {
          projectManager.removeProject(project, user);
        } catch (final ProjectManagerException e1) {
          e1.printStackTrace();
        }
      }
      return null;
    }

    return Integer.toString(project.getId());
  }

  private void updateProjectPermissions(final Project project,
      final ProjectManager projectManager, final Reportal report, final User currentUser)
      throws ProjectManagerException {
    // Old permissions and users
    final List<Pair<String, Permission>> oldPermissions =
        project.getUserPermissions();
    final Set<String> oldUsers = new HashSet<>();
    for (final Pair<String, Permission> userPermission : oldPermissions) {
      oldUsers.add(userPermission.getFirst());
    }

    // Update permissions
    report.updatePermissions();

    // New permissions and users
    final List<Pair<String, Permission>> newPermissions =
        project.getUserPermissions();
    final Set<String> newUsers = new HashSet<>();
    for (final Pair<String, Permission> userPermission : newPermissions) {
      newUsers.add(userPermission.getFirst());
    }

    // Save all new permissions
    for (final Pair<String, Permission> userPermission : newPermissions) {
      if (!oldPermissions.contains(userPermission)) {
        projectManager.updateProjectPermission(project,
            userPermission.getFirst(), userPermission.getSecond(), false,
            currentUser);
      }
    }

    // Remove permissions for any old users no longer in the new users
    for (final String oldUser : oldUsers) {
      if (!newUsers.contains(oldUser)) {
        projectManager.removeProjectPermission(project, oldUser, false,
            currentUser);
      }
    }
  }

  private void handleRunReportalWithVariables(final HttpServletRequest req,
      final HashMap<String, Object> ret, final Session session) throws ServletException,
      IOException {
    final boolean isTestRun = hasParam(req, "testRun");

    final int id = getIntParam(req, "id");
    final ProjectManager projectManager = this.server.getProjectManager();
    final Project project = projectManager.getProject(id);
    final Reportal report = Reportal.loadFromProject(project);
    final User user = session.getUser();

    if (report.getAccessExecutors().size() > 0
        && !hasPermission(project, user, Type.EXECUTE)) {
      ret.put("error", "You are not allowed to run this report.");
      return;
    }

    for (final Query query : report.queries) {
      final String jobType = query.type;
      final ReportalType type = ReportalType.getTypeByName(jobType);
      if (!type.checkPermission(user)) {
        ret.put(
            "error",
            "You are not allowed to run this report as you don't have permission to run job type "
                + type.toString() + ".");
        return;
      }
    }

    final Flow flow = project.getFlows().get(0);

    final ExecutableFlow exflow = new ExecutableFlow(project, flow);
    exflow.setSubmitUser(user.getUserId());
    exflow.addAllProxyUsers(project.getProxyUsers());

    final ExecutionOptions options = exflow.getExecutionOptions();

    int i = 0;
    for (final Variable variable : ReportalUtil.getRunTimeVariables(report.variables)) {
      options.getFlowParameters().put(REPORTAL_VARIABLE_PREFIX + i + ".from",
          variable.name);
      options.getFlowParameters().put(REPORTAL_VARIABLE_PREFIX + i + ".to",
          getParam(req, "variable" + i));
      i++;
    }

    options.getFlowParameters()
        .put("reportal.execution.user", user.getUserId());

    // Add the execution user's email to the list of success and failure emails.
    final String email = user.getEmail();

    if (email != null && !email.isEmpty()) {
      if (isTestRun) { // Only email the executor
        final List<String> emails = new ArrayList<>();
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
      final String message =
          this.server.getExecutorManager().submitExecutableFlow(exflow,
              session.getUser().getUserId())
              + ".";
      ret.put("message", message);
      ret.put("result", "success");
      ret.put("redirect", "/reportal?view&logs&id=" + project.getId()
          + "&execid=" + exflow.getExecutionId());
    } catch (final ExecutorManagerException e) {
      e.printStackTrace();
      ret.put("error",
          "Error running report " + report.title + ". " + e.getMessage());
    }
  }

  private void preparePage(final Page page, final Session session) {
    page.add("viewerName", this.viewerName);
    page.add("hideNavigation", !this.showNav);
    page.add("userid", session.getUser().getUserId());
    page.add("esc", new EscapeTool());
  }

  private class CleanerThread extends Thread {

    private static final long DEFAULT_CLEAN_INTERVAL_MS = 24 * 60 * 60 * 1000;
    private static final long DEFAULT_OUTPUT_DIR_RETENTION_MS = 7 * 24 * 60
        * 60 * 1000;
    private static final long DEFAULT_MAIL_TEMP_DIR_RETENTION_MS =
        24 * 60 * 60 * 1000;
    // The frequency, in milliseconds, that the Reportal output
    // and mail temp directories should be cleaned
    private final long CLEAN_INTERVAL_MS;
    // The duration, in milliseconds, that Reportal output should be retained
    // for
    private final long OUTPUT_DIR_RETENTION_MS;
    // The duration, in milliseconds, that Reportal mail temp files should be
    // retained for
    private final long MAIL_TEMP_DIR_RETENTION_MS;
    private boolean shutdown = false;

    public CleanerThread() {
      this.setName("Reportal-Cleaner-Thread");
      this.CLEAN_INTERVAL_MS =
          ReportalServlet.this.props
              .getLong("reportal.clean.interval.ms", DEFAULT_CLEAN_INTERVAL_MS);
      this.OUTPUT_DIR_RETENTION_MS =
          ReportalServlet.this.props.getLong("reportal.output.dir.retention.ms",
              DEFAULT_OUTPUT_DIR_RETENTION_MS);
      this.MAIL_TEMP_DIR_RETENTION_MS =
          ReportalServlet.this.props.getLong("reportal.mail.temp.dir.retention.ms",
              DEFAULT_MAIL_TEMP_DIR_RETENTION_MS);
    }

    @SuppressWarnings("unused")
    public void shutdown() {
      this.shutdown = true;
      this.interrupt();
    }

    @Override
    public void run() {
      while (!this.shutdown) {
        synchronized (this) {
          logger.info("Cleaning old execution output dirs");
          cleanOldReportalOutputDirs();

          logger.info("Cleaning Reportal mail temp directory");
          cleanReportalMailTempDir();
        }

        try {
          Thread.sleep(this.CLEAN_INTERVAL_MS);
        } catch (final InterruptedException e) {
          logger.error("CleanerThread's sleep was interrupted.", e);
        }
      }
    }

    private void cleanOldReportalOutputDirs() {
      final IStreamProvider streamProvider =
          ReportalUtil.getStreamProvider(ReportalMailCreator.outputFileSystem);

      if (streamProvider instanceof StreamProviderHDFS) {
        final StreamProviderHDFS hdfsStreamProvider =
            (StreamProviderHDFS) streamProvider;
        hdfsStreamProvider.setHadoopSecurityManager(ReportalServlet.this.hadoopSecurityManager);
        hdfsStreamProvider.setUser(ReportalServlet.this.reportalStorageUser);
      }

      final long pastTimeThreshold =
          System.currentTimeMillis() - this.OUTPUT_DIR_RETENTION_MS;

      String[] oldFiles = null;
      try {
        oldFiles =
            streamProvider.getOldFiles(ReportalMailCreator.outputLocation,
                pastTimeThreshold);
      } catch (final Exception e) {
        logger.error("Error getting old files from "
            + ReportalMailCreator.outputLocation + " on "
            + ReportalMailCreator.outputFileSystem + " file system.", e);
      }

      if (oldFiles != null) {
        for (final String file : oldFiles) {
          final String filePath = ReportalMailCreator.outputLocation + "/" + file;
          try {
            streamProvider.deleteFile(filePath);
          } catch (final Exception e) {
            logger.error("Error deleting file " + filePath + " from "
                + ReportalMailCreator.outputFileSystem + " file system.", e);
          }
        }
      }
    }

    private void cleanReportalMailTempDir() {
      final File dir = ReportalServlet.this.reportalMailTempDirectory;
      final long pastTimeThreshold =
          System.currentTimeMillis() - this.MAIL_TEMP_DIR_RETENTION_MS;

      final File[] oldMailTempDirs = dir.listFiles(new FileFilter() {
        @Override
        public boolean accept(final File path) {
          if (path.isDirectory() && path.lastModified() < pastTimeThreshold) {
            return true;
          }
          return false;
        }
      });

      for (final File tempDir : oldMailTempDirs) {
        try {
          FileUtils.deleteDirectory(tempDir);
        } catch (final IOException e) {
          logger.error(
              "Error cleaning Reportal mail temp dir " + tempDir.getPath(), e);
        }
      }
    }
  }
}
