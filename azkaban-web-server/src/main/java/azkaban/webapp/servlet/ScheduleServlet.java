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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.project.ProjectManager;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.server.session.Session;
import azkaban.server.HttpRequestUtils;
import azkaban.sla.SlaOption;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.JSONUtils;
import azkaban.utils.SplitterOutputStream;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.SchedulerStatistics;

public class ScheduleServlet extends LoginAbstractAzkabanServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger.getLogger(ScheduleServlet.class);
  private ProjectManager projectManager;
  private ScheduleManager scheduleManager;
  private UserManager userManager;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    userManager = server.getUserManager();
    projectManager = server.getProjectManager();
    scheduleManager = server.getScheduleManager();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else if (hasParam(req, "calendar")) {
      handleGetScheduleCalendar(req, resp, session);
    } else {
      handleGetAllSchedules(req, resp, session);
    }
  }

  private void handleAJAXAction(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    String ajaxName = getParam(req, "ajax");

    if (ajaxName.equals("slaInfo")) {
      ajaxSlaInfo(req, ret, session.getUser());
    } else if (ajaxName.equals("setSla")) {
      ajaxSetSla(req, ret, session.getUser());
    } else if (ajaxName.equals("loadFlow")) {
      ajaxLoadFlows(req, ret, session.getUser());
    } else if (ajaxName.equals("loadHistory")) {
      ajaxLoadHistory(req, resp, session.getUser());
      ret = null;
    } else if (ajaxName.equals("scheduleFlow")) {
      ajaxScheduleFlow(req, ret, session.getUser());
    } else if (ajaxName.equals("scheduleCronFlow")) {
      ajaxScheduleCronFlow(req, ret, session.getUser());
    } else if (ajaxName.equals("fetchSchedule")) {
      ajaxFetchSchedule(req, ret, session.getUser());
    }

    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void ajaxSetSla(HttpServletRequest req, HashMap<String, Object> ret,
      User user) {
    try {
      int scheduleId = getIntParam(req, "scheduleId");
      Schedule sched = scheduleManager.getSchedule(scheduleId);

      Project project = projectManager.getProject(sched.getProjectId());
      if (!hasPermission(project, user, Permission.Type.SCHEDULE)) {
        ret.put("error", "User " + user
            + " does not have permission to set SLA for this flow.");
        return;
      }

      String emailStr = getParam(req, "slaEmails");
      String[] emailSplit = emailStr.split("\\s*,\\s*|\\s*;\\s*|\\s+");
      List<String> slaEmails = Arrays.asList(emailSplit);

      Map<String, String> settings = getParamGroup(req, "settings");

      List<SlaOption> slaOptions = new ArrayList<SlaOption>();
      for (String set : settings.keySet()) {
        SlaOption sla;
        try {
          sla = parseSlaSetting(settings.get(set));
        } catch (Exception e) {
          throw new ServletException(e);
        }
        if (sla != null) {
          sla.getInfo().put(SlaOption.INFO_FLOW_NAME, sched.getFlowName());
          sla.getInfo().put(SlaOption.INFO_EMAIL_LIST, slaEmails);
          slaOptions.add(sla);
        }
      }

      sched.setSlaOptions(slaOptions);
      scheduleManager.insertSchedule(sched);

      if (slaOptions != null) {
        projectManager.postProjectEvent(project, EventType.SLA,
            user.getUserId(), "SLA for flow " + sched.getFlowName()
                + " has been added/changed.");
      }

    } catch (ServletException e) {
      ret.put("error", e.getMessage());
    } catch (ScheduleManagerException e) {
      ret.put("error", e.getMessage());
    }

  }

  private SlaOption parseSlaSetting(String set) throws ScheduleManagerException {
    logger.info("Tryint to set sla with the following set: " + set);

    String slaType;
    List<String> slaActions = new ArrayList<String>();
    Map<String, Object> slaInfo = new HashMap<String, Object>();
    String[] parts = set.split(",", -1);
    String id = parts[0];
    String rule = parts[1];
    String duration = parts[2];
    String emailAction = parts[3];
    String killAction = parts[4];
    if (emailAction.equals("true") || killAction.equals("true")) {
      if (emailAction.equals("true")) {
        slaActions.add(SlaOption.ACTION_ALERT);
        slaInfo.put(SlaOption.ALERT_TYPE, "email");
      }
      if (killAction.equals("true")) {
        slaActions.add(SlaOption.ACTION_CANCEL_FLOW);
      }
      if (id.equals("")) {
        if (rule.equals("SUCCESS")) {
          slaType = SlaOption.TYPE_FLOW_SUCCEED;
        } else {
          slaType = SlaOption.TYPE_FLOW_FINISH;
        }
      } else {
        slaInfo.put(SlaOption.INFO_JOB_NAME, id);
        if (rule.equals("SUCCESS")) {
          slaType = SlaOption.TYPE_JOB_SUCCEED;
        } else {
          slaType = SlaOption.TYPE_JOB_FINISH;
        }
      }

      ReadablePeriod dur;
      try {
        dur = parseDuration(duration);
      } catch (Exception e) {
        throw new ScheduleManagerException(
            "Unable to parse duration for a SLA that needs to take actions!", e);
      }

      slaInfo.put(SlaOption.INFO_DURATION, Utils.createPeriodString(dur));
      SlaOption r = new SlaOption(slaType, slaActions, slaInfo);
      logger.info("Parsing sla as id:" + id + " type:" + slaType + " rule:"
          + rule + " Duration:" + duration + " actions:" + slaActions);
      return r;
    }
    return null;
  }

  private ReadablePeriod parseDuration(String duration) {
    int hour = Integer.parseInt(duration.split(":")[0]);
    int min = Integer.parseInt(duration.split(":")[1]);
    return Minutes.minutes(min + hour * 60).toPeriod();
  }

  private void ajaxFetchSchedule(HttpServletRequest req,
      HashMap<String, Object> ret, User user) throws ServletException {

    int projectId = getIntParam(req, "projectId");
    String flowId = getParam(req, "flowId");
    try {
      Schedule schedule = scheduleManager.getSchedule(projectId, flowId);

      if (schedule != null) {
        Map<String, Object> jsonObj = new HashMap<String, Object>();
        jsonObj.put("scheduleId", Integer.toString(schedule.getScheduleId()));
        jsonObj.put("submitUser", schedule.getSubmitUser());
        jsonObj.put("firstSchedTime",
            utils.formatDateTime(schedule.getFirstSchedTime()));
        jsonObj.put("nextExecTime",
            utils.formatDateTime(schedule.getNextExecTime()));
        jsonObj.put("period", utils.formatPeriod(schedule.getPeriod()));
        jsonObj.put("cronExpression", schedule.getCronExpression());
        jsonObj.put("executionOptions", schedule.getExecutionOptions());
        ret.put("schedule", jsonObj);
      }
    } catch (ScheduleManagerException e) {
      ret.put("error", e);
    }
  }

  private void ajaxSlaInfo(HttpServletRequest req, HashMap<String, Object> ret,
      User user) {
    int scheduleId;
    try {
      scheduleId = getIntParam(req, "scheduleId");
      Schedule sched = scheduleManager.getSchedule(scheduleId);
      Project project =
          getProjectAjaxByPermission(ret, sched.getProjectId(), user, Type.READ);
      if (project == null) {
        ret.put("error",
            "Error loading project. Project " + sched.getProjectId()
                + " doesn't exist");
        return;
      }

      Flow flow = project.getFlow(sched.getFlowName());
      if (flow == null) {
        ret.put("error", "Error loading flow. Flow " + sched.getFlowName()
            + " doesn't exist in " + sched.getProjectId());
        return;
      }

      List<SlaOption> slaOptions = sched.getSlaOptions();
      ExecutionOptions flowOptions = sched.getExecutionOptions();

      if (slaOptions != null && slaOptions.size() > 0) {
        ret.put("slaEmails",
            slaOptions.get(0).getInfo().get(SlaOption.INFO_EMAIL_LIST));

        List<Object> setObj = new ArrayList<Object>();
        for (SlaOption sla : slaOptions) {
          setObj.add(sla.toWebObject());
        }
        ret.put("settings", setObj);
      } else if (flowOptions != null) {
        if (flowOptions.getFailureEmails() != null) {
          List<String> emails = flowOptions.getFailureEmails();
          if (emails.size() > 0) {
            ret.put("slaEmails", emails);
          }
        }
      } else {
        if (flow.getFailureEmails() != null) {
          List<String> emails = flow.getFailureEmails();
          if (emails.size() > 0) {
            ret.put("slaEmails", emails);
          }
        }
      }

      List<String> allJobs = new ArrayList<String>();
      for (Node n : flow.getNodes()) {
        allJobs.add(n.getId());
      }

      ret.put("allJobNames", allJobs);
    } catch (ServletException e) {
      ret.put("error", e);
    } catch (ScheduleManagerException e) {
      ret.put("error", e);
    }
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

  private void handleGetAllSchedules(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {

    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/scheduledflowpage.vm");

    List<Schedule> schedules;
    try {
      schedules = scheduleManager.getSchedules();
    } catch (ScheduleManagerException e) {
      throw new ServletException(e);
    }
    page.add("schedules", schedules);
    page.render();
  }

  private void handleGetScheduleCalendar(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {

    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/scheduledflowcalendarpage.vm");

    List<Schedule> schedules;
    try {
      schedules = scheduleManager.getSchedules();
    } catch (ScheduleManagerException e) {
      throw new ServletException(e);
    }
    page.add("schedules", schedules);
    page.render();
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      HashMap<String, Object> ret = new HashMap<String, Object>();
      if (hasParam(req, "action")) {
        String action = getParam(req, "action");
        if (action.equals("scheduleFlow")) {
          ajaxScheduleFlow(req, ret, session.getUser());
        } else if (action.equals("scheduleCronFlow")) {
          ajaxScheduleCronFlow(req, ret, session.getUser());
        } else if (action.equals("removeSched")) {
          ajaxRemoveSched(req, ret, session.getUser());
        }
      }

      if (ret.get("status") == ("success"))
        setSuccessMessageInCookie(resp, (String) ret.get("message"));
      else
        setErrorMessageInCookie(resp, (String) ret.get("message"));

      this.writeJSON(resp, ret);
    }
  }

  private void ajaxLoadFlows(HttpServletRequest req,
      HashMap<String, Object> ret, User user) throws ServletException {
    List<Schedule> schedules;
    try {
      schedules = scheduleManager.getSchedules();
    } catch (ScheduleManagerException e) {
      throw new ServletException(e);
    }
    // See if anything is scheduled
    if (schedules.size() <= 0)
      return;

    List<HashMap<String, Object>> output =
        new ArrayList<HashMap<String, Object>>();
    ret.put("items", output);

    for (Schedule schedule : schedules) {
      try {
        writeScheduleData(output, schedule);
      } catch (ScheduleManagerException e) {
        throw new ServletException(e);
      }
    }
  }

  private void writeScheduleData(List<HashMap<String, Object>> output,
      Schedule schedule) throws ScheduleManagerException {
    Map<String, Object> stats =
        SchedulerStatistics.getStatistics(schedule.getScheduleId(),
            (AzkabanWebServer) getApplication());
    HashMap<String, Object> data = new HashMap<String, Object>();
    data.put("scheduleid", schedule.getScheduleId());
    data.put("flowname", schedule.getFlowName());
    data.put("projectname", schedule.getProjectName());
    data.put("time", schedule.getFirstSchedTime());

    DateTime time = DateTime.now();
    long period = 0;
    if (schedule.getPeriod() != null) {
      period = time.plus(schedule.getPeriod()).getMillis() - time.getMillis();
    }
    data.put("period", period);
    int length = 3600 * 1000;
    if (stats.get("average") != null && stats.get("average") instanceof Integer) {
      length = (int) (Integer) stats.get("average");
      if (length == 0) {
        length = 3600 * 1000;
      }
    }
    data.put("length", length);
    data.put("history", false);
    data.put("stats", stats);
    output.add(data);
  }

  private void ajaxLoadHistory(HttpServletRequest req,
      HttpServletResponse resp, User user) throws ServletException, IOException {
    resp.setContentType(JSON_MIME_TYPE);
    long today = DateTime.now().withTime(0, 0, 0, 0).getMillis();
    long startTime = getLongParam(req, "startTime");
    DateTime start = new DateTime(startTime);
    // Ensure start time is 12:00 AM
    startTime = start.withTime(0, 0, 0, 0).getMillis();
    boolean useCache = false;
    if (startTime < today) {
      useCache = true;
    }
    long endTime = startTime + 24 * 3600 * 1000;
    int loadAll = getIntParam(req, "loadAll");

    // Cache file
    String cacheDir =
        getApplication().getServerProps().getString("cache.directory", "cache");
    File cacheDirFile = new File(cacheDir, "schedule-history");
    File cache = new File(cacheDirFile, startTime + ".cache");
    cache.getParentFile().mkdirs();

    if (useCache) {
      // Determine if cache exists
      boolean cacheExists = false;
      synchronized (this) {
        cacheExists = cache.exists() && cache.isFile();
      }
      if (cacheExists) {
        // Send the cache instead
        InputStream cacheInput =
            new BufferedInputStream(new FileInputStream(cache));
        try {
          IOUtils.copy(cacheInput, resp.getOutputStream());
          return;
        } finally {
          IOUtils.closeQuietly(cacheInput);
        }
      }
    }

    // Load data if not cached
    List<ExecutableFlow> history = null;
    try {
      AzkabanWebServer server = (AzkabanWebServer) getApplication();
      ExecutorManagerAdapter executorManager = server.getExecutorManager();
      history =
          executorManager.getExecutableFlows(null, null, null, 0, startTime,
              endTime, -1, -1);
    } catch (ExecutorManagerException e) {
      logger.error(e);
    }

    HashMap<String, Object> ret = new HashMap<String, Object>();
    List<HashMap<String, Object>> output =
        new ArrayList<HashMap<String, Object>>();
    ret.put("items", output);
    for (ExecutableFlow historyItem : history) {
      // Check if it is an scheduled execution
      if (historyItem.getScheduleId() >= 0 || loadAll != 0) {
        writeHistoryData(output, historyItem);
      }
    }

    // Make sure we're ready to cache it, otherwise output and return
    synchronized (this) {
      if (!useCache || cache.exists()) {
        JSONUtils.toJSON(ret, resp.getOutputStream(), false);
        return;
      }
    }

    // Create cache file
    File cacheTemp = new File(cacheDirFile, startTime + ".tmp");
    cacheTemp.createNewFile();
    OutputStream cacheOutput =
        new BufferedOutputStream(new FileOutputStream(cacheTemp));
    try {
      OutputStream outputStream =
          new SplitterOutputStream(cacheOutput, resp.getOutputStream());
      // Write to both the cache file and web output
      JSONUtils.toJSON(ret, outputStream, false);
    } finally {
      IOUtils.closeQuietly(cacheOutput);
    }
    // Move cache file
    synchronized (this) {
      cacheTemp.renameTo(cache);
    }
  }

  private void writeHistoryData(List<HashMap<String, Object>> output,
      ExecutableFlow history) {
    HashMap<String, Object> data = new HashMap<String, Object>();

    data.put("scheduleid", history.getScheduleId());
    Project project = projectManager.getProject(history.getProjectId());
    data.put("flowname", history.getFlowId());
    data.put("projectname", project.getName());
    data.put("time", history.getStartTime());
    data.put("period", "0");
    long endTime = history.getEndTime();
    if (endTime == -1) {
      endTime = System.currentTimeMillis();
    }
    data.put("length", endTime - history.getStartTime());
    data.put("history", true);
    data.put("status", history.getStatus().getNumVal());

    output.add(data);
  }

  private void ajaxRemoveSched(HttpServletRequest req, Map<String, Object> ret,
      User user) throws ServletException {
    int scheduleId = getIntParam(req, "scheduleId");
    Schedule sched;
    try {
      sched = scheduleManager.getSchedule(scheduleId);
    } catch (ScheduleManagerException e) {
      throw new ServletException(e);
    }
    if (sched == null) {
      ret.put("message", "Schedule with ID " + scheduleId + " does not exist");
      ret.put("status", "error");
      return;
    }

    Project project = projectManager.getProject(sched.getProjectId());

    if (project == null) {
      ret.put("message", "Project " + sched.getProjectId() + " does not exist");
      ret.put("status", "error");
      return;
    }

    if (!hasPermission(project, user, Type.SCHEDULE)) {
      ret.put("status", "error");
      ret.put("message", "Permission denied. Cannot remove schedule with id "
          + scheduleId);
      return;
    }

    scheduleManager.removeSchedule(sched);
    logger.info("User '" + user.getUserId() + " has removed schedule "
        + sched.getScheduleName());
    projectManager
        .postProjectEvent(project, EventType.SCHEDULE, user.getUserId(),
            "Schedule " + sched.toString() + " has been removed.");

    ret.put("status", "success");
    ret.put("message", "flow " + sched.getFlowName()
        + " removed from Schedules.");
    return;
  }

  private void ajaxScheduleFlow(HttpServletRequest req,
      HashMap<String, Object> ret, User user) throws ServletException {
    String projectName = getParam(req, "projectName");
    String flowName = getParam(req, "flow");
    int projectId = getIntParam(req, "projectId");

    Project project = projectManager.getProject(projectId);

    if (project == null) {
      ret.put("message", "Project " + projectName + " does not exist");
      ret.put("status", "error");
      return;
    }

    if (!hasPermission(project, user, Type.SCHEDULE)) {
      ret.put("status", "error");
      ret.put("message", "Permission denied. Cannot execute " + flowName);
      return;
    }

    Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put("status", "error");
      ret.put("message", "Flow " + flowName + " cannot be found in project "
          + project);
      return;
    }

    String scheduleTime = getParam(req, "scheduleTime");
    String scheduleDate = getParam(req, "scheduleDate");
    DateTime firstSchedTime;
    try {
      firstSchedTime = parseDateTime(scheduleDate, scheduleTime);
    } catch (Exception e) {
      ret.put("error", "Invalid date and/or time '" + scheduleDate + " "
          + scheduleTime);
      return;
    }

    ReadablePeriod thePeriod = null;
    try {
      if (hasParam(req, "is_recurring")
          && getParam(req, "is_recurring").equals("on")) {
        thePeriod = Schedule.parsePeriodString(getParam(req, "period"));
      }
    } catch (Exception e) {
      ret.put("error", e.getMessage());
    }

    ExecutionOptions flowOptions = null;
    try {
      flowOptions = HttpRequestUtils.parseFlowOptions(req);
      HttpRequestUtils.filterAdminOnlyFlowParams(userManager, flowOptions, user);
    } catch (Exception e) {
      ret.put("error", e.getMessage());
    }

    List<SlaOption> slaOptions = null;

    Schedule schedule =
        scheduleManager.scheduleFlow(-1, projectId, projectName, flowName,
            "ready", firstSchedTime.getMillis(), firstSchedTime.getZone(),
            thePeriod, DateTime.now().getMillis(), firstSchedTime.getMillis(),
            firstSchedTime.getMillis(), user.getUserId(), flowOptions,
            slaOptions);
    logger.info("User '" + user.getUserId() + "' has scheduled " + "["
        + projectName + flowName + " (" + projectId + ")" + "].");
    projectManager.postProjectEvent(project, EventType.SCHEDULE,
        user.getUserId(), "Schedule " + schedule.toString()
            + " has been added.");

    ret.put("status", "success");
    ret.put("scheduleId", schedule.getScheduleId());
    ret.put("message", projectName + "." + flowName + " scheduled.");
  }

  /**
   *
   * This method is in charge of doing cron scheduling.
   * @throws ServletException
   */
  private void ajaxScheduleCronFlow(HttpServletRequest req,
      HashMap<String, Object> ret, User user) throws ServletException {
    String projectName = getParam(req, "projectName");
    String flowName = getParam(req, "flow");

    Project project = projectManager.getProject(projectName);

    if (project == null) {
      ret.put("message", "Project " + projectName + " does not exist");
      ret.put("status", "error");
      return;
    }
    int projectId = project.getId();

    if (!hasPermission(project, user, Type.SCHEDULE)) {
      ret.put("status", "error");
      ret.put("message", "Permission denied. Cannot execute " + flowName);
      return;
    }

    Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put("status", "error");
      ret.put("message", "Flow " + flowName + " cannot be found in project "
          + project);
      return;
    }

    DateTimeZone timezone = DateTimeZone.getDefault();
    DateTime firstSchedTime = getPresentTimeByTimezone(timezone);

    String cronExpression = null;
    try {
      if (hasParam(req, "cronExpression")) {
        // everything in Azkaban functions is at the minute granularity, so we add 0 here
        // to let the expression to be complete.
        cronExpression = getParam(req, "cronExpression");
        if(azkaban.utils.Utils.isCronExpressionValid(cronExpression) == false) {
          ret.put("error", "This expression <" + cronExpression + "> can not be parsed to quartz cron.");
          return;
        }
      }
      if(cronExpression == null)
        throw new Exception("Cron expression must exist.");
    } catch (Exception e) {
      ret.put("error", e.getMessage());
    }

    ExecutionOptions flowOptions = null;
    try {
      flowOptions = HttpRequestUtils.parseFlowOptions(req);
      HttpRequestUtils.filterAdminOnlyFlowParams(userManager, flowOptions, user);
    } catch (Exception e) {
      ret.put("error", e.getMessage());
    }

    List<SlaOption> slaOptions = null;

    // Because either cronExpression or recurrence exists, we build schedule in the below way.
    Schedule schedule = scheduleManager.cronScheduleFlow(-1, projectId, projectName, flowName,
            "ready", firstSchedTime.getMillis(), firstSchedTime.getZone(),
            DateTime.now().getMillis(), firstSchedTime.getMillis(),
            firstSchedTime.getMillis(), user.getUserId(), flowOptions,
            slaOptions, cronExpression);

    logger.info("User '" + user.getUserId() + "' has scheduled " + "["
        + projectName + flowName + " (" + projectId + ")" + "].");
    projectManager.postProjectEvent(project, EventType.SCHEDULE,
        user.getUserId(), "Schedule " + schedule.toString()
            + " has been added.");

    ret.put("status", "success");
    ret.put("scheduleId", schedule.getScheduleId());
    ret.put("message", projectName + "." + flowName + " scheduled.");
  }

  private DateTime parseDateTime(String scheduleDate, String scheduleTime) {
    // scheduleTime: 12,00,pm,PDT
    String[] parts = scheduleTime.split(",", -1);
    int hour = Integer.parseInt(parts[0]);
    int minutes = Integer.parseInt(parts[1]);
    boolean isPm = parts[2].equalsIgnoreCase("pm");

    DateTimeZone timezone =
        parts[3].equals("UTC") ? DateTimeZone.UTC : DateTimeZone.getDefault();

    // scheduleDate: 02/10/2013
    DateTime day = null;
    if (scheduleDate == null || scheduleDate.trim().length() == 0) {
      day = new LocalDateTime().toDateTime();
    } else {
      day = DateTimeFormat.forPattern("MM/dd/yyyy")
          .withZone(timezone).parseDateTime(scheduleDate);
    }

    hour %= 12;

    if (isPm)
      hour += 12;

    DateTime firstSchedTime =
        day.withHourOfDay(hour).withMinuteOfHour(minutes).withSecondOfMinute(0);

    return firstSchedTime;
  }

  /**
   * @param cronTimezone represents the timezone from remote API call
   * @return if the string is equal to UTC, we return UTC; otherwise, we always return default timezone.
   */
  private DateTimeZone parseTimeZone(String cronTimezone) {
    if(cronTimezone != null && cronTimezone.equals("UTC"))
      return DateTimeZone.UTC;

    return DateTimeZone.getDefault();
  }

  private DateTime getPresentTimeByTimezone(DateTimeZone timezone) {
    return new DateTime(timezone);
  }
}
