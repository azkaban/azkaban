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
import azkaban.executor.ExecutionOptions;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.project.ProjectManager;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.server.AzkabanAPI;
import azkaban.server.HttpRequestUtils;
import azkaban.server.SlaRequestUtils;
import azkaban.server.session.Session;
import azkaban.sla.SlaOption;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.TimeUtils;
import azkaban.webapp.AzkabanWebServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;


public class ScheduleServlet extends LoginAbstractAzkabanServlet {

  private static final String API_SLA_INFO = "slaInfo";
  private static final String API_SET_SLA = "setSla";
  private static final String API_FETCH_SCHEDULES = "fetchSchedules";
  private static final String API_FETCH_SCHEDULE = "fetchSchedule";
  private static final String API_LOAD_FLOW = "loadFlow";
  private static final String API_SCHEDULE_FLOW = "scheduleFlow";
  private static final String API_SCHEDULE_CRON_FLOW = "scheduleCronFlow";
  private static final String API_REMOVE_SCHED = "removeSched";

  public static final String PARAM_SLA_EMAILS = "slaEmails";
  public static final String PARAM_SCHEDULE_ID = "scheduleId";
  public static final String PARAM_SETTINGS = "settings";
  public static final String PARAM_ERROR = "error";
  public static final String PARAM_ALL_JOB_NAMES = "allJobNames";
  public static final String PARAM_STATUS = "status";
  public static final String PARAM_MESSAGE = "message";

  public static final String STATUS_SUCCESS = "success";
  public static final String STATUS_ERROR = "error";

  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger.getLogger(ScheduleServlet.class);
  private ProjectManager projectManager;
  private ScheduleManager scheduleManager;
  private UserManager userManager;

  public ScheduleServlet() {
    super(createAPIEndpoints());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = getApplication();
    this.userManager = server.getUserManager();
    this.projectManager = server.getProjectManager();
    this.scheduleManager = server.getScheduleManager();
  }

  private static List<AzkabanAPI> createAPIEndpoints() {
    final List<AzkabanAPI> apiEndpoints = new ArrayList<>();
    apiEndpoints.add(new AzkabanAPI("ajax", API_SLA_INFO));
    apiEndpoints.add(new AzkabanAPI("ajax", API_SET_SLA));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_SCHEDULES));
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_SCHEDULE));
    apiEndpoints.add(new AzkabanAPI("ajax", API_LOAD_FLOW));
    apiEndpoints.add(new AzkabanAPI("ajax", API_SCHEDULE_FLOW));
    apiEndpoints.add(new AzkabanAPI("ajax", API_SCHEDULE_CRON_FLOW));

    apiEndpoints.add(new AzkabanAPI("action", API_SCHEDULE_FLOW));
    apiEndpoints.add(new AzkabanAPI("action", API_SCHEDULE_CRON_FLOW));
    apiEndpoints.add(new AzkabanAPI("action", API_REMOVE_SCHED));
    return apiEndpoints;
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      handleGetAllSchedules(req, resp, session);
    }
  }

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException, IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");

    if (API_SLA_INFO.equals(ajaxName)) {
      ajaxSlaInfo(req, ret, session.getUser());
    } else if (API_SET_SLA.equals(ajaxName)) {
      ajaxSetSla(req, ret, session.getUser());
      // alias loadFlow is preserved for backward compatibility
    } else if (API_FETCH_SCHEDULES.equals(ajaxName) || API_LOAD_FLOW.equals(ajaxName)) {
      ajaxFetchSchedules(ret);
    } else if (API_SCHEDULE_FLOW.equals(ajaxName)) {
      ajaxScheduleFlow(req, ret, session.getUser());
    } else if (API_SCHEDULE_CRON_FLOW.equals(ajaxName)) {
      ajaxScheduleCronFlow(req, ret, session.getUser());
    } else if (API_FETCH_SCHEDULE.equals(ajaxName)) {
      ajaxFetchSchedule(req, ret, session.getUser());
    }

    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void ajaxFetchSchedules(final HashMap<String, Object> ret) throws ServletException {
    final List<Schedule> schedules;
    try {
      schedules = this.scheduleManager.getSchedules();
    } catch (final ScheduleManagerException e) {
      throw new ServletException(e);
    }
    // See if anything is scheduled
    if (schedules.size() <= 0) {
      return;
    }

    final List<HashMap<String, Object>> output =
        new ArrayList<>();
    ret.put("items", output);

    for (final Schedule schedule : schedules) {
      try {
        writeScheduleData(output, schedule);
      } catch (final ScheduleManagerException e) {
        throw new ServletException(e);
      }
    }
  }

  private void writeScheduleData(final List<HashMap<String, Object>> output,
      final Schedule schedule) throws ScheduleManagerException {

    final HashMap<String, Object> data = new HashMap<>();
    data.put(PARAM_SCHEDULE_ID, schedule.getScheduleId());
    data.put("flowname", schedule.getFlowName());
    data.put("projectname", schedule.getProjectName());
    data.put("time", schedule.getFirstSchedTime());
    data.put("cron", schedule.getCronExpression());

    final DateTime time = DateTime.now();
    long period = 0;
    if (schedule.getPeriod() != null) {
      period = time.plus(schedule.getPeriod()).getMillis() - time.getMillis();
    }
    data.put("period", period);
    data.put("history", false);
    output.add(data);
  }

  private void ajaxSetSla(final HttpServletRequest req, final HashMap<String, Object> ret,
      final User user) {
    try {
      final int scheduleId = getIntParam(req, PARAM_SCHEDULE_ID);
      final Schedule sched = this.scheduleManager.getSchedule(scheduleId);
      if (sched == null) {
        ret.put(PARAM_ERROR,
            "Error loading schedule. Schedule " + scheduleId
                + " doesn't exist");
        return;
      }

      final Project project = this.projectManager.getProject(sched.getProjectId());
      if (!hasPermission(project, user, Permission.Type.SCHEDULE)) {
        ret.put(PARAM_ERROR, "User " + user
            + " does not have permission to set SLA for this flow.");
        return;
      }

      final String emailStr = getParam(req, PARAM_SLA_EMAILS);
      final Map<String, String> settings = getParamGroup(req, PARAM_SETTINGS);
      final List<SlaOption> slaOptions = SlaRequestUtils.parseSlaOptions(sched.getFlowName(),
          emailStr, settings);

      if (slaOptions.isEmpty()) {
        throw new ScheduleManagerException(
            String.format("SLA for schedule %s must have at least one action", scheduleId));
      }

      sched.getExecutionOptions().setSlaOptions(slaOptions);
      this.scheduleManager.insertSchedule(sched);
      this.projectManager.postProjectEvent(project, EventType.SLA,
          user.getUserId(), "SLA for flow " + sched.getFlowName()
              + " has been added/changed.");

    } catch (final ServletException e) {
      ret.put(PARAM_ERROR, e.getMessage());
    } catch (final ScheduleManagerException e) {
      logger.error(e.getMessage(), e);
      ret.put(PARAM_ERROR, e.getMessage());
    }

  }
  
  private void ajaxFetchSchedule(final HttpServletRequest req,
      final HashMap<String, Object> ret, final User user) throws ServletException {

    final int projectId = getIntParam(req, "projectId");
    final String flowId = getParam(req, "flowId");
    try {
      final Schedule schedule = this.scheduleManager.getSchedule(projectId, flowId);

      if (schedule != null) {
        final Map<String, Object> jsonObj = new HashMap<>();
        jsonObj.put(PARAM_SCHEDULE_ID, Integer.toString(schedule.getScheduleId()));
        jsonObj.put("submitUser", schedule.getSubmitUser());
        jsonObj.put("firstSchedTime",
            TimeUtils.formatDateTime(schedule.getFirstSchedTime()));
        jsonObj.put("nextExecTime",
            TimeUtils.formatDateTime(schedule.getNextExecTime()));
        jsonObj.put("period", TimeUtils.formatPeriod(schedule.getPeriod()));
        jsonObj.put("cronExpression", schedule.getCronExpression());
        jsonObj.put("executionOptions", schedule.getExecutionOptions());
        ret.put("schedule", jsonObj);
      }
    } catch (final ScheduleManagerException e) {
      logger.error(e.getMessage(), e);
      ret.put(PARAM_ERROR, e);
    }
  }

  private void ajaxSlaInfo(final HttpServletRequest req, final HashMap<String, Object> ret,
      final User user) {
    final int scheduleId;
    try {
      scheduleId = getIntParam(req, PARAM_SCHEDULE_ID);
      final Schedule sched = this.scheduleManager.getSchedule(scheduleId);
      if (sched == null) {
        ret.put(PARAM_ERROR,
            "Error loading schedule. Schedule " + scheduleId
                + " doesn't exist");
        return;
      }

      final Project project =
          getProjectAjaxByPermission(ret, sched.getProjectId(), user, Type.READ);
      if (project == null) {
        ret.put(PARAM_ERROR,
            "Error loading project. Project " + sched.getProjectId()
                + " doesn't exist");
        return;
      }

      final Flow flow = project.getFlow(sched.getFlowName());
      if (flow == null) {
        ret.put(PARAM_ERROR, "Error loading flow. Flow " + sched.getFlowName()
            + " doesn't exist in " + sched.getProjectId());
        return;
      }

      final List<SlaOption> slaOptions = sched.getExecutionOptions().getSlaOptions();
      final ExecutionOptions flowOptions = sched.getExecutionOptions();

      if (slaOptions != null && slaOptions.size() > 0) {
        ret.put(PARAM_SLA_EMAILS, slaOptions.get(0).getEmails());

        final List<Object> setObj = new ArrayList<>();
        for (final SlaOption slaOption : slaOptions) {
          setObj.add(slaOption.toWebObject());
        }
        ret.put(PARAM_SETTINGS, setObj);
      } else if (flowOptions != null) {
        if (flowOptions.getFailureEmails() != null) {
          final List<String> emails = flowOptions.getFailureEmails();
          if (emails.size() > 0) {
            ret.put(PARAM_SLA_EMAILS, emails);
          }
        }
      } else {
        if (flow.getFailureEmails() != null) {
          final List<String> emails = flow.getFailureEmails();
          if (emails.size() > 0) {
            ret.put(PARAM_SLA_EMAILS, emails);
          }
        }
      }

      final List<String> allJobs = new ArrayList<>();
      for (final Node n : flow.getNodes()) {
        allJobs.add(n.getId());
      }

      ret.put(PARAM_ALL_JOB_NAMES, allJobs);
    } catch (final ServletException e) {
      ret.put(PARAM_ERROR, e);
    } catch (final ScheduleManagerException e) {
      logger.error(e.getMessage(), e);
      ret.put(PARAM_ERROR, e);
    }
  }

  protected Project getProjectAjaxByPermission(final Map<String, Object> ret,
      final int projectId, final User user, final Permission.Type type) {
    return filterProjectByPermission(this.projectManager.getProject(projectId), user, type, ret);
  }

  private void handleGetAllSchedules(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException, IOException {

    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/scheduledflowpage.vm");

    final List<Schedule> schedules;
    try {
      schedules = this.scheduleManager.getSchedules();
    } catch (final ScheduleManagerException e) {
      throw new ServletException(e);
    }
    page.add("schedules", schedules);
    page.render();
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      final HashMap<String, Object> ret = new HashMap<>();
      if (hasParam(req, "action")) {
        final String action = getParam(req, "action");
        if (API_SCHEDULE_FLOW.equals(action)) {
          ajaxScheduleFlow(req, ret, session.getUser());
        } else if (API_SCHEDULE_CRON_FLOW.equals(action)) {
          ajaxScheduleCronFlow(req, ret, session.getUser());
        } else if (API_REMOVE_SCHED.equals(action)) {
          ajaxRemoveSched(req, ret, session.getUser());
        }
      }

      if (ret.get(PARAM_STATUS) == (STATUS_SUCCESS)) {
        setSuccessMessageInCookie(resp, (String) ret.get(PARAM_MESSAGE));
      } else {
        setErrorMessageInCookie(resp, (String) ret.get(PARAM_MESSAGE));
      }

      this.writeJSON(resp, ret);
    }
  }

  private void ajaxRemoveSched(final HttpServletRequest req, final Map<String, Object> ret,
      final User user) throws ServletException {
    final int scheduleId = getIntParam(req, PARAM_SCHEDULE_ID);
    final Schedule sched;
    try {
      sched = this.scheduleManager.getSchedule(scheduleId);
    } catch (final ScheduleManagerException e) {
      throw new ServletException(e);
    }
    if (sched == null) {
      ret.put(PARAM_MESSAGE, "Schedule with ID " + scheduleId + " does not exist");
      ret.put(PARAM_STATUS, STATUS_ERROR);
      return;
    }

    final Project project = this.projectManager.getProject(sched.getProjectId());

    if (project == null) {
      ret.put(PARAM_MESSAGE, "Project " + sched.getProjectId() + " does not exist");
      ret.put(PARAM_STATUS, STATUS_ERROR);
      return;
    }

    if (!hasPermission(project, user, Type.SCHEDULE)) {
      ret.put(PARAM_STATUS, STATUS_ERROR);
      ret.put(PARAM_MESSAGE, "Permission denied. Cannot remove schedule with id "
          + scheduleId);
      return;
    }

    this.scheduleManager.removeSchedule(sched);
    logger.info("User '" + user.getUserId() + " has removed schedule "
        + sched.getScheduleName());
    this.projectManager
        .postProjectEvent(project, EventType.SCHEDULE, user.getUserId(),
            "Schedule " + sched.toString() + " has been removed.");

    ret.put(PARAM_STATUS, STATUS_SUCCESS);
    ret.put(PARAM_MESSAGE, "flow " + sched.getFlowName()
        + " removed from Schedules.");
    return;
  }

  @Deprecated
  private void ajaxScheduleFlow(final HttpServletRequest req,
      final HashMap<String, Object> ret, final User user) throws ServletException {
    final String projectName = getParam(req, "projectName");
    final String flowName = getParam(req, "flow");
    final int projectId = getIntParam(req, "projectId");

    final Project project = this.projectManager.getProject(projectId);

    if (project == null) {
      ret.put(PARAM_MESSAGE, "Project " + projectName + " does not exist");
      ret.put(PARAM_STATUS, STATUS_ERROR);
      return;
    }

    if (!hasPermission(project, user, Type.SCHEDULE)) {
      ret.put(PARAM_STATUS, STATUS_ERROR);
      ret.put(PARAM_MESSAGE, "Permission denied. Cannot execute " + flowName);
      return;
    }

    final Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put(PARAM_STATUS, STATUS_ERROR);
      ret.put(PARAM_MESSAGE, "Flow " + flowName + " cannot be found in project "
          + projectName);
      return;
    }

    final String scheduleTime = getParam(req, "scheduleTime");
    final String scheduleDate = getParam(req, "scheduleDate");
    final DateTime firstSchedTime;
    try {
      firstSchedTime = parseDateTime(scheduleDate, scheduleTime);
    } catch (final Exception e) {
      ret.put(PARAM_ERROR, "Invalid date and/or time '" + scheduleDate + " "
          + scheduleTime);
      return;
    }

    final long endSchedTime = getLongParam(req, "endSchedTime",
        Constants.DEFAULT_SCHEDULE_END_EPOCH_TIME);
    try {
      // Todo kunkun-tang: Need to verify if passed end time is valid.
    } catch (final Exception e) {
      ret.put(PARAM_ERROR, "Invalid date and time: " + endSchedTime);
      return;
    }

    ReadablePeriod thePeriod = null;
    try {
      if (hasParam(req, "is_recurring")
          && getParam(req, "is_recurring").equals("on")) {
        thePeriod = TimeUtils.parsePeriodString(getParam(req, "period"));
      }
    } catch (final Exception e) {
      ret.put(PARAM_ERROR, e.getMessage());
    }

    ExecutionOptions flowOptions = null;
    try {
      flowOptions = HttpRequestUtils.parseFlowOptions(req);
      HttpRequestUtils.filterAdminOnlyFlowParams(this.userManager, flowOptions, user);
    } catch (final Exception e) {
      ret.put(PARAM_ERROR, e.getMessage());
    }

    final Schedule schedule =
        this.scheduleManager.scheduleFlow(-1, projectId, projectName, flowName,
            "ready", firstSchedTime.getMillis(), endSchedTime, firstSchedTime.getZone(),
            thePeriod, DateTime.now().getMillis(), firstSchedTime.getMillis(),
            firstSchedTime.getMillis(), user.getUserId(), flowOptions);
    logger.info("User '" + user.getUserId() + "' has scheduled " + "["
        + projectName + flowName + " (" + projectId + ")" + "].");
    this.projectManager.postProjectEvent(project, EventType.SCHEDULE,
        user.getUserId(), "Schedule " + schedule.toString()
            + " has been added.");

    ret.put(PARAM_STATUS, STATUS_SUCCESS);
    ret.put(PARAM_SCHEDULE_ID, schedule.getScheduleId());
    ret.put(PARAM_MESSAGE, projectName + "." + flowName + " scheduled.");
  }

  /**
   * This method is in charge of doing cron scheduling.
   */
  private void ajaxScheduleCronFlow(final HttpServletRequest req,
      final HashMap<String, Object> ret, final User user) throws ServletException {
    final String projectName = getParam(req, "projectName");
    final String flowName = getParam(req, "flow");

    final Project project = this.projectManager.getProject(projectName);

    if (project == null) {
      ret.put(PARAM_MESSAGE, "Project " + projectName + " does not exist");
      ret.put(PARAM_STATUS, STATUS_ERROR);
      return;
    }
    final int projectId = project.getId();

    if (!hasPermission(project, user, Type.SCHEDULE)) {
      ret.put(PARAM_STATUS, STATUS_ERROR);
      ret.put(PARAM_MESSAGE, "Permission denied. Cannot execute " + flowName);
      return;
    }

    final Flow flow = project.getFlow(flowName);
    if (flow == null) {
      ret.put(PARAM_STATUS, STATUS_ERROR);
      ret.put(PARAM_MESSAGE, "Flow " + flowName + " cannot be found in project "
          + projectName);
      return;
    }

    if (flow.isLocked()) {
      ret.put(PARAM_STATUS, STATUS_ERROR);
      ret.put(PARAM_MESSAGE, "Flow " + flowName + " in project " + projectName + " is locked.");
      return;
    }

    final boolean hasFlowTrigger;
    try {
      hasFlowTrigger = this.projectManager.hasFlowTrigger(project, flow);
    } catch (final Exception ex) {
      logger.error(ex);
      ret.put(PARAM_STATUS, STATUS_ERROR);
      ret.put(PARAM_MESSAGE, String.format("Error looking for flow trigger of flow: %s.%s ",
          projectName, flowName));
      return;
    }

    if (hasFlowTrigger) {
      ret.put(PARAM_STATUS, STATUS_ERROR);
      ret.put(PARAM_MESSAGE, String.format("<font color=\"red\"> Error: Flow %s.%s is already "
              + "associated with flow trigger, so schedule has to be defined in flow trigger config </font>",
          projectName, flowName));
      return;
    }

    final DateTimeZone timezone = DateTimeZone.getDefault();
    final DateTime firstSchedTime = getPresentTimeByTimezone(timezone);

    String cronExpression = null;
    try {
      if (hasParam(req, "cronExpression")) {
        // everything in Azkaban functions is at the minute granularity, so we add 0 here
        // to let the expression to be complete.
        cronExpression = getParam(req, "cronExpression");
        if (azkaban.utils.Utils.isCronExpressionValid(cronExpression, timezone) == false) {
          ret.put(PARAM_ERROR,
              "This expression <" + cronExpression + "> can not be parsed to quartz cron.");
          return;
        }
      }
      if (cronExpression == null) {
        throw new Exception("Cron expression must exist.");
      }
    } catch (final Exception e) {
      ret.put(PARAM_ERROR, e.getMessage());
    }

    final long endSchedTime = getLongParam(req, "endSchedTime",
        Constants.DEFAULT_SCHEDULE_END_EPOCH_TIME);
    try {
      // Todo kunkun-tang: Need to verify if passed end time is valid.
    } catch (final Exception e) {
      ret.put(PARAM_ERROR, "Invalid date and time: " + endSchedTime);
      return;
    }

    ExecutionOptions flowOptions = null;
    try {
      flowOptions = HttpRequestUtils.parseFlowOptions(req);
      HttpRequestUtils.filterAdminOnlyFlowParams(this.userManager, flowOptions, user);
    } catch (final Exception e) {
      ret.put(PARAM_ERROR, e.getMessage());
    }

    // Because either cronExpression or recurrence exists, we build schedule in the below way.
    final Schedule schedule = this.scheduleManager
        .cronScheduleFlow(-1, projectId, projectName, flowName,
            "ready", firstSchedTime.getMillis(), endSchedTime, firstSchedTime.getZone(),
            DateTime.now().getMillis(), firstSchedTime.getMillis(),
            firstSchedTime.getMillis(), user.getUserId(), flowOptions,
            cronExpression);

    logger.info("User '" + user.getUserId() + "' has scheduled " + "["
        + projectName + flowName + " (" + projectId + ")" + "].");
    this.projectManager.postProjectEvent(project, EventType.SCHEDULE,
        user.getUserId(), "Schedule " + schedule.toString()
            + " has been added.");
    this.projectManager.postScheduleEvent(project, azkaban.spi.EventType.SCHEDULE_CREATED, user,
            schedule, null);

    ret.put(PARAM_STATUS, STATUS_SUCCESS);
    ret.put(PARAM_SCHEDULE_ID, schedule.getScheduleId());
    ret.put(PARAM_MESSAGE, projectName + "." + flowName + " scheduled.");
  }

  private DateTime parseDateTime(final String scheduleDate, final String scheduleTime) {
    // scheduleTime: 12,00,pm,PDT
    final String[] parts = scheduleTime.split(",", -1);
    int hour = Integer.parseInt(parts[0]);
    final int minutes = Integer.parseInt(parts[1]);
    final boolean isPm = parts[2].equalsIgnoreCase("pm");

    final DateTimeZone timezone =
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

    if (isPm) {
      hour += 12;
    }

    final DateTime firstSchedTime =
        day.withHourOfDay(hour).withMinuteOfHour(minutes).withSecondOfMinute(0);

    return firstSchedTime;
  }

  /**
   * @param cronTimezone represents the timezone from remote API call
   * @return if the string is equal to UTC, we return UTC; otherwise, we always return default
   * timezone.
   */
  private DateTimeZone parseTimeZone(final String cronTimezone) {
    if (cronTimezone != null && cronTimezone.equals("UTC")) {
      return DateTimeZone.UTC;
    }

    return DateTimeZone.getDefault();
  }

  private DateTime getPresentTimeByTimezone(final DateTimeZone timezone) {
    return new DateTime(timezone);
  }
}
