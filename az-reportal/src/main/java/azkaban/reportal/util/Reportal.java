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

package azkaban.reportal.util;

import java.io.File;

import java.text.SimpleDateFormat;
import java.text.DateFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormat;

import azkaban.executor.ExecutionOptions;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.viewer.reportal.ReportalMailCreator;
import azkaban.viewer.reportal.ReportalTypeManager;

public class Reportal {
  private static Logger logger = Logger.getLogger(Reportal.class);

  public static final String REPORTAL_CONFIG_PREFIX = "reportal.config.";
  public static final String REPORTAL_CONFIG_PREFIX_REGEX =
    "^reportal[.]config[.].+";
  public static final String REPORTAL_CONFIG_PREFIX_NEGATION_REGEX =
    "(?!(^reportal[.]config[.])).+";

  public static final String ACCESS_LIST_SPLIT_REGEX =
      "\\s*,\\s*|\\s*;\\s*|\\s+";

  // One Schedule's default End Time: 01/01/2050, 00:00:00, UTC
  private static final long DEFAULT_SCHEDULE_END_EPOCH_TIME = 2524608000000L;

  public String reportalUser;
  public String ownerEmail;

  public String title;
  public String description;

  public List<Query> queries;
  public List<Variable> variables;

  public boolean schedule;
  public String scheduleHour;
  public String scheduleMinute;
  public String scheduleAmPm;
  public String scheduleTimeZone;
  public String scheduleDate;
  public boolean scheduleRepeat;
  public String scheduleIntervalQuantity;
  public String scheduleInterval;
  public String endSchedule;

  public boolean renderResultsAsHtml;

  public String accessViewer;
  public String accessExecutor;
  public String accessOwner;

  public String notifications;
  public String failureNotifications;

  public Project project;

  public void saveToProject(Project project) {
    this.project = project;

    project.getMetadata().put("reportal-user", reportalUser);
    project.getMetadata().put("owner-email", ownerEmail);

    project.getMetadata().put("title", title);
    project.setDescription(description);

    project.getMetadata().put("schedule", schedule);
    project.getMetadata().put("scheduleHour", scheduleHour);
    project.getMetadata().put("scheduleMinute", scheduleMinute);
    project.getMetadata().put("scheduleAmPm", scheduleAmPm);
    project.getMetadata().put("scheduleTimeZone", scheduleTimeZone);
    project.getMetadata().put("scheduleDate", scheduleDate);
    project.getMetadata().put("scheduleRepeat", scheduleRepeat);
    project.getMetadata().put("scheduleIntervalQuantity",
        scheduleIntervalQuantity);
    project.getMetadata().put("scheduleInterval", scheduleInterval);
    project.getMetadata().put("endSchedule", endSchedule);

    project.getMetadata().put("renderResultsAsHtml", renderResultsAsHtml);

    project.getMetadata().put("accessViewer", accessViewer);
    project.getMetadata().put("accessExecutor", accessExecutor);
    project.getMetadata().put("accessOwner", accessOwner);

    project.getMetadata().put("queryNumber", queries.size());
    for (int i = 0; i < queries.size(); i++) {
      Query query = queries.get(i);
      project.getMetadata().put("query" + i + "title", query.title);
      project.getMetadata().put("query" + i + "type", query.type);
      project.getMetadata().put("query" + i + "script", query.script);
    }

    project.getMetadata().put("variableNumber", variables.size());
    for (int i = 0; i < variables.size(); i++) {
      Variable variable = variables.get(i);
      project.getMetadata().put("variable" + i + "title", variable.title);
      project.getMetadata().put("variable" + i + "name", variable.name);
    }

    project.getMetadata().put("notifications", notifications);
    project.getMetadata().put("failureNotifications", failureNotifications);
  }

  public void removeSchedules(ScheduleManager scheduleManager)
      throws ScheduleManagerException {
    List<Flow> flows = project.getFlows();
    for (Flow flow : flows) {
      Schedule sched =
          scheduleManager.getSchedule(project.getId(), flow.getId());
      if (sched != null) {
        scheduleManager.removeSchedule(sched);
      }
    }
  }

  public void updateSchedules(Reportal report, ScheduleManager scheduleManager,
      User user, Flow flow) throws ScheduleManagerException {
    // Clear previous schedules
    removeSchedules(scheduleManager);
    // Add new schedule
    if (schedule) {
      int hour =
          (Integer.parseInt(scheduleHour) % 12)
              + (scheduleAmPm.equalsIgnoreCase("pm") ? 12 : 0);
      int minute = Integer.parseInt(scheduleMinute) % 60;
      DateTimeZone timeZone =
          scheduleTimeZone.equalsIgnoreCase("UTC") ? DateTimeZone.UTC
              : DateTimeZone.getDefault();
      DateTime firstSchedTime =
          DateTimeFormat.forPattern("MM/dd/yyyy").withZone(timeZone)
              .parseDateTime(scheduleDate);
      firstSchedTime =
          firstSchedTime.withHourOfDay(hour).withMinuteOfHour(minute)
              .withSecondOfMinute(0).withMillisOfSecond(0);

      ReadablePeriod period = null;
      if (scheduleRepeat) {
        int intervalQuantity = Integer.parseInt(scheduleIntervalQuantity);

        if (scheduleInterval.equals("y")) {
          period = Years.years(intervalQuantity);
        } else if (scheduleInterval.equals("m")) {
          period = Months.months(intervalQuantity);
        } else if (scheduleInterval.equals("w")) {
          period = Weeks.weeks(intervalQuantity);
        } else if (scheduleInterval.equals("d")) {
          period = Days.days(intervalQuantity);
        } else if (scheduleInterval.equals("h")) {
          period = Hours.hours(intervalQuantity);
        } else if (scheduleInterval.equals("M")) {
          period = Minutes.minutes(intervalQuantity);
        }
      }

      ExecutionOptions options = new ExecutionOptions();
      options.getFlowParameters().put("reportal.execution.user",
          user.getUserId());
      options.getFlowParameters().put("reportal.title", report.title);
      options.getFlowParameters().put("reportal.render.results.as.html",
          report.renderResultsAsHtml ? "true" : "false");
      options.setMailCreator(ReportalMailCreator.REPORTAL_MAIL_CREATOR);

      long endScheduleTime = report.endSchedule == null ?
          DEFAULT_SCHEDULE_END_EPOCH_TIME: parseDateToEpoch(report.endSchedule);

      logger.info("This report scheudle end time is " + endScheduleTime);

      scheduleManager.scheduleFlow(-1, project.getId(), project.getName(),
          flow.getId(), "ready", firstSchedTime.getMillis(), endScheduleTime,
          firstSchedTime.getZone(), period, DateTime.now().getMillis(),
          firstSchedTime.getMillis(), firstSchedTime.getMillis(),
          user.getUserId(), options, null);
    }
  }

  private long parseDateToEpoch(String date) throws ScheduleManagerException {
    DateFormat dffrom = new SimpleDateFormat("MM/dd/yyyy h:mm a");
    try {
      // this string will be parsed according to system's timezone setting.
      return dffrom.parse(date).getTime();
    } catch (Exception ex) {
      throw new ScheduleManagerException("can not parse this date " + date);
    }
  }

  /**
   * Updates the project permissions in MEMORY, but does NOT update the project
   * in the database.
   */
  public void updatePermissions() {
    String[] accessViewerList =
        accessViewer.trim().split(ACCESS_LIST_SPLIT_REGEX);
    String[] accessExecutorList =
        accessExecutor.trim().split(ACCESS_LIST_SPLIT_REGEX);
    String[] accessOwnerList =
        accessOwner.trim().split(ACCESS_LIST_SPLIT_REGEX);
    // Prepare permission types
    Permission admin = new Permission();
    admin.addPermission(Type.READ);
    admin.addPermission(Type.EXECUTE);
    admin.addPermission(Type.ADMIN);
    Permission executor = new Permission();
    executor.addPermission(Type.READ);
    executor.addPermission(Type.EXECUTE);
    Permission viewer = new Permission();
    viewer.addPermission(Type.READ);
    // Sets the permissions
    project.clearUserPermission();
    for (String user : accessViewerList) {
      user = user.trim();
      if (!user.isEmpty()) {
        project.setUserPermission(user, viewer);
      }
    }
    for (String user : accessExecutorList) {
      user = user.trim();
      if (!user.isEmpty()) {
        project.setUserPermission(user, executor);
      }
    }
    for (String user : accessOwnerList) {
      user = user.trim();
      if (!user.isEmpty()) {
        project.setUserPermission(user, admin);
      }
    }
    project.setUserPermission(reportalUser, admin);
  }

  public void createZipAndUpload(ProjectManager projectManager, User user,
      String reportalStorageUser) throws Exception {
    // Create temp folder to make the zip file for upload
    File tempDir = Utils.createTempDir();
    File dataDir = new File(tempDir, "data");
    dataDir.mkdirs();

    // Create all job files
    String dependentJob = null;
    List<String> jobs = new ArrayList<String>();
    Map<String, String> extraProps =
      ReportalUtil.getVariableMapByPrefix(variables, REPORTAL_CONFIG_PREFIX);
    for (Query query : queries) {
      // Create .job file
      File jobFile =
          ReportalHelper.findAvailableFileName(dataDir,
              ReportalHelper.sanitizeText(query.title), ".job");

      String fileName = jobFile.getName();
      String jobName = fileName.substring(0, fileName.length() - 4);
      jobs.add(jobName);

      // Populate the job file
      ReportalTypeManager.createJobAndFiles(this, jobFile, jobName,
          query.title, query.type, query.script, dependentJob, reportalUser,
          extraProps);

      // For dependency of next query
      dependentJob = jobName;
    }

    // Create the data collector job
    if (dependentJob != null) {
      String jobName = "data-collector";

      // Create .job file
      File jobFile =
          ReportalHelper.findAvailableFileName(dataDir,
              ReportalHelper.sanitizeText(jobName), ".job");
      Map<String, String> extras = new HashMap<String, String>();
      extras.put("reportal.job.number", Integer.toString(jobs.size()));
      for (int i = 0; i < jobs.size(); i++) {
        extras.put("reportal.job." + i, jobs.get(i));
      }
      ReportalTypeManager.createJobAndFiles(this, jobFile, jobName, "",
          ReportalTypeManager.DATA_COLLECTOR_JOB, "", dependentJob,
          reportalStorageUser, extras);
    }

    // Zip jobs together
    File archiveFile = new File(tempDir, project.getName() + ".zip");
    Utils.zipFolderContent(dataDir, archiveFile);

    // Upload zip
    projectManager.uploadProject(project, archiveFile, "zip", user, null);

    // Empty temp
    if (tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  public void loadImmutableFromProject(Project project) {
    reportalUser = stringGetter.get(project.getMetadata().get("reportal-user"));
    ownerEmail = stringGetter.get(project.getMetadata().get("owner-email"));
  }

  /**
   * @return A set of users explicitly granted viewer access to the report.
   */
  public Set<String> getAccessViewers() {
    Set<String> viewers = new HashSet<String>();
    for (String user : accessViewer.trim().split(ACCESS_LIST_SPLIT_REGEX)) {
      if (!user.isEmpty()) {
        viewers.add(user);
      }
    }
    return viewers;
  }

  /**
   * @return A set of users explicitly granted executor access to the report.
   */
  public Set<String> getAccessExecutors() {
    Set<String> executors = new HashSet<String>();
    for (String user : accessExecutor.trim().split(ACCESS_LIST_SPLIT_REGEX)) {
      if (!user.isEmpty()) {
        executors.add(user);

      }
    }
    return executors;
  }

  public static Reportal loadFromProject(Project project) {
    if (project == null) {
      return null;
    }

    Reportal reportal = new Reportal();
    Map<String, Object> metadata = project.getMetadata();

    reportal.loadImmutableFromProject(project);

    if (reportal.reportalUser == null || reportal.reportalUser.isEmpty()) {
      return null;
    }

    reportal.title = stringGetter.get(metadata.get("title"));
    reportal.description = project.getDescription();
    int queries = intGetter.get(project.getMetadata().get("queryNumber"));
    int variables = intGetter.get(project.getMetadata().get("variableNumber"));

    reportal.schedule = boolGetter.get(project.getMetadata().get("schedule"));
    reportal.scheduleHour =
        stringGetter.get(project.getMetadata().get("scheduleHour"));
    reportal.scheduleMinute =
        stringGetter.get(project.getMetadata().get("scheduleMinute"));
    reportal.scheduleAmPm =
        stringGetter.get(project.getMetadata().get("scheduleAmPm"));
    reportal.scheduleTimeZone =
        stringGetter.get(project.getMetadata().get("scheduleTimeZone"));
    reportal.scheduleDate =
        stringGetter.get(project.getMetadata().get("scheduleDate"));
    reportal.scheduleRepeat =
        boolGetter.get(project.getMetadata().get("scheduleRepeat"));
    reportal.scheduleIntervalQuantity =
        stringGetter.get(project.getMetadata().get("scheduleIntervalQuantity"));
    reportal.scheduleInterval =
        stringGetter.get(project.getMetadata().get("scheduleInterval"));
    reportal.endSchedule =
        stringGetter.get(project.getMetadata().get("endSchedule"));

    reportal.renderResultsAsHtml =
        boolGetter.get(project.getMetadata().get("renderResultsAsHtml"));

    reportal.accessViewer =
        stringGetter.get(project.getMetadata().get("accessViewer"));
    reportal.accessExecutor =
        stringGetter.get(project.getMetadata().get("accessExecutor"));
    reportal.accessOwner =
        stringGetter.get(project.getMetadata().get("accessOwner"));

    reportal.notifications =
        stringGetter.get(project.getMetadata().get("notifications"));
    reportal.failureNotifications =
        stringGetter.get(project.getMetadata().get("failureNotifications"));

    reportal.queries = new ArrayList<Query>();

    for (int i = 0; i < queries; i++) {
      Query query = new Query();
      reportal.queries.add(query);
      query.title =
          stringGetter.get(project.getMetadata().get("query" + i + "title"));
      query.type =
          stringGetter.get(project.getMetadata().get("query" + i + "type"));
      query.script =
          stringGetter.get(project.getMetadata().get("query" + i + "script"));
    }

    reportal.variables = new ArrayList<Variable>();

    for (int i = 0; i < variables; i++) {
      String title =
          stringGetter.get(project.getMetadata().get("variable" + i + "title"));
      String name =
          stringGetter.get(project.getMetadata().get("variable" + i + "name"));
      Variable variable = new Variable(title, name);
      reportal.variables.add(variable);
    }

    reportal.project = project;

    return reportal;
  }

  public static Getter<Boolean> boolGetter = new Getter<Boolean>(false,
      Boolean.class);
  public static Getter<Integer> intGetter = new Getter<Integer>(0,
      Integer.class);
  public static Getter<String> stringGetter = new Getter<String>("",
      String.class);

  public static class Getter<T> {
    Class<?> cls;
    T defaultValue;

    public Getter(T defaultValue, Class<?> cls) {
      this.cls = cls;
      this.defaultValue = defaultValue;
    }

    @SuppressWarnings("unchecked")
    public T get(Object object) {
      if (object == null || !(cls.isAssignableFrom(object.getClass()))) {
        return defaultValue;
      }
      return (T) object;
    }
  }

  public static class Query {
    public String title;
    public String type;
    public String script;

    public String getTitle() {
      return title;
    }

    public String getType() {
      return type;
    }

    public String getScript() {
      return script;
    }
  }

  public static class Variable {
    public String title;
    public String name;

    public Variable() {}

    public Variable(String title, String name) {
      this.title = title;
      this.name = name;
    }

    public String getTitle() {
      return title;
    }

    public String getName() {
      return name;
    }
  }
}
