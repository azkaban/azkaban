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
package azkaban.reportal.util;

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
import azkaban.utils.Utils;
import azkaban.viewer.reportal.ReportalMailCreator;
import azkaban.viewer.reportal.ReportalTypeManager;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Reportal {

  private static final Logger LOG = LoggerFactory.getLogger(Reportal.class);
  public static final String REPORTAL_CONFIG_PREFIX = "reportal.config.";
  public static final String REPORTAL_CONFIG_PREFIX_REGEX =
      "^reportal[.]config[.].+";
  public static final String REPORTAL_CONFIG_PREFIX_NEGATION_REGEX =
      "(?!(^reportal[.]config[.])).+";
  public static final String ACCESS_LIST_SPLIT_REGEX =
      "\\s*,\\s*|\\s*;\\s*|\\s+";
  // One Schedule's default End Time: 01/01/2050, 00:00:00, UTC
  private static final long DEFAULT_SCHEDULE_END_EPOCH_TIME = 2524608000000L;
  public static Getter<Boolean> boolGetter = new Getter<>(false,
      Boolean.class);
  public static Getter<Integer> intGetter = new Getter<>(0,
      Integer.class);
  public static Getter<String> stringGetter = new Getter<>("",
      String.class);
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

  public static Reportal loadFromProject(final Project project) {
    if (project == null) {
      return null;
    }

    final Reportal reportal = new Reportal();
    final Map<String, Object> metadata = project.getMetadata();

    reportal.loadImmutableFromProject(project);

    if (reportal.reportalUser == null || reportal.reportalUser.isEmpty()) {
      return null;
    }

    reportal.title = stringGetter.get(metadata.get("title"));
    reportal.description = project.getDescription();
    final int queries = intGetter.get(project.getMetadata().get("queryNumber"));
    final int variables = intGetter.get(project.getMetadata().get("variableNumber"));

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

    reportal.queries = new ArrayList<>();

    for (int i = 0; i < queries; i++) {
      final Query query = new Query();
      reportal.queries.add(query);
      query.title =
          stringGetter.get(project.getMetadata().get("query" + i + "title"));
      query.type =
          stringGetter.get(project.getMetadata().get("query" + i + "type"));
      query.script =
          stringGetter.get(project.getMetadata().get("query" + i + "script"));
    }

    reportal.variables = new ArrayList<>();

    for (int i = 0; i < variables; i++) {
      final String title =
          stringGetter.get(project.getMetadata().get("variable" + i + "title"));
      final String name =
          stringGetter.get(project.getMetadata().get("variable" + i + "name"));
      final Variable variable = new Variable(title, name);
      reportal.variables.add(variable);
    }

    reportal.project = project;

    return reportal;
  }

  public void saveToProject(final Project project) {
    this.project = project;

    project.getMetadata().put("reportal-user", this.reportalUser);
    project.getMetadata().put("owner-email", this.ownerEmail);

    project.getMetadata().put("title", this.title);
    project.setDescription(this.description);

    project.getMetadata().put("schedule", this.schedule);
    project.getMetadata().put("scheduleHour", this.scheduleHour);
    project.getMetadata().put("scheduleMinute", this.scheduleMinute);
    project.getMetadata().put("scheduleAmPm", this.scheduleAmPm);
    project.getMetadata().put("scheduleTimeZone", this.scheduleTimeZone);
    project.getMetadata().put("scheduleDate", this.scheduleDate);
    project.getMetadata().put("scheduleRepeat", this.scheduleRepeat);
    project.getMetadata().put("scheduleIntervalQuantity",
        this.scheduleIntervalQuantity);
    project.getMetadata().put("scheduleInterval", this.scheduleInterval);
    project.getMetadata().put("endSchedule", this.endSchedule);

    project.getMetadata().put("renderResultsAsHtml", this.renderResultsAsHtml);

    project.getMetadata().put("accessViewer", this.accessViewer);
    project.getMetadata().put("accessExecutor", this.accessExecutor);
    project.getMetadata().put("accessOwner", this.accessOwner);

    project.getMetadata().put("queryNumber", this.queries.size());
    for (int i = 0; i < this.queries.size(); i++) {
      final Query query = this.queries.get(i);
      project.getMetadata().put("query" + i + "title", query.title);
      project.getMetadata().put("query" + i + "type", query.type);
      project.getMetadata().put("query" + i + "script", query.script);
    }

    project.getMetadata().put("variableNumber", this.variables.size());
    for (int i = 0; i < this.variables.size(); i++) {
      final Variable variable = this.variables.get(i);
      project.getMetadata().put("variable" + i + "title", variable.title);
      project.getMetadata().put("variable" + i + "name", variable.name);
    }

    project.getMetadata().put("notifications", this.notifications);
    project.getMetadata().put("failureNotifications", this.failureNotifications);
  }

  public void removeSchedules(final ScheduleManager scheduleManager)
      throws ScheduleManagerException {
    final List<Flow> flows = this.project.getFlows();
    for (final Flow flow : flows) {
      final Schedule sched =
          scheduleManager.getSchedule(this.project.getId(), flow.getId());
      if (sched != null) {
        scheduleManager.removeSchedule(sched);
      }
    }
  }

  public void updateSchedules(final Reportal report, final ScheduleManager scheduleManager,
      final User user, final Flow flow) throws ScheduleManagerException {
    // Clear previous schedules
    removeSchedules(scheduleManager);
    // Add new schedule
    if (this.schedule) {
      final int hour =
          (Integer.parseInt(this.scheduleHour) % 12)
              + (this.scheduleAmPm.equalsIgnoreCase("pm") ? 12 : 0);
      final int minute = Integer.parseInt(this.scheduleMinute) % 60;
      final DateTimeZone timeZone =
          this.scheduleTimeZone.equalsIgnoreCase("UTC") ? DateTimeZone.UTC
              : DateTimeZone.getDefault();
      DateTime firstSchedTime =
          DateTimeFormat.forPattern("MM/dd/yyyy").withZone(timeZone)
              .parseDateTime(this.scheduleDate);
      firstSchedTime =
          firstSchedTime.withHourOfDay(hour).withMinuteOfHour(minute)
              .withSecondOfMinute(0).withMillisOfSecond(0);

      ReadablePeriod period = null;
      if (this.scheduleRepeat) {
        final int intervalQuantity = Integer.parseInt(this.scheduleIntervalQuantity);

        if (this.scheduleInterval.equals("y")) {
          period = Years.years(intervalQuantity);
        } else if (this.scheduleInterval.equals("m")) {
          period = Months.months(intervalQuantity);
        } else if (this.scheduleInterval.equals("w")) {
          period = Weeks.weeks(intervalQuantity);
        } else if (this.scheduleInterval.equals("d")) {
          period = Days.days(intervalQuantity);
        } else if (this.scheduleInterval.equals("h")) {
          period = Hours.hours(intervalQuantity);
        } else if (this.scheduleInterval.equals("M")) {
          period = Minutes.minutes(intervalQuantity);
        }
      }

      final ExecutionOptions options = new ExecutionOptions();
      options.getFlowParameters().put("reportal.execution.user",
          user.getUserId());
      options.getFlowParameters().put("reportal.title", report.title);
      options.getFlowParameters().put("reportal.render.results.as.html",
          report.renderResultsAsHtml ? "true" : "false");
      options.setMailCreator(ReportalMailCreator.REPORTAL_MAIL_CREATOR);

      final long endScheduleTime = report.endSchedule == null ?
          DEFAULT_SCHEDULE_END_EPOCH_TIME : parseDateToEpoch(report.endSchedule);

      LOG.info("This report scheudle end time is " + endScheduleTime);

      scheduleManager.scheduleFlow(-1, this.project.getId(), this.project.getName(),
          flow.getId(), "ready", firstSchedTime.getMillis(), endScheduleTime,
          firstSchedTime.getZone(), period, DateTime.now().getMillis(),
          firstSchedTime.getMillis(), firstSchedTime.getMillis(),
          user.getUserId(), options);
    }
  }

  private long parseDateToEpoch(final String date) throws ScheduleManagerException {
    final DateFormat dffrom = new SimpleDateFormat("MM/dd/yyyy h:mm a");
    try {
      // this string will be parsed according to system's timezone setting.
      return dffrom.parse(date).getTime();
    } catch (final Exception ex) {
      throw new ScheduleManagerException("can not parse this date " + date);
    }
  }

  /**
   * Updates the project permissions in MEMORY, but does NOT update the project
   * in the database.
   */
  public void updatePermissions() {
    final String[] accessViewerList =
        this.accessViewer.trim().split(ACCESS_LIST_SPLIT_REGEX);
    final String[] accessExecutorList =
        this.accessExecutor.trim().split(ACCESS_LIST_SPLIT_REGEX);
    final String[] accessOwnerList =
        this.accessOwner.trim().split(ACCESS_LIST_SPLIT_REGEX);
    // Prepare permission types
    final Permission admin = new Permission();
    admin.addPermission(Type.READ);
    admin.addPermission(Type.EXECUTE);
    admin.addPermission(Type.ADMIN);
    final Permission executor = new Permission();
    executor.addPermission(Type.READ);
    executor.addPermission(Type.EXECUTE);
    final Permission viewer = new Permission();
    viewer.addPermission(Type.READ);
    // Sets the permissions
    this.project.clearUserPermission();
    for (String user : accessViewerList) {
      user = user.trim();
      if (!user.isEmpty()) {
        this.project.setUserPermission(user, viewer);
      }
    }
    for (String user : accessExecutorList) {
      user = user.trim();
      if (!user.isEmpty()) {
        this.project.setUserPermission(user, executor);
      }
    }
    for (String user : accessOwnerList) {
      user = user.trim();
      if (!user.isEmpty()) {
        this.project.setUserPermission(user, admin);
      }
    }
    this.project.setUserPermission(this.reportalUser, admin);
  }

  public void createZipAndUpload(final ProjectManager projectManager, final User user,
      final String reportalStorageUser) throws Exception {
    // Create temp folder to make the zip file for upload
    final File tempDir = Utils.createTempDir();
    final File dataDir = new File(tempDir, "data");
    dataDir.mkdirs();

    // Create all job files
    String dependentJob = null;
    final List<String> jobs = new ArrayList<>();
    final Map<String, String> extraProps =
        ReportalUtil.getVariableMapByPrefix(this.variables, REPORTAL_CONFIG_PREFIX);
    for (final Query query : this.queries) {
      // Create .job file
      final File jobFile =
          ReportalHelper.findAvailableFileName(dataDir,
              ReportalHelper.sanitizeText(query.title), ".job");

      final String fileName = jobFile.getName();
      final String jobName = fileName.substring(0, fileName.length() - 4);
      jobs.add(jobName);

      // Populate the job file
      ReportalTypeManager.createJobAndFiles(this, jobFile, jobName,
          query.title, query.type, query.script, dependentJob, this.reportalUser,
          extraProps);

      // For dependency of next query
      dependentJob = jobName;
    }

    // Create the data collector job
    if (dependentJob != null) {
      final String jobName = "data-collector";

      // Create .job file
      final File jobFile =
          ReportalHelper.findAvailableFileName(dataDir,
              ReportalHelper.sanitizeText(jobName), ".job");
      final Map<String, String> extras = new HashMap<>();
      extras.put("reportal.job.number", Integer.toString(jobs.size()));
      for (int i = 0; i < jobs.size(); i++) {
        extras.put("reportal.job." + i, jobs.get(i));
      }
      ReportalTypeManager.createJobAndFiles(this, jobFile, jobName, "",
          ReportalTypeManager.DATA_COLLECTOR_JOB, "", dependentJob,
          reportalStorageUser, extras);
    }

    // Zip jobs together
    final File archiveFile = new File(tempDir, this.project.getName() + ".zip");
    Utils.zipFolderContent(dataDir, archiveFile);

    // Upload zip
    projectManager.uploadProject(this.project, archiveFile, "zip", user, null);

    // Empty temp
    if (tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  public void loadImmutableFromProject(final Project project) {
    this.reportalUser = stringGetter.get(project.getMetadata().get("reportal-user"));
    this.ownerEmail = stringGetter.get(project.getMetadata().get("owner-email"));
  }

  /**
   * @return A set of users explicitly granted viewer access to the report.
   */
  public Set<String> getAccessViewers() {
    final Set<String> viewers = new HashSet<>();
    for (final String user : this.accessViewer.trim().split(ACCESS_LIST_SPLIT_REGEX)) {
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
    final Set<String> executors = new HashSet<>();
    for (final String user : this.accessExecutor.trim().split(ACCESS_LIST_SPLIT_REGEX)) {
      if (!user.isEmpty()) {
        executors.add(user);

      }
    }
    return executors;
  }

  public static class Getter<T> {

    Class<?> cls;
    T defaultValue;

    public Getter(final T defaultValue, final Class<?> cls) {
      this.cls = cls;
      this.defaultValue = defaultValue;
    }

    @SuppressWarnings("unchecked")
    public T get(final Object object) {
      if (object == null || !(this.cls.isAssignableFrom(object.getClass()))) {
        return this.defaultValue;
      }
      return (T) object;
    }
  }

  public static class Query {

    public String title;
    public String type;
    public String script;

    public String getTitle() {
      return this.title;
    }

    public String getType() {
      return this.type;
    }

    public String getScript() {
      return this.script;
    }
  }

  public static class Variable {

    public String title;
    public String name;

    public Variable() {
    }

    public Variable(final String title, final String name) {
      this.title = title;
      this.name = name;
    }

    public String getTitle() {
      return this.title;
    }

    public String getName() {
      return this.name;
    }
  }
}
