/*
 * Copyright 2018 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.project;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_ENABLED;
import static azkaban.Constants.EventReporterConstants.FLOW_NAME;
import static azkaban.Constants.EventReporterConstants.MODIFIED_BY;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static azkaban.Constants.DEFAULT_SCHEDULE_END_EPOCH_TIME;

import azkaban.AzkabanCommonModule;
import azkaban.db.DatabaseOperator;
import azkaban.event.EventListener;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutorLoader;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.metrics.CommonMetrics;
import azkaban.scheduler.Schedule;
import azkaban.sla.SlaType;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.Storage;
import azkaban.storage.ProjectStorageManager;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.utils.Emailer;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.ValidatorUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.HashMap;

/**
 * Test class for project manager
 */
public class ProjectManagerTest {

  private static final String flowName = "flow1";
  // For project upload event
  private static final String PROJECT_ZIP = "Archive.zip";
  private static final String IPv4 = "111.111.111.111";
  private static final String IPv6 = "2607:f0d0:1002:0051:0000:0000:0000:0004";
  // For schedule & sla event
  private static final DateTimeZone timezone = DateTimeZone.getDefault();
  private static final DateTime firstSchedTime = new DateTime(timezone);
  private static final long endSchedTime = DEFAULT_SCHEDULE_END_EPOCH_TIME;
  private static final String cronExpression = "0 3 13 13 * ? 2020";
  private static final Duration dur = Duration.ofMinutes(10 + 0 * 60);
  private static final String id = "";
  private static final String slaEmail = "noreply@linkedin.com";
  private static final SlaType type = SlaType.FLOW_FINISH;
  // For job override property event
  private static final String jobName = "job1";
  private static final String diffMsg = "Newly created Properties: [ type, command], [ command, sleep 2], [ dependencies, job0], \n";
  private final int ID = 107;
  private final int VERSION = 10;

  private ProjectManager manager;
  private AzkabanProjectLoader azkabanProjectLoader;
  private ProjectLoader projectLoader;
  private ProjectStorageManager projectStorageManager;
  private Props props;
  private ExecutorLoader executorLoader;
  private DatabaseOperator dbOperator;
  private Storage storage;
  private ArchiveUnthinner archiveUnthinner;
  private ValidatorUtils validatorUtils;
  private CommonMetrics commonMetrics;
  private ProjectCache cache;
  private AzkabanEventReporter azkabanEventReporter;
  private Project project;
  private AlerterHolder alerterHolder;
  private Emailer emailer;

  @Before
  public void setUp() throws Exception {
    this.props = new Props();
    this.projectStorageManager = mock(ProjectStorageManager.class);
    this.projectLoader = mock(ProjectLoader.class);
    this.executorLoader = mock(ExecutorLoader.class);
    this.dbOperator = mock(DatabaseOperator.class);
    this.storage = mock(Storage.class);
    this.archiveUnthinner = mock(ArchiveUnthinner.class);
    this.validatorUtils = mock(ValidatorUtils.class);
    this.commonMetrics = mock(CommonMetrics.class);
    this.cache = new InMemoryProjectCache(this.projectLoader);
    this.azkabanProjectLoader = new AzkabanProjectLoader(this.props, this.commonMetrics,
        this.projectLoader,
        this.projectStorageManager, mock(FlowLoaderFactory.class), this.executorLoader,
        this.dbOperator,
        this.storage, this.archiveUnthinner,
        this.validatorUtils);

    this.emailer = mock(Emailer.class);
    this.alerterHolder = new AlerterHolder(this.props, emailer);
    this.manager = new ProjectManager(this.azkabanProjectLoader, this.projectLoader,
        this.projectStorageManager, this.props, this.cache, this.alerterHolder);
    // Set up project and azkaban event reporter
    props.put(AZKABAN_EVENT_REPORTING_ENABLED, "true");
    props.put(AZKABAN_EVENT_REPORTING_CLASS_PARAM,
            "azkaban.project.AzkabanEventReporterTest");
    props.put("database.type", "h2");
    props.put("h2.path", "h2");
    final Injector injector = Guice.createInjector( new AzkabanCommonModule(props));
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(injector);
    this.project = new Project(this.ID, "project1");
    this.project.setVersion(this.VERSION);
  }

  @Test
  public void testProjectAzkabanEventReport() throws IOException {
    assertThat(this.project.getAzkabanEventReporter()).isNotNull();
    assertThat(this.project.getAzkabanEventReporter()).isInstanceOf(AzkabanEventReporterTest.class);
    // Check creating a project from Object and its azkaban event reporter
    final Object obj = project.toObject();
    final String json = JSONUtils.toJSON(obj);
    final Object jsonObj = JSONUtils.parseJSONFromString(json);
    final Project parsedProject = Project.projectFromObject(jsonObj);
    assertThat(parsedProject.getAzkabanEventReporter()).isNotNull();
    assertThat(parsedProject.getAzkabanEventReporter()).isInstanceOf(AzkabanEventReporterTest.class);
  }

  @Test
  public void testCreateProjectsWithDifferentCases() {
    final String projectName = "mytestproject";
    final String projectDescription = "This is my new project with lower cases.";
    final User user = new User("testUser1");
    when(this.projectLoader.createNewProject(projectName, projectDescription, user))
        .thenReturn(new Project(1, projectName));
    this.manager.createProject(projectName, projectDescription, user);
    final String projectName2 = "MYTESTPROJECT";
    final String projectDescription2 = "This is my new project with UPPER CASES.";
    assertThatThrownBy(
        () -> this.manager.createProject(projectName2, projectDescription2, user))
        .isInstanceOf(ProjectManagerException.class)
        .hasMessageContaining(
            "Project already exists.");
  }

  @Test
  public void testUploadEvent() throws Exception {
    final URL resource = requireNonNull(getClass().getClassLoader().getResource("sample_flow_01.zip"));
    final File projectZipFile = new File(resource.getPath());
    final User uploader = new User("testUser1");
    this.project.setVersion(this.VERSION);
    // Add a customized event listener
    project.addListener((event) -> {
      ProjectEvent projectEvent = (ProjectEvent) event;
      // Check upload event data metadata
      Assert.assertEquals("Event metadata not created as expected.", "testUser1", projectEvent.getEventData().get("modifiedBy"));
      Assert.assertEquals("Event metadata not created as expected.", IPv4, projectEvent.getEventData().get("uploaderIPAddress"));
      Assert.assertTrue("Event metadata not created as expected.", (long)projectEvent.getEventData().get("projectUploadTime") >= 0);
      Assert.assertTrue("Event metadata not created as expected.", (long)projectEvent.getEventData().get("projectZipSize") >= 0);
      Assert.assertEquals("Event metadata not created as expected.", "FAT_ZIP", projectEvent.getEventData().get("zipType"));
      Assert.assertEquals("Event metadata not created as expected.", "ERROR", projectEvent.getEventData().get("projectEventStatus"));
      Assert.assertEquals("Event metadata not created as expected.", "java.lang.NullPointerException", projectEvent.getEventData().get("errorMessage"));
    });
    assertThatThrownBy(
            () -> this.azkabanProjectLoader.uploadProject(this.project, projectZipFile, "zip", uploader, this.props, IPv4))
            .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testScheduleEvent() {
    final User user = new User("testUser2");
    // Add a customized event listener
    project.addListener((event) -> {
      ProjectEvent projectEvent = (ProjectEvent) event;
      // Check schedule event metadata
      Assert.assertEquals("Event metadata not created as expected.", "testUser2",
          projectEvent.getEventData().get(MODIFIED_BY));
      Assert.assertEquals("Event metadata not created as expected.", flowName,
          projectEvent.getEventData().get(FLOW_NAME));
      Assert.assertEquals("Event metadata not created as expected.", firstSchedTime.getMillis(), projectEvent.getEventData().get("firstScheduledExecutionTime"));
      Assert.assertEquals("Event metadata not created as expected.", endSchedTime, projectEvent.getEventData().get("lastScheduledExecutionTime"));
      Assert.assertEquals("Event metadata not created as expected.", timezone.toString(), projectEvent.getEventData().get("timezone"));
      Assert.assertEquals("Event metadata not created as expected.", cronExpression, projectEvent.getEventData().get("cronExpression"));
      Assert.assertEquals("Event metadata not created as expected.", "SUCCESS", projectEvent.getEventData().get("projectEventStatus"));
      Assert.assertEquals("Event metadata not created as expected.", "null", projectEvent.getEventData().get("errorMessage"));
    });
    // Create a new schedule
    final Schedule schedule =
            new Schedule(0, project.getId(), project.getName(), flowName, "schedule",
                    firstSchedTime.getMillis(), endSchedTime, timezone, null, DateTime.now().getMillis(),
                    firstSchedTime.getMillis(), firstSchedTime.getMillis(), user.getUserId(), null, cronExpression, false);
    manager.postScheduleEvent(project, azkaban.spi.EventType.SCHEDULE_CREATED, user,
            schedule, null);
  }

  @Test
  public void testPermissionEvent() {
    final User user = new User("testUser4"), modifier = new User("Dementor1");
    Permission perm1 = new Permission(), perm2 = new Permission();
    perm1.setPermission(Permission.Type.READ, true);
    perm1.setPermission(Permission.Type.WRITE, true);
    perm1.setPermission(Permission.Type.EXECUTE, true);
    perm2.setPermission(Permission.Type.ADMIN,true);
    project.setGroupPermission("group1", perm1);
    // Add customized event listener
    EventListener<ProjectEvent> listener1 = ((event) -> {
      ProjectEvent projectEvent = (ProjectEvent) event;
      Assert.assertEquals("Event metadata not created as expected.",  "Dementor1",
          projectEvent.getEventData().get(MODIFIED_BY));
      Assert.assertEquals("Event metadata not created as expected.", "testUser4", projectEvent.getEventData().get("updatedUser"));
      Assert.assertEquals("Event metadata not created as expected.",  "null", projectEvent.getEventData().get("updatedGroup"));
      Assert.assertEquals("Event metadata not created as expected.", "ADMIN", projectEvent.getEventData().get("permission"));
      Assert.assertEquals("Event metadata not created as expected.", "SUCCESS", projectEvent.getEventData().get("projectEventStatus"));
      Assert.assertEquals("Event metadata not created as expected.", "null", projectEvent.getEventData().get("errorMessage"));

      String updatedGroupPermissions = (String) projectEvent.getEventData().get("updatedGroupPermissions");
      Assert.assertNotNull("Event metadata not created as expected.", updatedGroupPermissions);
      Assert.assertTrue("Event metadata not created as expected.", updatedGroupPermissions.matches("((\\w+)=(\\w+,?)+:?)+"));
      Assert.assertTrue("Event metadata not created as expected.", updatedGroupPermissions.contains("READ")
          && updatedGroupPermissions.contains("WRITE") && updatedGroupPermissions.contains("EXECUTE"));
      Assert.assertEquals("Event metadata not created as expected.", "", projectEvent.getEventData().get("updatedUserPermissions"));
    });
    project.addListener(listener1);
    // Add a user permission
    manager.updateProjectPermission(project, user.getUserId(), perm2, false, modifier);

    project.removeListener(listener1);
    EventListener<ProjectEvent> listener2 = ((event) -> {
      ProjectEvent projectEvent = (ProjectEvent) event;
      Assert.assertEquals("Event metadata not created as expected.",  "Dementor1",
          projectEvent.getEventData().get(MODIFIED_BY));
      Assert.assertEquals("Event metadata not created as expected.", "null", projectEvent.getEventData().get("updatedUser"));
      Assert.assertEquals("Event metadata not created as expected.",  "group1", projectEvent.getEventData().get("updatedGroup"));
      Assert.assertEquals("Event metadata not created as expected.", "remove", projectEvent.getEventData().get("permission"));
      Assert.assertEquals("Event metadata not created as expected.", "SUCCESS", projectEvent.getEventData().get("projectEventStatus"));
    });
    project.addListener(listener2);
    // Remove a group_permission
    manager.removeProjectPermission(project, "group1", true, modifier);
  }

  @Test
  public void testJobOverrideProperty() throws Exception {
    final User user = new User("testUser5");
    // Set job override property in a flow
    final Flow flow = new Flow(flowName);
    // Set override email list for the flow
    List<String> overrideEmailList = new ArrayList<>();
    overrideEmailList.add("dementer@azkaban.org");
    flow.addOverrideEmails(overrideEmailList);
    final Node node = new Node(jobName);
    flow.addNode(node);
    final Map<String, Flow> flowMap = new HashMap();
    flowMap.put(flowName, flow);
    project.setFlows(flowMap);
    final Map<String, String> jobParamGroup = new HashMap();
    jobParamGroup.put("type", "command");
    jobParamGroup.put("command", "sleep 2");
    jobParamGroup.put("dependencies", "job0");
    final Props overrideParams = new Props(null, jobParamGroup);
    final Map<String, Object> eventData = new HashMap<>();
    // Add customized event listener
    project.addListener((event) -> {
      ProjectEvent projectEvent = (ProjectEvent) event;
      Assert.assertEquals("Event metadata not created as expected.", flowName,
          projectEvent.getEventData().get(FLOW_NAME));
      Assert.assertEquals("Event metadata not created as expected.", jobName, projectEvent.getEventData().get("jobOverridden"));
      Assert.assertEquals("Event metadata not created as expected.", diffMsg, projectEvent.getEventData().get("diffMessage"));
      Assert.assertEquals("Event metadata not created as expected.", "SUCCESS", projectEvent.getEventData().get("projectEventStatus"));
    });
    // Override a job property
    manager.setJobOverrideProperty(project, flow, overrideParams, jobName, node.getJobSource(), user);
    verify(this.emailer).alertOnJobPropertyOverridden(any(), any(), any());
    }
}