/*
 * Copyright 2019 LinkedIn Corp.
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

import static azkaban.test.Utils.initServiceProvider;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import azkaban.project.DirectoryYamlFlowLoader;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleChangeEmailerManager;
import azkaban.scheduler.ScheduleManager;
import azkaban.server.session.Session;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.CSRFTokenUtility;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.velocity.app.VelocityEngine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ProjectManagerServletTest {

  private final ScheduleManager scheduleManager = mock(ScheduleManager.class);
  private final ProjectManagerServlet projectManagerServlet = mock(ProjectManagerServlet.class);
  private final ScheduleChangeEmailerManager scheduleChangeEmailerManager = mock(ScheduleChangeEmailerManager.class);

  @Test
  public void testRemoveScheduleOfDeletedFlows() throws Exception {
    final Project project = new Project(1, "myTestProject");
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(project, ExecutionsTestUtil.getFlowDir("multipleflowyamltest"));
    project.setFlows(loader.getFlowMap());
    Assert.assertEquals(2, project.getFlows().size());
    final Set<String> flowNameList =
        project.getFlows().stream().map(f -> f.getId()).collect(Collectors.toSet());
    Assert.assertTrue(flowNameList.containsAll(Arrays.asList("basic_flow", "basic_flow2")));

    final List<Schedule> schedules = new ArrayList<>();
    final long now = System.currentTimeMillis();
    final Schedule sched1 = new Schedule(1, 1, "myTestProject", "basic_flow_renamed", "ready", now,
        now, null, null, now, now, now, "testUser1", null, null, false);
    schedules.add(sched1);
    final Schedule sched2 = new Schedule(2, 1, "myTestProject", "basic_flow2", "ready", now + 20,
        now + 20, null, null, now + 20, now + 20, now + 20, "testUser1", null, null, false);
    schedules.add(sched2);
    final Schedule sched3 = new Schedule(3, 3, "anotherTestProject", "anotherFlow", "ready",
        now + 30, now + 30, null, null, now + 30, now + 30, now + 30, "testUser3", null, null, false);
    schedules.add(sched3);

    when(this.scheduleManager.getAllSchedules()).thenReturn(new ArrayList<>(schedules));
    doAnswer(invocation -> schedules.remove(invocation.getArguments()[0]))
        .when(this.scheduleManager).removeSchedule(any(Schedule.class));
    this.projectManagerServlet
        .removeScheduleOfDeletedFlows(project, this.scheduleManager, this.scheduleChangeEmailerManager, schedule -> {
        });
    Assert.assertEquals(2, schedules.size());
    Assert.assertTrue(schedules.containsAll(Arrays.asList(sched2, sched3)));
  }

  @Test
  public void verifyCallingAddCSRFTokenForPermissionsPage() throws ServletException, IOException {
    Session session = new Session(UUID.randomUUID().toString(), new User("luke"), "0.0.0.0");

    ProjectManagerServlet projectManagerServlet = getSpyProjectManagerServlet();
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    doReturn("testProject").when(req).getParameter("project");
    doReturn(Boolean.TRUE).when(projectManagerServlet).hasParam(req, "project");
    doReturn(Boolean.TRUE).when(projectManagerServlet).hasParam(req, "permissions");

    projectManagerServlet.handleGet(req, resp, session);
    Mockito.verify(projectManagerServlet, times(1))
        .addCSRFTokenToPage(Mockito.any(), Mockito.any());
  }

  @Test
  public void verifyCallingValidateCSRFToken()
      throws ServletException, IOException {
    assertCallingValidateCSRFToken("removeProxyUser");
    assertCallingValidateCSRFToken("addProxyUser");
    assertCallingValidateCSRFToken("addPermission");
    assertCallingValidateCSRFToken("changePermission");
  }

  private void assertCallingValidateCSRFToken(String type) throws IOException, ServletException {
    Session session = new Session(UUID.randomUUID().toString(), new User("luke"), "0.0.0.0");
    CSRFTokenUtility csrfTokenUtility = CSRFTokenUtility.getCSRFTokenUtility();
    String csrfTokenFromSession = csrfTokenUtility.getCSRFTokenFromSession(session);

    ProjectManagerServlet projectManagerServlet = getSpyProjectManagerServlet();
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    doReturn(csrfTokenFromSession).when(req).getHeader("CSRFTOKEN");
    doReturn(Boolean.TRUE).when(projectManagerServlet).hasParam(req, "project");
    doReturn("testProject").when(req).getParameter("project");
    doReturn(Boolean.TRUE).when(projectManagerServlet).hasParam(req, "ajax");

    // Following relies on the order of the else-if statement of
    //method azkaban.webapp.servlet.ProjectManagerServlet.handleAJAXAction

    doReturn(type).when(req).getParameter("ajax");
    projectManagerServlet.handlePost(req, resp, session);
    Mockito.verify(projectManagerServlet, times(1))
        .validateCSRFToken(Mockito.any());
  }

  private ProjectManagerServlet getSpyProjectManagerServlet() throws IOException {
    initServiceProvider();

    AzkabanWebServer mockAzkbanServer = mock(AzkabanWebServer.class);
    VelocityEngine mockVelocityEngine = mock(VelocityEngine.class);
    ProjectManager mockProjectManager = mock(ProjectManager.class);

    Project testProject = new Project(123, "testProject");
    doReturn(testProject).when(mockProjectManager).getProject("testProject");

    when(mockAzkbanServer.getVelocityEngine()).thenReturn(mockVelocityEngine);
    ProjectManagerServlet projectManagerServlet = Mockito.spy(ProjectManagerServlet.class);
    when(projectManagerServlet.getApplication()).thenReturn(mockAzkbanServer);
    projectManagerServlet.setProjectManager(mockProjectManager);
    doNothing().when(projectManagerServlet).writeJSON(Mockito.any(), Mockito.any());

    return projectManagerServlet;
  }

}
