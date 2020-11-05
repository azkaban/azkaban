/*
 * Copyright 2020 LinkedIn Corp.
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

import azkaban.ServiceProvider;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.scheduler.ScheduleManager;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.metrics.WebMetrics;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import java.io.IOException;
import javax.servlet.ServletConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

public abstract class LoginAbstractAzkabanServletTestBase {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  @Mock
  protected Injector injector;

  @Mock
  protected AzkabanWebServer azkabanWebServer;
  @Mock
  protected WebMetrics webMetrics;

  @Mock
  protected ProjectManager projectManager;
  @Mock
  protected UserManager userManager;
  @Mock
  protected ExecutorManagerAdapter executorManager;
  @Mock
  protected ScheduleManager scheduleManager;
  @Mock
  protected FlowTriggerService flowTriggerService;

  @Mock
  protected ServletConfig servletConfig;
  @Mock
  protected Session session;
  protected AzkabanMockHttpServletRequest req;
  protected AzkabanMockHttpServletResponse res;

  @Captor
  protected ArgumentCaptor<ExecutableFlow> exFlow;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    ServiceProvider.SERVICE_PROVIDER.unsetInjector();
    ServiceProvider.SERVICE_PROVIDER.setInjector(this.injector);
    Mockito.when(this.injector.getInstance(AzkabanWebServer.class))
        .thenReturn(this.azkabanWebServer);
    Mockito.when(this.injector.getInstance(WebMetrics.class))
        .thenReturn(this.webMetrics);

    // This could possibly load
    // azkaban-public/azkaban-web-server/src/main/resources/conf/azkaban.properties
    // , but so far not needed
    Mockito.when(this.azkabanWebServer.getServerProps()).thenReturn(Props.of());
    Mockito.when(this.azkabanWebServer.getProjectManager()).thenReturn(this.projectManager);
    Mockito.when(this.azkabanWebServer.getUserManager()).thenReturn(this.userManager);
    Mockito.when(this.azkabanWebServer.getExecutorManager()).thenReturn(this.executorManager);
    Mockito.when(this.azkabanWebServer.getScheduleManager()).thenReturn(this.scheduleManager);
    Mockito.when(this.azkabanWebServer.getFlowTriggerService()).thenReturn(this.flowTriggerService);

    mockTesUserWithAdminRole();

    this.req = new AzkabanMockHttpServletRequest();
    this.res = new AzkabanMockHttpServletResponse();
  }

  private void mockTesUserWithAdminRole() {
    final User testAdminUser = new User("testAdminUser");
    testAdminUser.addRole("adminRole");
    final Role adminRole = new Role("adminRole", new Permission(Type.ADMIN));
    Mockito.when(this.userManager.getRole("adminRole")).thenReturn(adminRole);
    Mockito.when(this.session.getUser()).thenReturn(testAdminUser);
  }

  protected void mockTestProjectAndFlow() {
    final Project project = new Project(11, "testProject");
    Mockito.when(this.projectManager.getProject("testProject")).thenReturn(project);
    final Flow flow = new Flow("testFlow");
    project.setFlows(ImmutableMap.of(flow.getId(), flow));
  }

  protected void mockSubmitExecution() throws ExecutorManagerException {
    Mockito.doAnswer(invocation -> {
      final ExecutableFlow exFlow = invocation.getArgument(0, ExecutableFlow.class);
      // Mock the behavior of ExecutorLoader#uploadExecutableFlow: assign an execution id.
      exFlow.setExecutionId(99);
      return "Submitted (mocked)";
    }).when(this.executorManager).submitExecutableFlow(this.exFlow.capture(), Mockito.anyString());
  }

  public static class AzkabanMockHttpServletRequest extends MockHttpServletRequest {

  }

  public static class AzkabanMockHttpServletResponse extends MockHttpServletResponse {

    protected JsonNode getResponseJson() throws IOException {
      return LoginAbstractAzkabanServletTestBase.MAPPER.readTree(this.getContentAsString());
    }

  }

}
