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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import azkaban.executor.AbstractExecutorManagerAdapter;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.executor.DummyEventListener;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.metrics.DummyContainerizationMetricsImpl;
import azkaban.sla.SlaAction;
import azkaban.sla.SlaOption;
import azkaban.sla.SlaType;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.ServletException;
import org.codehaus.jackson.JsonNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ExecutorServletTest extends LoginAbstractAzkabanServletTestBase {

  private ExecutorServlet executorServlet;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.executorServlet = new ExecutorServlet();
    this.executorServlet.init(this.servletConfig);
  }

  /**
   * When executeFlow is called with SLA options, a new execution should be submitted with SLA.
   */
  @Test
  public void postAjaxExecuteFlowWithSlaSettings() throws Exception {
    mockTestProjectAndFlow();
    mockSubmitExecution();
    this.req.addParameter("ajax", "executeFlow");
    this.req.addParameter("project", "testProject");
    this.req.addParameter("flow", "testFlow");
    this.req.addParameter("slaSettings[1]", ",FINISH,2:30,true,false");
    this.req.addParameter("slaEmails", "sla1@example.com,sla2@example.com");

    this.executorServlet.handlePost(this.req, this.res, this.session);

    assertEquals(200, this.res.getStatus());

    final JsonNode json = this.res.getResponseJson();
    assertEquals("testProject", json.path("project").asText());
    assertEquals("testFlow", json.path("flow").asText());
    assertEquals("Submitted (mocked)", json.path("message").asText());
    assertEquals(99, json.path("execid").asInt());

    final List<SlaOption> slaOptions = this.exFlow.getValue().getExecutionOptions().getSlaOptions();
    final List<SlaOption> expected = Arrays.asList(new SlaOption(SlaType.FLOW_FINISH, "testFlow",
        "", Duration.ofMinutes(150), ImmutableSet.of(SlaAction.ALERT),
        ImmutableList.of("sla1@example.com", "sla2@example.com")));
    Assert.assertEquals(expected, slaOptions);
  }

  /**
   * When SLA options can't be parsed, API is expected to return 200 OK with JSON that has "error".
   */
  @Test
  public void postAjaxExecuteFlowWithInvalidSlaSettings() throws Exception {
    mockTestProjectAndFlow();
    mockSubmitExecution();
    this.req.addParameter("ajax", "executeFlow");
    this.req.addParameter("project", "testProject");
    this.req.addParameter("flow", "testFlow");
    this.req.addParameter("slaSettings[1]", "broken-syntax-for-sla");

    this.executorServlet.handlePost(this.req, this.res, this.session);

    assertEquals(200, this.res.getStatus());
    assertEquals("Error parsing flow options: Error parsing SLA setting 'broken-syntax-for-sla': "
            + "java.lang.ArrayIndexOutOfBoundsException: 1",
        this.res.getResponseJson().path("error").asText());
  }

  /**
   * When executeFlow is called without project param, API is expected to raise an exception.
   */
  @Test
  public void postAjaxExecuteFlowWithMissingProjectParameter() throws Exception {
    this.req.addParameter("ajax", "executeFlow");
    try {
      this.executorServlet.handlePost(this.req, this.res, this.session);
      fail("Expected exception was not thrown");
    } catch (final ServletException e) {
      assertEquals("Missing required parameter 'project'.", e.getMessage());
    }
  }

  /**
   * When execution is not found, API is expected to return 200 OK with JSON that has "error".
   */
  @Test
  public void postAjaxFlowInfoErrorFetching() throws Exception {
    this.req.addParameter("ajax", "flowInfo");
    this.req.addParameter("execid", "123");

    this.executorServlet.handlePost(this.req, this.res, this.session);

    assertEquals(200, this.res.getStatus());
    assertEquals("Cannot find execution '123'", this.res.getResponseJson().path("error").asText());
  }

  @Test
  public void testPostAjaxUpdateContainerProperty() throws Exception {
    ContainerizedDispatchManager containerizedDispatchManager = new ContainerizedDispatchManager(
        new Props(), null, null, null, null, null,
        null, null, null, null, new DummyEventListener(),
        new DummyContainerizationMetricsImpl(), null);
    Mockito.when(this.azkabanWebServer.getExecutorManager())
        .thenReturn(containerizedDispatchManager);
    this.executorServlet.init(this.servletConfig);

    this.req.addParameter("ajax", "updateProp");
    this.req.addParameter("propType", "containerDispatch");
    this.req.addParameter("subType", "updateAllowList");
    this.req.addParameter("val", "spark,java");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    Set<String> output = containerizedDispatchManager.getContainerJobTypeCriteria().getAllowList();
    assertEquals(ImmutableSet.of("java", "spark"), output);

    this.req.removeParameter("subType");
    this.req.removeParameter("val");
    this.req.addParameter("subType", "appendAllowList");
    this.req.addParameter("val", "noop");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    output = containerizedDispatchManager.getContainerJobTypeCriteria().getAllowList();
    assertEquals(ImmutableSet.of("java", "spark", "noop"), output);

    this.req.removeParameter("subType");
    this.req.removeParameter("val");
    this.req.addParameter("subType", "removeFromAllowList");
    this.req.addParameter("val", "spark");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    output = containerizedDispatchManager.getContainerJobTypeCriteria().getAllowList();
    assertEquals(ImmutableSet.of("java", "noop"), output);

    this.req.removeParameter("subType");
    this.req.removeParameter("val");
    this.req.addParameter("subType", "updateRampUp");
    this.req.addParameter("val", "7");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    assertEquals(7, containerizedDispatchManager.getContainerRampUpCriteria().getRampUp());

    // test append proxy.to.user deny list
    this.req.removeParameter("subType");
    this.req.removeParameter("val");
    this.req.addParameter("subType", "appendDenyList");
    this.req.addParameter("val","azktest,azkdata,azdev");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    output = containerizedDispatchManager.getContainerProxyUserCriteria().getDenyList();
    assertEquals(ImmutableSet.of("azktest", "azkdata","azdev"), output);

    //test remove proxy.to.user deny list
    this.req.removeParameter("subType");
    this.req.removeParameter("val");
    this.req.addParameter("subType", "removeFromDenyList");
    this.req.addParameter("val","azkdev,azkdata,azktest");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    output = containerizedDispatchManager.getContainerProxyUserCriteria().getDenyList();
    assertEquals(ImmutableSet.of("azdev"), output);
  }

  @Test
  public void testPostAjaxUpdateGeneralProperty() throws Exception {
    AbstractExecutorManagerAdapter abstractExecutorManagerAdapter =
        mock(AbstractExecutorManagerAdapter.class);
    Mockito.doCallRealMethod().when(abstractExecutorManagerAdapter).enableOfflineLogsLoader(anyBoolean());
    Mockito.doCallRealMethod().when(abstractExecutorManagerAdapter).isOfflineLogsLoaderEnabled();
    Mockito.when(this.azkabanWebServer.getExecutorManager())
        .thenReturn(abstractExecutorManagerAdapter);
    this.executorServlet.init(this.servletConfig);

    this.req.addParameter("ajax", "updateProp");
    this.req.addParameter("propType", "general");

    this.req.removeParameter("subType");
    this.req.removeParameter("val");
    this.req.addParameter("subType", "enableOfflineLogsLoader");
    this.req.addParameter("val","true");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    assertTrue(abstractExecutorManagerAdapter.isOfflineLogsLoaderEnabled());

    this.req.removeParameter("subType");
    this.req.removeParameter("val");
    this.req.addParameter("subType", "enableOfflineLogsLoader");
    this.req.addParameter("val","false");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    assertFalse(abstractExecutorManagerAdapter.isOfflineLogsLoaderEnabled());
  }

  @Test
  public void testPostAjaxUpdateContainerizedImplProperty() throws Exception {
    AtomicInteger vpaRampup = new AtomicInteger(0);
    AtomicBoolean vpaEnabled = new AtomicBoolean(true);
    ContainerizedImpl containerizedImpl = mock(ContainerizedImpl.class);
    Mockito.when(containerizedImpl.getVPARampUp()).thenReturn(vpaRampup.get());
    Mockito.when(containerizedImpl.getVPAEnabled()).thenReturn(vpaEnabled.get());

    doAnswer(invocation -> {
      vpaRampup.set(invocation.getArgument(0));
      return null;
    }).when(containerizedImpl).setVPARampUp(anyInt());

    doAnswer(invocation -> {
      vpaEnabled.set(invocation.getArgument(0));
      return null;
    }).when(containerizedImpl).setVPAEnabled(anyBoolean());

    ContainerizedDispatchManager containerizedDispatchManager = new ContainerizedDispatchManager(
        new Props(), null, null, null, null, null,
        null, containerizedImpl, null, null, new DummyEventListener(),
        new DummyContainerizationMetricsImpl(), null);
    Mockito.when(this.azkabanWebServer.getExecutorManager())
        .thenReturn(containerizedDispatchManager);
    this.executorServlet.init(this.servletConfig);

    this.req.addParameter("ajax", "updateProp");
    this.req.addParameter("propType", "containerizedImpl");
    this.req.addParameter("subType", "updateVPARampUp");
    this.req.addParameter("val", "100");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    assertEquals(vpaRampup.get(), 100);

    this.req.removeParameter("val");
    this.req.addParameter("val", "0");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    assertEquals(vpaRampup.get(), 0);

    this.req.removeParameter("subType");
    this.req.addParameter("subType", "updateVPAEnabled");
    this.req.addParameter("val", "false");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    assertFalse(vpaEnabled.get());

    this.req.removeParameter("val");
    this.req.addParameter("val", "true");
    this.executorServlet.handlePost(this.req, this.res, this.session);
    assertTrue(vpaEnabled.get());
  }
}
