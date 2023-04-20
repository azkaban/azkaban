/*
 * Copyright 2021 LinkedIn Corp.
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
 *
 */

package azkaban.container;

import azkaban.executor.ConnectorParams;
import azkaban.executor.ExecutorManagerException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ContainerServletTest {

  private final HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
  private final HttpServletResponse mockHttpServletResponse =
      Mockito.mock(HttpServletResponse.class);
  private final ContainerServlet mockContainerServlet = Mockito.mock(ContainerServlet.class);
  private final FlowContainer flowContainer = Mockito.mock(FlowContainer.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  private void init() throws IOException {
    Mockito.when(this.mockHttpServletRequest.getParameter(ConnectorParams.EXECID_PARAM)).thenReturn(
        "007");
    Mockito.when(this.mockHttpServletRequest.getParameter(ConnectorParams.USER_PARAM))
        .thenReturn("James");
    Mockito.doCallRealMethod().when(this.mockContainerServlet)
        .handleRequest(this.mockHttpServletRequest, this.mockHttpServletResponse);
    ServletOutputStream servletOutputStream = getServletOutputStream(this.baos);
    Mockito.when(this.mockHttpServletResponse.getOutputStream()).thenReturn(servletOutputStream);
  }

  @Test
  public void testPing() throws IOException {
    init();
    Mockito.when(this.mockHttpServletRequest.getParameter(ConnectorParams.ACTION_PARAM))
        .thenReturn(ConnectorParams.PING_ACTION);
    Mockito.doCallRealMethod().when(this.mockContainerServlet)
        .handlePing(Mockito.any());
    this.mockContainerServlet.handleRequest(this.mockHttpServletRequest,
        this.mockHttpServletResponse);
    this.baos.flush();
    Mockito.verify(this.mockContainerServlet, Mockito.times(1)).handlePing(Mockito.any());
    Assert.assertEquals(baos.toString(), "{\"status\":\"alive\"}");
    this.baos.reset();
  }

  /**
   * Verify that the method handleModifyExecutionRequest indeed is getting called.
   * Verify that the method retryFailures for FlowContainer indeed is getting called.
   */
  @Test
  public void testRetryFailures() throws IOException, ServletException, ExecutorManagerException {
    init();
    Mockito.when(this.mockHttpServletRequest.getParameter(ConnectorParams.ACTION_PARAM))
        .thenReturn(ConnectorParams.MODIFY_EXECUTION_ACTION);
    Mockito.when(
        this.mockHttpServletRequest.getParameter(ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE))
        .thenReturn(ConnectorParams.MODIFY_RETRY_FAILURES);
    Mockito.doCallRealMethod().when(this.mockContainerServlet)
        .handleModifyExecutionRequest(Mockito.any(), Mockito.anyInt(), Mockito.anyString(),
            Mockito.any());
    Mockito.doCallRealMethod().when(this.mockContainerServlet).setFlowContainer(Mockito.any());
    this.mockContainerServlet.setFlowContainer(this.flowContainer);
    this.mockContainerServlet.handleRequest(this.mockHttpServletRequest,
        this.mockHttpServletResponse);
    Mockito.verify(this.mockContainerServlet, Mockito.times(1))
        .handleModifyExecutionRequest(Mockito.any(), Mockito.anyInt(), Mockito.anyString(),
            Mockito.any());
    Mockito.verify(this.flowContainer, Mockito.times(1))
        .retryFailures(Mockito.anyInt(), Mockito.anyString());
  }

  @Test
  public void testUnsupportedActions() throws IOException {
    init();
    Mockito.when(this.mockHttpServletRequest.getParameter(ConnectorParams.ACTION_PARAM))
        .thenReturn("SomeAction");
    this.mockContainerServlet.handleRequest(this.mockHttpServletRequest,
        this.mockHttpServletResponse);
    this.baos.flush();
    Assert.assertEquals(baos.toString(), "{\"error\":\"Unsupported action type: SomeAction\"}");
  }

  private ServletOutputStream getServletOutputStream(ByteArrayOutputStream baos) {
    return new ServletOutputStream() {
      @Override
      public void write(int b) throws IOException {
        baos.write(b);
      }
    };
  }
}
