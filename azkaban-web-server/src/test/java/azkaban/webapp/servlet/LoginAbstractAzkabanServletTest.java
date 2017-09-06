/*
 * Copyright 2016 LinkedIn Corp.
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
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.fixture.MockLoginAzkabanServlet;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;

public class LoginAbstractAzkabanServletTest {

  @Before
  public void setUp() {
    // TODO: reallocf remove setUp() when LoginAbstractServlet fully guiced
    initServiceProvider();
  }

  private HttpServletResponse getResponse(final StringWriter stringWriter) {
    final HttpServletResponse resp = mock(HttpServletResponse.class);
    final PrintWriter writer = new PrintWriter(stringWriter);

    try {
      when(resp.getWriter()).thenReturn(writer);
    } catch (final IOException ex) {
      System.out.println(ex);
    }

    return resp;
  }

  @Test
  public void testWhenGetRequestSessionIsValid() throws Exception, IOException, ServletException {

    final String clientIp = "127.0.0.1:10000";
    final String sessionId = "111";

    final HttpServletRequest req = MockLoginAzkabanServlet
        .getRequestWithNoUpstream(clientIp, sessionId, "GET");

    final StringWriter writer = new StringWriter();
    final HttpServletResponse resp = getResponse(writer);

    final MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
        "user", "127.0.0.1");

    servlet.doGet(req, resp);

    // Assert that our response was written (we have a valid session)
    assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
  }

  @Test
  public void testWhenPostRequestSessionIsValid() throws Exception {

    final String clientIp = "127.0.0.1:10000";
    final String sessionId = "111";

    final HttpServletRequest req = MockLoginAzkabanServlet
        .getRequestWithNoUpstream(clientIp, sessionId, "POST");
    final StringWriter writer = new StringWriter();
    final HttpServletResponse resp = getResponse(writer);

    final MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
        "user", "127.0.0.1");

    servlet.doPost(req, resp);

    // Assert that our response was written (we have a valid session)
    assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
  }

  @Test
  public void testWhenPostRequestChangedClientIpSessionIsInvalid() throws Exception {

    final String clientIp = "127.0.0.2:10000";
    final String sessionId = "111";

    final HttpServletRequest req = MockLoginAzkabanServlet
        .getRequestWithNoUpstream(clientIp, sessionId, "POST");

    final StringWriter writer = new StringWriter();
    final HttpServletResponse resp = getResponse(writer);

    final MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
        "user", "127.0.0.1");

    servlet.doPost(req, resp);

    // Assert that our response was written (we have a valid session)
    assertNotSame("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
  }

  @Test
  public void testWhenPostRequestChangedClientPortSessionIsValid() throws Exception {

    final String clientIp = "127.0.0.1:10000";
    final String sessionId = "111";

    final HttpServletRequest req = MockLoginAzkabanServlet
        .getRequestWithNoUpstream(clientIp, sessionId, "POST");

    final StringWriter writer = new StringWriter();
    final HttpServletResponse resp = getResponse(writer);

    final MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
        "user", "127.0.0.1");

    servlet.doPost(req, resp);

    // Assert that our response was written (we have a valid session)
    assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
  }

  @Test
  public void testWhenPostRequestWithUpstreamSessionIsValid() throws Exception {

    final String clientIp = "127.0.0.1:10000";
    final String upstreamIp = "192.168.1.1:11111";
    final String sessionId = "111";

    final HttpServletRequest req = MockLoginAzkabanServlet
        .getRequestWithUpstream(clientIp, upstreamIp,
            sessionId, "POST");

    final StringWriter writer = new StringWriter();
    final HttpServletResponse resp = getResponse(writer);

    final MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
        "user", "192.168.1.1");

    servlet.doPost(req, resp);

    // Assert that our response was written (we have a valid session)
    assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
  }

  @Test
  public void testWhenPostRequestWithMultipleUpstreamsSessionIsValid() throws Exception {

    final String clientIp = "127.0.0.1:10000";
    final String upstreamIp = "192.168.1.1:11111,888.8.8.8:2222,5.5.5.5:5555";
    final String sessionId = "111";

    final HttpServletRequest req = MockLoginAzkabanServlet
        .getRequestWithUpstream(clientIp, upstreamIp,
            sessionId, "POST");

    final StringWriter writer = new StringWriter();
    final HttpServletResponse resp = getResponse(writer);

    final MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
        "user", "192.168.1.1");

    servlet.doPost(req, resp);

    // Assert that our response was written (we have a valid session)
    assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
  }

  /**
   * Simulates users passing username/password via URI where it would be logged by Azkaban Web
   * Server
   */
  @Test
  public void testLoginRevealingCredentialsShouldThrowFailure() throws Exception {

    final String clientIp = "127.0.0.1:10000";
    final String sessionId = "111";
    final String queryString = "action=login&username=azkaban&password=azkaban";
    final String[] mockCredentials = {"azkaban"};
    final HashMap<String, String[]> mockParameterMap = new HashMap<String, String[]>() {
      {
        put("username", mockCredentials);
        put("password", mockCredentials);
      }
    };

    final HttpServletRequest req = MockLoginAzkabanServlet
        .getRequestWithNoUpstream(clientIp, sessionId, "POST");
    when(req.getParameterMap()).thenReturn(mockParameterMap);
    when(req.getQueryString()).thenReturn(queryString);
    final StringWriter writer = new StringWriter();
    final HttpServletResponse resp = getResponse(writer);

    final MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
        "user", "127.0.0.1");

    servlet.doPost(req, resp);

    // Assert that expected error message is returned
    assertEquals("Login error. Must pass username and password in request body", writer.toString());

  }
}
