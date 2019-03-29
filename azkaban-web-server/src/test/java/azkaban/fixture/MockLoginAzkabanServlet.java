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

package azkaban.fixture;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import azkaban.server.session.Session;
import azkaban.server.session.SessionCache;
import azkaban.user.UserManager;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.velocity.app.VelocityEngine;
import org.mortbay.jetty.Server;

public class MockLoginAzkabanServlet extends LoginAbstractAzkabanServlet {

  private static final String SESSION_ID_NAME = "azkaban.browser.session.id";
  private static final long serialVersionUID = 5872898140052356540L;

  public static HttpServletRequest getRequestWithNoUpstream(final String clientIp,
      final String sessionId,
      final String requestMethod) {

    final HttpServletRequest req = mock(HttpServletRequest.class);

    when(req.getRemoteAddr()).thenReturn(clientIp);
    when(req.getHeader("x-forwarded-for")).thenReturn(null);
    when(req.getMethod()).thenReturn(requestMethod);
    when(req.getContentType()).thenReturn("application/x-www-form-urlencoded");

    // Requires sessionId to be passed that is in the application's session cache
    when(req.getParameter("session.id")).thenReturn(sessionId);

    return req;
  }

  public static HttpServletRequest getRequestWithUpstream(final String clientIp,
      final String upstreamIp,
      final String sessionId, final String requestMethod) {

    final HttpServletRequest req = mock(HttpServletRequest.class);

    when(req.getRemoteAddr()).thenReturn("2.2.2.2:9999");
    when(req.getHeader("x-forwarded-for")).thenReturn(upstreamIp);
    when(req.getMethod()).thenReturn(requestMethod);
    when(req.getContentType()).thenReturn("application/x-www-form-urlencoded");

    // Requires sessionId to be passed that is in the application's session cache
    when(req.getParameter("session.id")).thenReturn(sessionId);

    return req;
  }

  public static HttpServletRequest getRequestWithMultipleUpstreams(final String clientIp,
      final String upstreamIp, final String sessionId, final String requestMethod) {

    final HttpServletRequest req = mock(HttpServletRequest.class);

    when(req.getRemoteAddr()).thenReturn("2.2.2.2:9999");
    when(req.getHeader("x-forwarded-for")).thenReturn(upstreamIp + ",1.1.1.1,3.3.3.3:33333");
    when(req.getMethod()).thenReturn(requestMethod);
    when(req.getContentType()).thenReturn("application/x-www-form-urlencoded");

    // Requires sessionId to be passed that is in the application's session cache
    when(req.getParameter("session.id")).thenReturn(sessionId);

    return req;
  }

  public static MockLoginAzkabanServlet getServletWithSession(final String sessionId,
      final String username, final String clientIp)
      throws Exception {

    final MockLoginAzkabanServlet servlet = new MockLoginAzkabanServlet();

    final Server server = mock(Server.class);
    final Props props = new Props();
    final UserManager userManager = mock(UserManager.class);

    // Need to mock and inject an application instance into the servlet
    final AzkabanWebServer app = mock(AzkabanWebServer.class);

    final MockLoginAzkabanServlet servletSpy = spy(servlet);

    when(servletSpy.getApplication()).thenReturn(app);

    // Create a concrete SessionCache so a session will get persisted
    // and can get looked up
    final SessionCache cache = new SessionCache(props, null);
    when(app.getSessionCache()).thenReturn(cache);

    // Need a valid object here when processing a request
    when(app.getVelocityEngine()).thenReturn(mock(VelocityEngine.class));

    // Construct and store a session in the servlet
    final azkaban.user.User user = mock(azkaban.user.User.class);
    when(user.getEmail()).thenReturn(username + "@mail.com");
    when(user.getUserId()).thenReturn(username);

    final Session session = new Session(sessionId, user, clientIp);
    servletSpy.getApplication().getSessionCache().addSession(session);

    // Return the servletSpy since we replaced implementation for 'getApplication'
    return servletSpy;
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session)
      throws ServletException, IOException {

    resp.getWriter().write("SUCCESS_MOCK_LOGIN_SERVLET");
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session)
      throws ServletException, IOException {

    resp.getWriter().write("SUCCESS_MOCK_LOGIN_SERVLET");
  }
}
