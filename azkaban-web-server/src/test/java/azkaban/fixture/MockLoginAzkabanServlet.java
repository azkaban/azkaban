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


import azkaban.server.AzkabanServer;
import azkaban.server.session.Session;
import azkaban.server.session.SessionCache;
import azkaban.user.UserManager;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.user.User;
import org.apache.velocity.app.VelocityEngine;
import org.mockito.Spy;
import org.mortbay.jetty.Server;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.Mockito.*;

public class MockLoginAzkabanServlet extends LoginAbstractAzkabanServlet {

    private static final String SESSION_ID_NAME = "azkaban.browser.session.id";

    public static HttpServletRequest getRequestWithNoUpstream(String clientIp, String sessionId, String requestMethod){

        HttpServletRequest req = mock(HttpServletRequest.class);

        when(req.getRemoteAddr()).thenReturn(clientIp);
        when(req.getHeader("x-forwarded-for")).thenReturn(null);
        when(req.getMethod()).thenReturn(requestMethod);
        when(req.getContentType()).thenReturn("application/x-www-form-urlencoded");

        // Requires sessionId to be passed that is in the application's session cache
        when(req.getParameter("session.id")).thenReturn(sessionId);

        return req;
    }

    public static HttpServletRequest getRequestWithUpstream(String clientIp, String upstreamIp, String sessionId, String requestMethod){

        HttpServletRequest req = mock(HttpServletRequest.class);

        when(req.getRemoteAddr()).thenReturn("2.2.2.2:9999");
        when(req.getHeader("x-forwarded-for")).thenReturn(upstreamIp);
        when(req.getMethod()).thenReturn(requestMethod);
        when(req.getContentType()).thenReturn("application/x-www-form-urlencoded");

        // Requires sessionId to be passed that is in the application's session cache
        when(req.getParameter("session.id")).thenReturn(sessionId);

        return req;
    }

    public static HttpServletRequest getRequestWithMultipleUpstreams(String clientIp, String upstreamIp, String sessionId, String requestMethod){

        HttpServletRequest req = mock(HttpServletRequest.class);

        when(req.getRemoteAddr()).thenReturn("2.2.2.2:9999");
        when(req.getHeader("x-forwarded-for")).thenReturn(upstreamIp + ",1.1.1.1,3.3.3.3:33333");
        when(req.getMethod()).thenReturn(requestMethod);
        when(req.getContentType()).thenReturn("application/x-www-form-urlencoded");

        // Requires sessionId to be passed that is in the application's session cache
        when(req.getParameter("session.id")).thenReturn(sessionId);

        return req;
    }

    public static MockLoginAzkabanServlet getServletWithSession(String sessionId,
                                                                String username, String clientIp)
            throws Exception{

        MockLoginAzkabanServlet servlet = new MockLoginAzkabanServlet();

        Server server = mock(Server.class);
        Props props = new Props();
        UserManager userManager = mock(UserManager.class);

        // Need to mock and inject an application instance into the servlet
        AzkabanWebServer app = mock(AzkabanWebServer.class);

        MockLoginAzkabanServlet servletSpy = spy(servlet);

        when(servletSpy.getApplication()).thenReturn(app);

        // Create a concrete SessionCache so a session will get persisted
        // and can get looked up
        SessionCache cache = new SessionCache(props);
        when(app.getSessionCache()).thenReturn(cache);

        // Need a valid object here when processing a request
        when(app.getVelocityEngine()).thenReturn(mock(VelocityEngine.class));

        // Construct and store a session in the servlet
        azkaban.user.User user = mock(azkaban.user.User.class);
        when(user.getEmail()).thenReturn(username + "@mail.com");
        when(user.getUserId()).thenReturn(username);

        Session session = new Session(sessionId, user, clientIp);
        servletSpy.getApplication().getSessionCache().addSession(session);


        // Return the servletSpy since we replaced implementation for 'getApplication'
        return servletSpy;
    }

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
            throws ServletException, IOException {

        resp.getWriter().write("SUCCESS_MOCK_LOGIN_SERVLET");
    }

    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
            throws ServletException, IOException {

        resp.getWriter().write("SUCCESS_MOCK_LOGIN_SERVLET");
    }
}
