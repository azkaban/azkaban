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


import azkaban.fixture.MockLoginAzkabanServlet;
import azkaban.server.session.Session;
import azkaban.server.session.SessionCache;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoginAbstractAzkabanServletTest {

    private HttpServletResponse getResponse(StringWriter stringWriter){
        HttpServletResponse resp = mock(HttpServletResponse.class);
        PrintWriter writer = new PrintWriter(stringWriter);

        try{
            when(resp.getWriter()).thenReturn(writer);
        }
        catch(IOException ex){
            System.out.println(ex);
        }

        return resp;
    }

    @Test
    public void testWhenGetRequestSessionIsValid() throws Exception, IOException, ServletException {

        String clientIp = "127.0.0.1:10000";
        String sessionId = "111";


        HttpServletRequest req = MockLoginAzkabanServlet.getRequestWithNoUpstream(clientIp, sessionId, "GET");

        StringWriter writer = new StringWriter();
        HttpServletResponse resp = getResponse(writer);

        MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
                "user", "127.0.0.1");

        servlet.doGet(req, resp);

        // Assert that our response was written (we have a valid session)
        assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
    }

    @Test
    public void testWhenPostRequestSessionIsValid() throws Exception{

        String clientIp = "127.0.0.1:10000";
        String sessionId = "111";


        HttpServletRequest req = MockLoginAzkabanServlet.getRequestWithNoUpstream(clientIp, sessionId, "POST");
        StringWriter writer = new StringWriter();
        HttpServletResponse resp = getResponse(writer);

        MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
                "user", "127.0.0.1");


        servlet.doPost(req, resp);

        // Assert that our response was written (we have a valid session)
        assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
    }

    @Test
    public void testWhenPostRequestChangedClientIpSessionIsInvalid() throws Exception{

        String clientIp = "127.0.0.2:10000";
        String sessionId = "111";


        HttpServletRequest req = MockLoginAzkabanServlet.getRequestWithNoUpstream(clientIp, sessionId, "POST");

        StringWriter writer = new StringWriter();
        HttpServletResponse resp = getResponse(writer);


        MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
                "user", "127.0.0.1");


        servlet.doPost(req, resp);

        // Assert that our response was written (we have a valid session)
        assertNotSame("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
    }

    @Test
    public void testWhenPostRequestChangedClientPortSessionIsValid() throws Exception{

        String clientIp = "127.0.0.1:10000";
        String sessionId = "111";


        HttpServletRequest req = MockLoginAzkabanServlet.getRequestWithNoUpstream(clientIp, sessionId, "POST");

        StringWriter writer = new StringWriter();
        HttpServletResponse resp = getResponse(writer);


        MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
                "user", "127.0.0.1");


        servlet.doPost(req, resp);

        // Assert that our response was written (we have a valid session)
        assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
    }

    @Test
    public void testWhenPostRequestWithUpstreamSessionIsValid() throws Exception{

        String clientIp = "127.0.0.1:10000";
        String upstreamIp = "192.168.1.1:11111";
        String sessionId = "111";


        HttpServletRequest req = MockLoginAzkabanServlet.getRequestWithUpstream(clientIp, upstreamIp,
                sessionId, "POST");

        StringWriter writer = new StringWriter();
        HttpServletResponse resp = getResponse(writer);


        MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
                "user", "192.168.1.1");


        servlet.doPost(req, resp);

        // Assert that our response was written (we have a valid session)
        assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
    }

    @Test
    public void testWhenPostRequestWithMultipleUpstreamsSessionIsValid() throws Exception{

        String clientIp = "127.0.0.1:10000";
        String upstreamIp = "192.168.1.1:11111,888.8.8.8:2222,5.5.5.5:5555";
        String sessionId = "111";


        HttpServletRequest req = MockLoginAzkabanServlet.getRequestWithUpstream(clientIp, upstreamIp,
                sessionId, "POST");

        StringWriter writer = new StringWriter();
        HttpServletResponse resp = getResponse(writer);


        MockLoginAzkabanServlet servlet = MockLoginAzkabanServlet.getServletWithSession(sessionId,
                "user", "192.168.1.1");


        servlet.doPost(req, resp);

        // Assert that our response was written (we have a valid session)
        assertEquals("SUCCESS_MOCK_LOGIN_SERVLET", writer.toString());
    }
}
