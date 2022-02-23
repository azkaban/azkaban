/*
 * Copyright 2022 LinkedIn Corp.
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
package azkaban.webapp.servlet;

import azkaban.server.AzkabanAPI;
import java.io.IOException;
import java.util.Arrays;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * This Servlet class is for admin APIs. One of those APIs returns GOOD response to integrate it
 * with health checker.
 */
public class AdminServlet extends AbstractAzkabanServlet {
  private final static Logger logger = Logger
      .getLogger(AdminServlet.class);
  public AdminServlet() {
    super(Arrays.asList(new AzkabanAPI("", "")));
  }

  /**
   * Currently, /admin only returns GOOD. This use case is to verify health of a service. In
   * future, more admin related GET APIs can be covered here.
   * @param req
   * @param resp
   * @throws ServletException
   * @throws IOException
   */
  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      resp.setContentType(JSON_MIME_TYPE);
      resp.getOutputStream().println("GOOD");
      resp.setStatus(HttpServletResponse.SC_OK);
    } catch (final Exception e) {
      logger.error("Error!! while reporting status: ", e);
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
    } finally {
      resp.getOutputStream().close();
    }
  }
}
