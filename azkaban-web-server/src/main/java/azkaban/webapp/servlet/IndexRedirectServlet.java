/*
 * Copyright 2012 LinkedIn Corp.
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

import azkaban.server.session.Session;
import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The main page
 */
public class IndexRedirectServlet extends LoginAbstractAzkabanServlet {

  private static final long serialVersionUID = -1;
  private String defaultServletPath;

  public IndexRedirectServlet(final String defaultServletPath) {
    super(new ArrayList<>());
    this.defaultServletPath = defaultServletPath;
    if (this.defaultServletPath.isEmpty()
        || this.defaultServletPath.equals("/")) {
      this.defaultServletPath = "/index";
    }
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    resp.sendRedirect(req.getContextPath() + this.defaultServletPath);
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    resp.sendRedirect(req.getContextPath() + this.defaultServletPath);
  }
}
