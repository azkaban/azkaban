/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.server;

import azkaban.Constants;
import java.io.IOException;
import java.io.OutputStream;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.codehaus.jackson.map.ObjectMapper;

public class AbstractServiceServlet extends HttpServlet {

  public static final String JSON_MIME_TYPE = "application/json";
  private static final long serialVersionUID = 1L;
  private AzkabanServer application;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    this.application =
        (AzkabanServer) config.getServletContext()
            .getAttribute(Constants.AZKABAN_SERVLET_CONTEXT_KEY);

    if (this.application == null) {
      throw new IllegalStateException(
          "No batch application is defined in the servlet context!");
    }
  }

  protected void writeJSON(final HttpServletResponse resp, final Object obj)
      throws IOException {
    resp.setContentType(JSON_MIME_TYPE);
    final ObjectMapper mapper = new ObjectMapper();
    final OutputStream stream = resp.getOutputStream();
    mapper.writeValue(stream, obj);
  }

  public boolean hasParam(final HttpServletRequest request, final String param) {
    return request.getParameter(param) != null;
  }

  public String getParam(final HttpServletRequest request, final String name)
      throws ServletException {
    final String p = request.getParameter(name);
    if (p == null) {
      throw new ServletException("Missing required parameter '" + name + "'.");
    } else {
      return p;
    }
  }

  public String getParam(final HttpServletRequest request, final String name,
      final String defaultVal) {
    final String p = request.getParameter(name);
    if (p == null) {
      return defaultVal;
    }

    return p;
  }

  public int getIntParam(final HttpServletRequest request, final String name)
      throws ServletException {
    final String p = getParam(request, name);
    return Integer.parseInt(p);
  }

  public int getIntParam(final HttpServletRequest request, final String name,
      final int defaultVal) {
    if (hasParam(request, name)) {
      try {
        return getIntParam(request, name);
      } catch (final Exception e) {
        return defaultVal;
      }
    }
    return defaultVal;
  }

  public long getLongParam(final HttpServletRequest request, final String name)
      throws ServletException {
    final String p = getParam(request, name);
    return Long.parseLong(p);
  }

  public long getLongParam(final HttpServletRequest request, final String name,
      final long defaultVal) {
    if (hasParam(request, name)) {
      try {
        return getLongParam(request, name);
      } catch (final Exception e) {
        return defaultVal;
      }
    }
    return defaultVal;
  }
}
