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
package azkaban.execapp;

import azkaban.Constants;
import azkaban.executor.ConnectorParams;
import azkaban.server.HttpRequestUtils;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class JMXHttpServlet extends HttpServlet implements ConnectorParams {

  private static final long serialVersionUID = -3085603824826446270L;
  private AzkabanExecutorServer server;

  @Override
  public void init(final ServletConfig config) {
    this.server =
        (AzkabanExecutorServer) config.getServletContext().getAttribute(
            Constants.AZKABAN_SERVLET_CONTEXT_KEY);
  }

  public boolean hasParam(final HttpServletRequest request, final String param) {
    return HttpRequestUtils.hasParam(request, param);
  }

  public String getParam(final HttpServletRequest request, final String name)
      throws ServletException {
    return HttpRequestUtils.getParam(request, name);
  }

  /**
   * @deprecated GET available for seamless upgrade. azkaban-web now uses POST.
   */
  @Deprecated
  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    final Map<String, Object> ret = new HashMap<>();

    if (hasParam(req, JMX_GET_MBEANS)) {
      ret.put("mbeans", this.server.getMBeanRegistrationManager().getMBeanNames());
    } else if (hasParam(req, JMX_GET_ALL_MBEAN_ATTRIBUTES)) {
      if (!hasParam(req, JMX_MBEAN)) {
        ret.put("error", "Parameters 'mbean' must be set");
      } else {
        ret.putAll(
            this.server.getMBeanRegistrationManager().getMBeanResult(getParam(req, JMX_MBEAN)));
      }
    }

    JSONUtils.toJSON(ret, resp.getOutputStream(), true);
  }
}
