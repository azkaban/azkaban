/*
 * Copyright 2018 LinkedIn Corp.
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

import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import java.io.IOException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class FlowTriggerServlet extends LoginAbstractAzkabanServlet {

  private static final long serialVersionUID = 1L;
  private FlowTriggerScheduler scheduler;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.scheduler = server.getScheduler();
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    handlePage(req, resp, session);
  }

  private void handlePage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/flowtriggerspage.vm");

    page.add("flowTriggers", this.scheduler.getScheduledFlowTriggerJobs());
    page.render();
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
  }
}
