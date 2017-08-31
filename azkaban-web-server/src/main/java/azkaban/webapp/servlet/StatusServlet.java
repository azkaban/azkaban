/*
 * Copyright 2017 LinkedIn Corp.
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

import static azkaban.webapp.servlet.AbstractAzkabanServlet.JSON_MIME_TYPE;

import azkaban.webapp.StatusService;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusServlet extends HttpServlet {

  private static final Logger log = LoggerFactory.getLogger(StatusServlet.class);

  private final StatusService statusService;

  public StatusServlet(final StatusService statusService) {
    this.statusService = statusService;
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      resp.setContentType(JSON_MIME_TYPE);
      resp.getOutputStream()
          .println(new GsonBuilder()
              .setPrettyPrinting()
              .create()
              .toJson(this.statusService.getStatus()));
      resp.setStatus(HttpServletResponse.SC_OK);
    } catch (final Exception e) {
      log.error("Error!! while reporting status: ", e);
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
    } finally {
      resp.getOutputStream().close();
    }
  }
}
