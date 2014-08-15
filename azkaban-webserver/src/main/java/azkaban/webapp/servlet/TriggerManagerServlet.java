/*
 * Copyright 2012 LinkedIn, Inc
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.server.session.Session;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerException;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;

public class TriggerManagerServlet extends LoginAbstractAzkabanServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger
      .getLogger(TriggerManagerServlet.class);
  private TriggerManager triggerManager;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    triggerManager = server.getTriggerManager();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      handleGetAllSchedules(req, resp, session);
    }
  }

  private void handleAJAXAction(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    String ajaxName = getParam(req, "ajax");

    try {
      if (ajaxName.equals("expireTrigger")) {
        ajaxExpireTrigger(req, ret, session.getUser());
      }
    } catch (Exception e) {
      ret.put("error", e.getMessage());
    }

    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void handleGetAllSchedules(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {

    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/triggerspage.vm");

    List<Trigger> triggers = triggerManager.getTriggers();
    page.add("triggers", triggers);
    page.render();
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    }
  }

  private void ajaxExpireTrigger(HttpServletRequest req,
      Map<String, Object> ret, User user) throws ServletException,
      TriggerManagerException {
    int triggerId = getIntParam(req, "triggerId");
    Trigger t = triggerManager.getTrigger(triggerId);
    if (t == null) {
      ret.put("message", "Trigger with ID " + triggerId + " does not exist");
      ret.put("status", "error");
      return;
    }

    triggerManager.expireTrigger(triggerId);
    logger.info("User '" + user.getUserId() + " has removed trigger "
        + t.getDescription());

    ret.put("status", "success");
    ret.put("message", "trigger " + triggerId + " removed from Schedules.");
    return;
  }

}
