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

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

public class NoteServlet extends LoginAbstractAzkabanServlet {

  private static final String API_ADD_NOTE = "addNote";
  private static final String API_REMOVE_NOTE = "removeNote";

  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger.getLogger(NoteServlet.class);

  public static String type = null;
  public static String message = null;
  public static String url = null;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (isAdmin(session.getUser())) {
      handleNotePageLoad(req, resp, session);
    } else {
      warningNonAdminUsers(resp, "The requested user doesn't have admin permission");
    }
  }

  private void warningNonAdminUsers(final HttpServletResponse resp, final String message)
      throws IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    ret.put("error", message);
    this.writeJSON(resp, ret);
  }

  private void handleNotePageLoad(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {

    final Page page = newPage(req, resp, session,
        "azkaban/webapp/servlet/velocity/notepage.vm");

    page.add("note_type", type);
    page.add("note_message", message);
    page.add("note_url", url);
    page.render();
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (isAdmin(session.getUser()) && hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      warningNonAdminUsers(resp, "The requested user doesn't have admin permission, "
          + "Or the HTTP request doesn't include ajax.");
    }
  }

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");
    try {
      if (API_ADD_NOTE.equals(ajaxName)) {
        ajaxAddNotes(req, ret);
      } else if (API_REMOVE_NOTE.equals(ajaxName)) {
        ajaxRemoveNotes(ret);
      } else {
        ret.put("error", "Can not find the ajax operation");
      }
    } catch (final Exception e) {
      ret.put("error", e.getMessage());
    }
    this.writeJSON(resp, ret);
  }

  private void ajaxAddNotes(final HttpServletRequest req,
      final Map<String, Object> ret) throws ServletException {
    type = getParam(req, "type");
    message = getParam(req, "message");
    url = getParam(req, "url");
    logger.info("receive note message. Type: " + type + " message: " + message + " url: " + url);
    ret.put("status", "success");
  }

  private void ajaxRemoveNotes(final Map<String, Object> ret) throws ServletException {
    type = null;
    message = null;
    url = null;
    logger.info("removing note from memory.");
    ret.put("status", "success");
  }

  private boolean isAdmin(final User user) {
    return HttpRequestUtils.hasPermission(getApplication().getUserManager(), user, Type.ADMIN);
  }
}
