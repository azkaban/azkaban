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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.server.AzkabanServer;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.WebUtils;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.plugin.PluginRegistry;
import azkaban.webapp.plugin.TriggerPlugin;
import azkaban.webapp.plugin.ViewerPlugin;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.joda.time.DateTime;

/**
 * Base Servlet for pages
 */
public abstract class AbstractAzkabanServlet extends HttpServlet {

  public static final String JSON_MIME_TYPE = "application/json";
  public static final String jarVersion = AbstractAzkabanServlet.class.getPackage()
      .getImplementationVersion();
  protected static final WebUtils utils = new WebUtils();
  private static final String AZKABAN_SUCCESS_MESSAGE =
      "azkaban.success.message";
  private static final String AZKABAN_WARN_MESSAGE =
      "azkaban.warn.message";
  private static final String AZKABAN_FAILURE_MESSAGE =
      "azkaban.failure.message";
  private static final long serialVersionUID = -1;

  protected String passwordPlaceholder;
  private AzkabanServer application;
  private String name;
  private String label;
  private String color;

  private List<ViewerPlugin> viewerPlugins;
  private List<TriggerPlugin> triggerPlugins;

  private int displayExecutionPageSize;

  public static String createJsonResponse(final String status, final String message,
      final String action, final Map<String, Object> params) {
    final HashMap<String, Object> response = new HashMap<>();
    response.put("status", status);
    if (message != null) {
      response.put("message", message);
    }
    if (action != null) {
      response.put("action", action);
    }
    if (params != null) {
      response.putAll(params);
    }

    return JSONUtils.toJSON(response);
  }

  /**
   * To retrieve the application for the servlet
   */
  public AzkabanServer getApplication() {
    return this.application;
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    this.application = SERVICE_PROVIDER.getInstance(AzkabanWebServer.class);

    if (this.application == null) {
      throw new IllegalStateException(
          "No batch application is defined in the servlet context!");
    }

    final Props props = this.application.getServerProps();
    this.name = props.getString("azkaban.name", "");
    this.label = props.getString("azkaban.label", "");
    this.color = props.getString("azkaban.color", "#FF0000");
    this.passwordPlaceholder = props.getString("azkaban.password.placeholder", "Password");
    this.executionsPageSize = props.getInt("azkaban.display.execution_page_size", 16);

    if (this.application instanceof AzkabanWebServer) {
      final AzkabanWebServer server = (AzkabanWebServer) this.application;
      this.viewerPlugins = PluginRegistry.getRegistry().getViewerPlugins();
      this.triggerPlugins =
          new ArrayList<>(server.getTriggerPlugins().values());
    }
  }

  /**
   * Checks for the existance of the parameter in the request
   */
  public boolean hasParam(final HttpServletRequest request, final String param) {
    return HttpRequestUtils.hasParam(request, param);
  }

  /**
   * Retrieves the param from the http servlet request. Will throw an exception if not found
   */
  public String getParam(final HttpServletRequest request, final String name)
      throws ServletException {
    return HttpRequestUtils.getParam(request, name);
  }

  /**
   * Retrieves the param from the http servlet request.
   */
  public String getParam(final HttpServletRequest request, final String name,
      final String defaultVal) {
    return HttpRequestUtils.getParam(request, name, defaultVal);
  }

  /**
   * Returns the param and parses it into an int. Will throw an exception if not found, or a parse
   * error if the type is incorrect.
   */
  public int getIntParam(final HttpServletRequest request, final String name)
      throws ServletException {
    return HttpRequestUtils.getIntParam(request, name);
  }

  public int getIntParam(final HttpServletRequest request, final String name,
      final int defaultVal) {
    return HttpRequestUtils.getIntParam(request, name, defaultVal);
  }

  public long getLongParam(final HttpServletRequest request, final String name)
      throws ServletException {
    return HttpRequestUtils.getLongParam(request, name);
  }

  public long getLongParam(final HttpServletRequest request, final String name,
      final long defaultVal) {
    return HttpRequestUtils.getLongParam(request, name, defaultVal);
  }

  public Map<String, String> getParamGroup(final HttpServletRequest request,
      final String groupName) throws ServletException {
    return HttpRequestUtils.getParamGroup(request, groupName);
  }

  /**
   * Returns the session value of the request.
   */
  protected void setSessionValue(final HttpServletRequest request, final String key,
      final Object value) {
    request.getSession(true).setAttribute(key, value);
  }

  /**
   * Adds a session value to the request
   */
  protected void addSessionValue(final HttpServletRequest request, final String key,
      final Object value) {
    List l = (List) request.getSession(true).getAttribute(key);
    if (l == null) {
      l = new ArrayList();
    }
    l.add(value);
    request.getSession(true).setAttribute(key, l);
  }

  /**
   * Sets an error message in azkaban.failure.message in the cookie. This will be used by the web
   * client javascript to somehow display the message
   */
  protected void setErrorMessageInCookie(final HttpServletResponse response,
      final String errorMsg) {
    final Cookie cookie = new Cookie(AZKABAN_FAILURE_MESSAGE, errorMsg);
    cookie.setPath("/");
    response.addCookie(cookie);
  }

  /**
   * Sets a warning message in azkaban.warn.message in the cookie. This will be used by the web
   * client javascript to somehow display the message
   */
  protected void setWarnMessageInCookie(final HttpServletResponse response,
      final String errorMsg) {
    final Cookie cookie = new Cookie(AZKABAN_WARN_MESSAGE, errorMsg);
    cookie.setPath("/");
    response.addCookie(cookie);
  }

  /**
   * Sets a message in azkaban.success.message in the cookie. This will be used by the web client
   * javascript to somehow display the message
   */
  protected void setSuccessMessageInCookie(final HttpServletResponse response,
      final String message) {
    final Cookie cookie = new Cookie(AZKABAN_SUCCESS_MESSAGE, message);
    cookie.setPath("/");
    response.addCookie(cookie);
  }

  /**
   * Retrieves a success message from a cookie. azkaban.success.message
   */
  protected String getSuccessMessageFromCookie(final HttpServletRequest request) {
    final Cookie cookie = getCookieByName(request, AZKABAN_SUCCESS_MESSAGE);

    if (cookie == null) {
      return null;
    }
    return cookie.getValue();
  }

  /**
   * Retrieves a warn message from a cookie. azkaban.warn.message
   */
  protected String getWarnMessageFromCookie(final HttpServletRequest request) {
    final Cookie cookie = getCookieByName(request, AZKABAN_WARN_MESSAGE);

    if (cookie == null) {
      return null;
    }
    return cookie.getValue();
  }

  /**
   * Retrieves a success message from a cookie. azkaban.failure.message
   */
  protected String getErrorMessageFromCookie(final HttpServletRequest request) {
    final Cookie cookie = getCookieByName(request, AZKABAN_FAILURE_MESSAGE);
    if (cookie == null) {
      return null;
    }

    return cookie.getValue();
  }

  /**
   * Retrieves a cookie by name. Potential issue in performance if a lot of cookie variables are
   * used.
   */
  protected Cookie getCookieByName(final HttpServletRequest request, final String name) {
    final Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (final Cookie cookie : cookies) {
        if (name.equals(cookie.getName())) {
          return cookie;
        }
      }
    }

    return null;
  }

  /**
   * Creates a new velocity page to use. With session.
   */
  protected Page newPage(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session, final String template) {
    final Page page = new Page(req, resp, getApplication().getVelocityEngine(), template);
    page.add("version", jarVersion);
    page.add("azkaban_name", this.name);
    page.add("azkaban_label", this.label);
    page.add("azkaban_color", this.color);
    page.add("note_type", NoteServlet.type);
    page.add("note_message", NoteServlet.message);
    page.add("note_url", NoteServlet.url);
    page.add("utils", utils);
    page.add("timezone", TimeZone.getDefault().getID());
    page.add("currentTime", (new DateTime()).getMillis());
    if (session != null && session.getUser() != null) {
      page.add("user_id", session.getUser().getUserId());
    }
    page.add("context", req.getContextPath());

    final String errorMsg = getErrorMessageFromCookie(req);
    page.add("error_message", errorMsg == null || errorMsg.isEmpty() ? "null"
        : errorMsg);
    setErrorMessageInCookie(resp, null);

    final String warnMsg = getWarnMessageFromCookie(req);
    page.add("warn_message", warnMsg == null || warnMsg.isEmpty() ? "null"
        : warnMsg);
    setWarnMessageInCookie(resp, null);

    final String successMsg = getSuccessMessageFromCookie(req);
    page.add("success_message",
        successMsg == null || successMsg.isEmpty() ? "null" : successMsg);
    setSuccessMessageInCookie(resp, null);

    // @TODO, allow more than one type of viewer. For time sake, I only install
    // the first one
    if (this.viewerPlugins != null && !this.viewerPlugins.isEmpty()) {
      page.add("viewers", this.viewerPlugins);
    }

    if (this.triggerPlugins != null && !this.triggerPlugins.isEmpty()) {
      page.add("triggerPlugins", this.triggerPlugins);
    }

    page.add("displayExecutionPageSize", displayExecutionPageSize);

    return page;
  }

  /**
   * Creates a new velocity page to use.
   */
  protected Page newPage(final HttpServletRequest req, final HttpServletResponse resp,
      final String template) {
    final Page page = new Page(req, resp, getApplication().getVelocityEngine(), template);
    page.add("version", jarVersion);
    page.add("azkaban_name", this.name);
    page.add("azkaban_label", this.label);
    page.add("azkaban_color", this.color);
    page.add("note_type", NoteServlet.type);
    page.add("note_message", NoteServlet.message);
    page.add("note_url", NoteServlet.url);
    page.add("timezone", TimeZone.getDefault().getID());
    page.add("currentTime", (new DateTime()).getMillis());
    page.add("context", req.getContextPath());

    // @TODO, allow more than one type of viewer. For time sake, I only install
    // the first one
    if (this.viewerPlugins != null && !this.viewerPlugins.isEmpty()) {
      page.add("viewers", this.viewerPlugins);
      final ViewerPlugin plugin = this.viewerPlugins.get(0);
      page.add("viewerName", plugin.getPluginName());
      page.add("viewerPath", plugin.getPluginPath());
    }

    if (this.triggerPlugins != null && !this.triggerPlugins.isEmpty()) {
      page.add("triggers", this.triggerPlugins);
    }

    page.add("displayExecutionPageSize", displayExecutionPageSize);

    return page;
  }

  /**
   * Writes json out to the stream.
   */
  protected void writeJSON(final HttpServletResponse resp, final Object obj)
      throws IOException {
    writeJSON(resp, obj, false);
  }

  protected void writeJSON(final HttpServletResponse resp, final Object obj, final boolean pretty)
      throws IOException {
    resp.setContentType(JSON_MIME_TYPE);
    JSONUtils.toJSON(obj, resp.getOutputStream(), true);
  }

  public int getDisplayExecutionPageSize() {
    return displayExecutionPageSize;
  }
}