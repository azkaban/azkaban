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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.utils.WebUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanServer;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.session.Session;

/**
 * Base Servlet for pages
 */
public abstract class AbstractAzkabanServlet extends HttpServlet {
	private static final DateTimeFormatter ZONE_FORMATTER = DateTimeFormat.forPattern("z");
	private static final String AZKABAN_SUCCESS_MESSAGE = "azkaban.success.message";
	private static final String AZKABAN_FAILURE_MESSAGE = "azkaban.failure.message";

	private static final long serialVersionUID = -1;
	public static final String DEFAULT_LOG_URL_PREFIX = "predefined_log_url_prefix";
	public static final String LOG_URL_PREFIX = "log_url_prefix";
	public static final String HTML_TYPE = "text/html";
	public static final String XML_MIME_TYPE = "application/xhtml+xml";
	public static final String JSON_MIME_TYPE = "application/json";

	private static final WebUtils utils = new WebUtils();
	
	private AzkabanServer application;
	private String name;
	private String label;
	private String color;

	/**
	 * To retrieve the application for the servlet
	 * 
	 * @return
	 */
	public AzkabanServer getApplication() {
		return application;
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		application = (AzkabanServer) config.getServletContext().getAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY);

		if (application == null) {
			throw new IllegalStateException(
					"No batch application is defined in the servlet context!");
		}

		Props props = application.getServerProps();
		name = props.getString("azkaban.name", "");
		label = props.getString("azkaban.label", "");
		color = props.getString("azkaban.color", "#FF0000");
	}

	/**
	 * Checks for the existance of the parameter in the request
	 * 
	 * @param request
	 * @param param
	 * @return
	 */
	public boolean hasParam(HttpServletRequest request, String param) {
		return request.getParameter(param) != null;
	}

	/**
	 * Retrieves the param from the http servlet request. Will throw an
	 * exception if not found
	 * 
	 * @param request
	 * @param name
	 * @return
	 * @throws ServletException
	 */
	public String getParam(HttpServletRequest request, String name) throws ServletException {
		String p = request.getParameter(name);
		if (p == null) {
			throw new ServletException("Missing required parameter '" + name + "'.");
		}
		else {
			return p;
		}
	}

	/**
	 * Retrieves the param from the http servlet request.
	 * 
	 * @param request
	 * @param name
	 * @param default
	 * 
	 * @return
	 */
	public String getParam(HttpServletRequest request, String name, String defaultVal){
		String p = request.getParameter(name);
		if (p == null) {
			return defaultVal;
		}
		return p;
	}

	
	/**
	 * Returns the param and parses it into an int. Will throw an exception if
	 * not found, or a parse error if the type is incorrect.
	 * 
	 * @param request
	 * @param name
	 * @return
	 * @throws ServletException
	 */
	public int getIntParam(HttpServletRequest request, String name) throws ServletException {
		String p = getParam(request, name);
		return Integer.parseInt(p);
	}
	
	public int getIntParam(HttpServletRequest request, String name, int defaultVal) {
		if (hasParam(request, name)) {
			try {
				return getIntParam(request, name);
			} catch (Exception e) {
				return defaultVal;
			}
		}
		
		return defaultVal;
	}

	public long getLongParam(HttpServletRequest request, String name) throws ServletException {
		String p = getParam(request, name);
		return Long.valueOf(p);
	}
	
	public long getLongParam(HttpServletRequest request, String name, long defaultVal) {
		if (hasParam(request, name)) {
			try {
				return getLongParam(request, name);
			} catch (Exception e) {
				return defaultVal;
			}
		}
		
		return defaultVal;
	}

	
	public Map<String, String> getParamGroup(HttpServletRequest request, String groupName)  throws ServletException {
		@SuppressWarnings("unchecked")
		Enumeration<Object> enumerate = (Enumeration<Object>)request.getParameterNames();
		String matchString = groupName + "[";

		HashMap<String, String> groupParam = new HashMap<String, String>();
		while( enumerate.hasMoreElements() ) {
			String str = (String)enumerate.nextElement();
			if (str.startsWith(matchString)) {
				groupParam.put(str.substring(matchString.length(), str.length() - 1), request.getParameter(str));
			}
			
		}
		return groupParam;
	}
	
	/**
	 * Returns the session value of the request.
	 * 
	 * @param request
	 * @param key
	 * @param value
	 */
	protected void setSessionValue(HttpServletRequest request, String key, Object value) {
		request.getSession(true).setAttribute(key, value);
	}

	/**
	 * Adds a session value to the request
	 * 
	 * @param request
	 * @param key
	 * @param value
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void addSessionValue(HttpServletRequest request, String key, Object value) {
		List l = (List) request.getSession(true).getAttribute(key);
		if (l == null)
			l = new ArrayList();
		l.add(value);
		request.getSession(true).setAttribute(key, l);
	}

	/**
	 * Sets an error message in azkaban.failure.message in the cookie. This will
	 * be used by the web client javascript to somehow display the message
	 * 
	 * @param response
	 * @param errorMsg
	 */
	protected void setErrorMessageInCookie(HttpServletResponse response, String errorMsg) {
		Cookie cookie = new Cookie(AZKABAN_FAILURE_MESSAGE, errorMsg);
		cookie.setPath("/");
		response.addCookie(cookie);
	}

	/**
	 * Sets a message in azkaban.success.message in the cookie. This will be
	 * used by the web client javascript to somehow display the message
	 * 
	 * @param response
	 * @param errorMsg
	 */
	protected void setSuccessMessageInCookie(HttpServletResponse response, String message) {
		Cookie cookie = new Cookie(AZKABAN_SUCCESS_MESSAGE, message);
		cookie.setPath("/");
		response.addCookie(cookie);
	}

	/**
	 * Retrieves a success message from a cookie. azkaban.success.message
	 * 
	 * @param request
	 * @return
	 */
	protected String getSuccessMessageFromCookie(HttpServletRequest request) {
		Cookie cookie = getCookieByName(request, AZKABAN_SUCCESS_MESSAGE);

		if (cookie == null) {
			return null;
		}
		return cookie.getValue();
	}

	/**
	 * Retrieves a success message from a cookie. azkaban.failure.message
	 * 
	 * @param request
	 * @return
	 */
	protected String getErrorMessageFromCookie(HttpServletRequest request) {
		Cookie cookie = getCookieByName(request, AZKABAN_FAILURE_MESSAGE);
		if (cookie == null) {
			return null;
		}

		return cookie.getValue();
	}

	/**
	 * Retrieves a cookie by name. Potential issue in performance if a lot of
	 * cookie variables are used.
	 * 
	 * @param request
	 * @return
	 */
	protected Cookie getCookieByName(HttpServletRequest request, String name) {
		Cookie[] cookies = request.getCookies();
		if (cookies != null) {
			for (Cookie cookie : cookies) {
				//if (name.equals(cookie.getName()) && cookie.getPath()!=null && cookie.getPath().equals("/")) {
				if (name.equals(cookie.getName())) {
					return cookie;
				}
			}
		}
		
		return null;
	}

	/**
	 * Creates a new velocity page to use. With session.
	 * 
	 * @param req
	 * @param resp
	 * @param template
	 * @return
	 */
	protected Page newPage(HttpServletRequest req, HttpServletResponse resp, Session session, String template) {
		Page page = new Page(req, resp, application.getVelocityEngine(), template);
		page.add("azkaban_name", name);
		page.add("azkaban_label", label);
		page.add("azkaban_color", color);
		page.add("utils", utils);
		page.add("timezone", ZONE_FORMATTER.print(System.currentTimeMillis()));
		page.add("currentTime", (new DateTime()).getMillis());
		if (session != null && session.getUser() != null) {
			page.add("user_id", session.getUser().getUserId());
		}
		page.add("context", req.getContextPath());

		String errorMsg = getErrorMessageFromCookie(req);
		page.add("error_message", errorMsg == null || errorMsg.isEmpty() ? "null" : errorMsg);
		setErrorMessageInCookie(resp, null);

		String successMsg = getSuccessMessageFromCookie(req);
		page.add("success_message", successMsg == null || successMsg.isEmpty() ? "null" : successMsg);
		setSuccessMessageInCookie(resp, null);

		return page;
	}

	/**
	 * Creates a new velocity page to use.
	 * 
	 * @param req
	 * @param resp
	 * @param template
	 * @return
	 */
	protected Page newPage(HttpServletRequest req, HttpServletResponse resp, String template) {
		Page page = new Page(req, resp, application.getVelocityEngine(), template);
		page.add("azkaban_name", name);
		page.add("azkaban_label", label);
		page.add("azkaban_color", color);
		page.add("timezone", ZONE_FORMATTER.print(System.currentTimeMillis()));
		page.add("currentTime", (new DateTime()).getMillis());
		page.add("context", req.getContextPath());
		return page;
	}

	/**
	 * Writes json out to the stream.
	 * 
	 * @param resp
	 * @param obj
	 * @throws IOException
	 */
	protected void writeJSON(HttpServletResponse resp, Object obj) throws IOException {
		resp.setContentType(JSON_MIME_TYPE);
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(resp.getOutputStream(), obj);
	}

	/**
	 * Retrieve the Azkaban application
	 * 
	 * @param config
	 * @return
	 */
	public static AzkabanWebServer getApp(ServletConfig config) {
		AzkabanWebServer app = (AzkabanWebServer) config.getServletContext().getAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY);

		if (app == null) {
			throw new IllegalStateException("No batch application is defined in the servlet context!");
		} 
		else {
			return app;
		}
	}

	public static String createJsonResponse(String status, String message, String action, Map<String, Object> params) {
		HashMap<String, Object> response = new HashMap<String, Object>();
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
}
