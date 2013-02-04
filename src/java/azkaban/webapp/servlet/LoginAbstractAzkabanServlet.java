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
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;

import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.webapp.session.Session;

/**
 * Abstract Servlet that handles auto login when the session hasn't been
 * verified.
 */
public abstract class LoginAbstractAzkabanServlet extends AbstractAzkabanServlet {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger.getLogger(LoginAbstractAzkabanServlet.class.getName());
	private static final String SESSION_ID_NAME = "azkaban.browser.session.id";
	private static final int DEFAULT_UPLOAD_DISK_SPOOL_SIZE = 20 * 1024 * 1024;
	
	private MultipartParser multipartParser;
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		
		multipartParser = new MultipartParser(DEFAULT_UPLOAD_DISK_SPOOL_SIZE);
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// Set session id
		Session session = getSessionFromRequest(req);
		if (hasParam(req, "logout")) {
			resp.sendRedirect(req.getContextPath());
			if (session != null) {
				getApplication().getSessionCache().removeSession(session.getSessionId());
			}
			return;
		}

		if (session != null) {
			logger.info("Found session " + session.getUser());
			handleGet(req, resp, session);
		} else {
			if (hasParam(req, "ajax")) {
				HashMap<String, String> retVal = new HashMap<String, String>();
				retVal.put("error", "session");
				this.writeJSON(resp, retVal);
			}
			else {
				handleLogin(req, resp);
			}
		}
	}
	
	private Session getSessionFromRequest(HttpServletRequest req) throws ServletException {
		String remoteIp = req.getRemoteAddr();
		Cookie cookie = getCookieByName(req, SESSION_ID_NAME);
		String sessionId = null;

		if (cookie != null) {
			sessionId = cookie.getValue();
			logger.info("Session id " + sessionId);
		}
		
		if (sessionId == null && hasParam(req, "session.id")) {
			sessionId = getParam(req, "session.id");
		}
		return getSessionFromSessionId(sessionId, remoteIp);
	}
	
	private Session getSessionFromSessionId(String sessionId, String remoteIp) {
		if (sessionId == null) {
			return null;
		}
		
		Session session = getApplication().getSessionCache().getSession(sessionId);
		// Check if the IP's are equal. If not, we invalidate the sesson.
		if (session == null || !remoteIp.equals(session.getIp())) {
			return null;
		}
		
		return session;
	}

	private void handleLogin(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		handleLogin(req, resp, null);
	}

	private void handleLogin(HttpServletRequest req, HttpServletResponse resp, String errorMsg) throws ServletException, IOException {
		Page page = newPage(req, resp,"azkaban/webapp/servlet/velocity/login.vm");
		if (errorMsg != null) {
			page.add("errorMsg", errorMsg);
		}

		page.render();
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		Session session = getSessionFromRequest(req);
		
		// Handle Multipart differently from other post messages
		if(ServletFileUpload.isMultipartContent(req)) {
			Map<String, Object> params = multipartParser.parseMultipart(req);
			if (session == null) {
				// See if the session id is properly set.
				if (params.containsKey("session.id")) {
					String sessionId = (String)params.get("session.id");
					String ip = req.getRemoteAddr();
					
					session = getSessionFromSessionId(sessionId, ip);
					if (session != null) {
						handleMultiformPost(req, resp, params, session);
						return;
					}
				}

				// if there's no valid session, see if it's a one time session.
				if (!params.containsKey("username") || !params.containsKey("password")) {
					writeResponse(resp, "Login error. Need username and password");
					return;
				}
				
				String username = (String)params.get("username");
				String password = (String)params.get("password");
				String ip = req.getRemoteAddr();
				
				try {
					session = createSession(username, password, ip);
				} catch (UserManagerException e) {
					writeResponse(resp, "Login error: " + e.getMessage());
					return;
				}
			}

			handleMultiformPost(req, resp, params, session);
		}
		else if (hasParam(req, "action") && getParam(req, "action").equals("login")) {
			HashMap<String,Object> obj = new HashMap<String,Object>();
			handleAjaxLoginAction(req, resp, obj);
			this.writeJSON(resp, obj);
		}
		else if (session == null) {
			if (hasParam(req, "username") && hasParam(req, "password")) {
				// If it's a post command with curl, we create a temporary session
				try {
					session = createSession(req);
				} catch (UserManagerException e) {
					writeResponse(resp, "Login error: " + e.getMessage());
				}
				
				handlePost(req, resp, session);
			}
			else {
				// There are no valid sessions and temporary logins, no we either pass back a message or redirect.
				if (isAjaxCall(req)) {
					String response = createJsonResponse("error", "Invalid Session. Need to re-login", "login", null);
					writeResponse(resp, response);
				} 
				else {
					handleLogin(req, resp, "Enter username and password");
				}
			}
		}
		else {
			handlePost(req, resp, session);
		}
	}

	private Session createSession(HttpServletRequest req) throws UserManagerException, ServletException {
		String username = getParam(req, "username");
		String password = getParam(req, "password");
		String ip = req.getRemoteAddr();
		
		return createSession(username, password, ip);
	}
	
	private Session createSession(String username, String password, String ip) throws UserManagerException, ServletException {
		UserManager manager = getApplication().getUserManager();
		User user = manager.getUser(username, password);

		String randomUID = UUID.randomUUID().toString();
		Session session = new Session(randomUID, user, ip);
		
		return session;
	}
	
	protected void handleAjaxLoginAction(HttpServletRequest req, HttpServletResponse resp, Map<String, Object> ret) throws ServletException {
		if (hasParam(req, "username") && hasParam(req, "password")) {
			Session session = null;
			try {
				session = createSession(req);
			} catch (UserManagerException e) {
				ret.put("error", "Incorrect Login. " + e.getMessage());
				return;
			}
			
			Cookie cookie = new Cookie(SESSION_ID_NAME, session.getSessionId());
			cookie.setPath("/");
			resp.addCookie(cookie);
			getApplication().getSessionCache().addSession(session);
			ret.put("status", "success");
			ret.put("session.id", session.getSessionId());
		} 
		else {
			ret.put("error", "Incorrect Login.");
		}
	}
	
	protected void writeResponse(HttpServletResponse resp, String response) throws IOException {
		Writer writer = resp.getWriter();
		writer.append(response);
		writer.flush();
	}

	protected boolean isAjaxCall(HttpServletRequest req) throws ServletException {
		String value = req.getHeader("X-Requested-With");
		if (value != null) {
			logger.info("has X-Requested-With " + value);
			return value.equals("XMLHttpRequest");
		}

		return false;
	}

	/**
	 * The get request is handed off to the implementor after the user is logged
	 * in.
	 * 
	 * @param req
	 * @param resp
	 * @param session
	 * @throws ServletException
	 * @throws IOException
	 */
	protected abstract void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException;

	/**
	 * The post request is handed off to the implementor after the user is
	 * logged in.
	 * 
	 * @param req
	 * @param resp
	 * @param session
	 * @throws ServletException
	 * @throws IOException
	 */
	protected abstract void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException;
	
	/**
	 * The post request is handed off to the implementor after the user is
	 * logged in.
	 * 
	 * @param req
	 * @param resp
	 * @param session
	 * @throws ServletException
	 * @throws IOException
	 */
	protected void handleMultiformPost(HttpServletRequest req, HttpServletResponse resp, Map<String,Object> multipart, Session session) throws ServletException, IOException {
	}
}