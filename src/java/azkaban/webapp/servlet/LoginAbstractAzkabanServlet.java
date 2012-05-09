package azkaban.webapp.servlet;

import java.io.IOException;
import java.io.Writer;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.webapp.session.Session;

/**
 * Abstract Servlet that handles auto login when the session hasn't been
 * verified.
 */
public abstract class LoginAbstractAzkabanServlet extends
        AbstractAzkabanServlet {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger
            .getLogger(LoginAbstractAzkabanServlet.class.getName());
    private static final String SESSION_ID_NAME = "azkaban.session.id";

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        // Set session id
        Session session = getSessionFromRequest(req);
        if (hasParam(req, "logout")) {
            resp.sendRedirect(req.getContextPath());
            if (session != null) {
                getApplication().getSessionCache().removeSession(
                        session.getSessionId());
            }
            return;
        }

        if (session != null) {
            logger.info("Found session " + session.getUser());
            handleGet(req, resp, session);
        } else {
            handleLogin(req, resp);
        }
    }

    private Session getSessionFromRequest(HttpServletRequest req) {
        Cookie cookie = getCookieByName(req, SESSION_ID_NAME);
        String sessionId = null;

        if (cookie != null) {
            sessionId = cookie.getValue();
            logger.info("Session id " + sessionId);
        }
        if (sessionId == null) {
            return null;
        } else {
            return getApplication().getSessionCache().getSession(sessionId);
        }
    }

    private void handleLogin(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        handleLogin(req, resp, null);
    }

    private void handleLogin(HttpServletRequest req, HttpServletResponse resp,
            String errorMsg) throws ServletException, IOException {

        Page page = newPage(req, resp, "azkaban/webapp/servlet/velocity/login.vm");
        if (errorMsg != null) {
            page.add("errorMsg", errorMsg);
        }

        page.render();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (hasParam(req, "action")) {
            String action = getParam(req, "action");
            if (action.equals("login")) {
                if (hasParam(req, "username") && hasParam(req, "password")) {
                    String username = getParam(req, "username");
                    String password = getParam(req, "password");

                    UserManager manager = getApplication().getUserManager();

                    User user = null;
					try {
						user = manager.getUser(username, password);
					} catch (UserManagerException e) {
						handleLogin(req, resp, e.getMessage());
						return;
					}

                    String randomUID = UUID.randomUUID().toString();
                    Session session = new Session(randomUID, user);
                    resp.addCookie(new Cookie(SESSION_ID_NAME, randomUID));
                    getApplication().getSessionCache().addSession(session);
                    handleGet(req, resp, session);
                } else {
                	if (isAjaxCall(req)) {
                		String response = createJsonResponse("error", "Incorrect Login.", "login", null);
                		writeResponse(resp, response);
                	}
                	else {
                		handleLogin(req, resp, "Enter username and password");
                	}
                }
            } else {
                Session session = getSessionFromRequest(req);
                if (session == null) {
                	if (isAjaxCall(req)) {
                		String response = createJsonResponse("error", "Invalid Session. Need to re-login", "login", null);
                		writeResponse(resp, response);
                	}
                	else {
                		handleLogin(req, resp, "Enter username and password");
                	}
                } else {
                	handlePost(req, resp, session);
                }
            }
        } else {
            Session session = getSessionFromRequest(req);
            if (session == null) {
            	if (isAjaxCall(req)) {
            		String response = createJsonResponse("error", "Invalid Session. Need to re-login", "login", null);
            		writeResponse(resp, response);
            	}
            	else {
            		handleLogin(req, resp, "Enter username and password");
            	}
            } else {
            	handlePost(req, resp, session);
            }
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
     * The get request is handed off to the implementor after the user is logged in.
     * 
     * @param req
     * @param resp
     * @param session
     * @throws ServletException
     * @throws IOException
     */
    protected abstract void handleGet(HttpServletRequest req,
            HttpServletResponse resp, Session session) throws ServletException,
            IOException;

    /**
     * The post request is handed off to the implementor after the user is logged in.
     * 
     * @param req
     * @param resp
     * @param session
     * @throws ServletException
     * @throws IOException
     */
    protected abstract void handlePost(HttpServletRequest req,
            HttpServletResponse resp, Session session) throws ServletException,
            IOException;
}