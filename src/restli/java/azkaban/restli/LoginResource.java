package azkaban.restli;

import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import azkaban.restli.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.webapp.AzkabanServer;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.session.Session;

import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.RestLiActions;
import com.linkedin.restli.server.resources.ResourceContextHolder;

@RestLiActions(name = "login", namespace = "azkaban.restli")
public class LoginResource extends ResourceContextHolder {
	private AzkabanServer azkaban;

	public AzkabanWebServer getAzkaban() {
		return AzkabanWebServer.getInstance();
	}

	public void setAzkaban(AzkabanServer azkaban) {
		this.azkaban = azkaban;
	}

	@Action(name = "login")
	public String login(
			@ActionParam("username") String username,
			@ActionParam("password") String password)
			throws UserManagerException, ServletException {
//		String ip = requestContext.getRemoteAddr();
//
//		Session session = createSession(username, password, ip);
//		return session.getSessionId();
		return "123";
	}

	@Action(name = "getUserFromSessionId")
	public User getUserFromSessionId(@ActionParam("sessionId") String sessionId) {
		// String ip = req.getRemoteAddr();
		String ip = this.getContext().getParameter("ip");
		Session session = getSessionFromSessionId(sessionId, ip);
		azkaban.user.User azUser = session.getUser();

		// Fill out the restli object with properties from the Azkaban user
		User user = new User();
		user.setUserId(azUser.getUserId());
		user.setEmail(azUser.getEmail());
		return user;
	}

	private Session createSession(String username, String password, String ip)
			throws UserManagerException, ServletException {
		UserManager manager = getAzkaban().getUserManager();
		azkaban.user.User user = manager.getUser(username, password);

		String randomUID = UUID.randomUUID().toString();
		Session session = new Session(randomUID, user, ip);

		return session;
	}

	private Session getSessionFromSessionId(String sessionId, String remoteIp) {
		if (sessionId == null) {
			return null;
		}

		Session session = getAzkaban().getSessionCache().getSession(sessionId);
		// Check if the IP's are equal. If not, we invalidate the sesson.
		if (session == null || !remoteIp.equals(session.getIp())) {
			return null;
		}

		return session;
	}
}