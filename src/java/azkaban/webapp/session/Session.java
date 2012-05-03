package azkaban.webapp.session;

import azkaban.user.User;

/**
 * Container for the session, mapping session id to user in map
 */
public class Session {
	private final User user;
	private final String sessionId;
	
	/**
	 * Constructor for the session
	 * 
	 * @param sessionId
	 * @param user
	 */
	public Session(String sessionId, User user) {
		this.user = user;
		this.sessionId = sessionId;
	}

	/**
	 * Returns the User object
	 * @return
	 */
	public User getUser() {
		return user;
	}

	/**
	 * Returns the sessionId
	 * @return
	 */
	public String getSessionId() {
		return sessionId;
	}
}
