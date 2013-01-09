package azkaban.webapp;

import org.apache.velocity.app.VelocityEngine;

import azkaban.user.UserManager;
import azkaban.utils.Props;
import azkaban.webapp.session.SessionCache;

public interface AzkabanServer {
	public Props getServerProps();
	
	public VelocityEngine getVelocityEngine();

	public SessionCache getSessionCache();
	
	public UserManager getUserManager();
}