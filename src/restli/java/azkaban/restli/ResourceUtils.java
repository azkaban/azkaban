package azkaban.restli;

import azkaban.project.Project;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.session.Session;

public class ResourceUtils {
	
	public static boolean hasPermission(Project project, User user, Permission.Type type) {
		UserManager userManager = AzkabanWebServer.getInstance().getUserManager();
		if (project.hasPermission(user, type)) {
			return true;
		}
		
		for (String roleName: user.getRoles()) {
			Role role = userManager.getRole(roleName);
			if (role.getPermission().isPermissionSet(type) || 
				role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
				return true;
			}
		}
		
		return false;
	}
	
	public static User getUserFromSessionId(String sessionId, String ip) throws UserManagerException {
		Session session = AzkabanWebServer.getInstance().getSessionCache().getSession(sessionId);
		if (session == null) {
			throw new UserManagerException("Invalid session. Login required");
		}
		else if (!session.getIp().equals(ip)) {
			throw new UserManagerException("Invalid session. Session expired.");
		}
		
		return session.getUser();
	}
}
