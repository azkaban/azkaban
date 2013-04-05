package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.session.Session;

/**
 * Limited set of jmx calls for when you cannot attach to the jvm
 */
public class JMXHttpServlet extends LoginAbstractAzkabanServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger.getLogger(JMXHttpServlet.class.getName());

	private UserManager userManager;
	private AzkabanWebServer server;
	
	private static final String GET_MBEANS = "getMBeans";
	private static final String GET_MBEAN_INFO = "getMBeanInfo";
	private static final String GET_MBEAN_ATTRIBUTE = "getAttribute";
	private static final String ATTRIBUTE = "attribute";
	private static final String MBEAN = "mBean";
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		
		server = (AzkabanWebServer)getApplication();
		userManager = server.getUserManager();
	}
	
	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")){
			HashMap<String,Object> ret = new HashMap<String,Object>();

			if(!hasAdminRole(session.getUser())) {
				ret.put("error", "User " + session.getUser().getUserId() + " has no permission.");
			}
			String ajax = getParam(req, "ajax");
			if (GET_MBEANS.equals(ajax)) {
				ret.put("mbeans", server.getMbeanNames());
			}
			else if (GET_MBEAN_INFO.equals(ajax)) {
				if (hasParam(req, MBEAN)) {
					String mbeanName = getParam(req, MBEAN);
					try {
						ObjectName name = new ObjectName(mbeanName);
						MBeanInfo info = server.getMBeanInfo(name);
						ret.put("attributes", info.getAttributes());
						ret.put("description", info.getDescription());
					} catch (Exception e) {
						logger.error(e);
						ret.put("error", "'" + mbeanName + "' is not a valid mBean name");
					}
				}
				else {
					ret.put("error", "No 'mbean' name parameter specified" );
				}
			}
			else if (GET_MBEAN_ATTRIBUTE.equals(ajax)) {
				if (!hasParam(req, MBEAN) || !hasParam(req, ATTRIBUTE)) {
					ret.put("error", "Parameters 'mbean' and 'attribute' must be set");
				}
				else {
					String mbeanName = getParam(req, MBEAN);
					String attribute = getParam(req, ATTRIBUTE);
					
					try {
						ObjectName name = new ObjectName(mbeanName);
						Object obj = server.getMBeanAttribute(name, attribute);
						ret.put("value", obj);
					} catch (Exception e) {
						logger.error(e);
						ret.put("error", "'" + mbeanName + "' is not a valid mBean name");
					}
				}
			}
			else {
				ret.put("commands", new String[] {
						GET_MBEANS, 
						GET_MBEAN_INFO+"&"+MBEAN+"=<name>", 
						GET_MBEAN_ATTRIBUTE+"&"+MBEAN+"=<name>&"+ATTRIBUTE+"=<attributename>"}
				);
			}
			this.writeJSON(resp, ret, true);
		}
		else {
			handleJMXPage(req, resp, session);
		}
	}

	private void handleJMXPage(HttpServletRequest req, HttpServletResponse resp, Session session) {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/jmxpage.vm");
		
		if(!hasAdminRole(session.getUser())) {
			page.add("errorMsg", "User " + session.getUser().getUserId() + " has no permission.");
			page.render();
			return;
		}

		page.add("mbeans", server.getMbeanNames());

		page.render();
	}
	
	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {

	}
	
	private boolean hasAdminRole(User user) {
		for(String roleName: user.getRoles()) {
			Role role = userManager.getRole(roleName);
			Permission perm = role.getPermission();
			if (perm.isPermissionSet(Permission.Type.ADMIN)) {
				return true;
			}
		}
		
		return false;
	}
}
