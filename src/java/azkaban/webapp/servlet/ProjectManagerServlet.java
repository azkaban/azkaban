package azkaban.webapp.servlet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;


import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.user.User;
import azkaban.utils.JSONUtils;
import azkaban.webapp.session.Session;

public class ProjectManagerServlet extends LoginAbstractAzkabanServlet {
    private static final long serialVersionUID = 1;
    private static final Logger logger = Logger.getLogger(ProjectManagerServlet.class);

    private ProjectManager manager;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        manager = this.getApplication().getProjectManager();
    }

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
            Session session) throws ServletException, IOException {
        Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/projectmanager.vm");
        
        page.render();
    }

    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
            Session session) throws ServletException, IOException {
    	if (hasParam(req, "action")) {
    		String action = getParam(req, "action");
            if (action.equals("create")) {
            	handleCreate(req, resp, session);
            }    		
    	}

    }
    
    private void handleCreate(HttpServletRequest req, HttpServletResponse resp,
            Session session) throws ServletException {
    	
//    	String projectName = hasParam(req, "name") ? getParam(req, "name") : null;
//    	String projectDescription = hasParam(req, "description") ? getParam(req, "description") : null;
//    	logger.info("Create project " + projectName);
//    	
//    	User user = session.getUser();
//    	HashMap<String, Object> responseObj = new HashMap<String, Object>();
//    	String status = null;
//    	String redirect = null;
//    	String message = null;
//
//    	try {
//			manager.createProjects(projectName, projectDescription, user);
//			status = "success";
//			redirect = "manager?project=" + projectName;
//		} catch (ProjectManagerException e) {
//			message = e.getMessage();
//			status = "error";
//		}
//    	
//    	String response = createJsonResponse(status, message, redirect, null);
//    	try {
//			Writer write = resp.getWriter();
//			write.append(response);
//			write.flush();
//    	} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    }

}
