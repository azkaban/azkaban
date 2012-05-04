package azkaban.webapp.servlet;

import java.io.File;
import java.io.IOException;


import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.project.ProjectManager;
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
            	
            }    		
    	}

    }
    
    private void handleCreate() {
    	
    }
}
