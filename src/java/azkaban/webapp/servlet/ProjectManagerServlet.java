package azkaban.webapp.servlet;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.security.AccessControlException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipFile;


import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Utils;
import azkaban.webapp.session.Session;
import azkaban.webapp.servlet.MultipartParser;

public class ProjectManagerServlet extends LoginAbstractAzkabanServlet {
    private static final long serialVersionUID = 1;
    private static final Logger logger = Logger.getLogger(ProjectManagerServlet.class);
    private static final int DEFAULT_UPLOAD_DISK_SPOOL_SIZE = 20 * 1024 * 1024;
    
    private ProjectManager manager;
    private MultipartParser multipartParser;
    private File tempDir;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        manager = this.getApplication().getProjectManager();
        tempDir = this.getApplication().getTempDirectory();
        multipartParser = new MultipartParser(DEFAULT_UPLOAD_DISK_SPOOL_SIZE);
    }

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
            Session session) throws ServletException, IOException {
        Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/projectmanager.vm");
        User user = session.getUser();
        
        if ( hasParam(req, "project") ) {
        	String projectName = getParam(req, "project");
        	Project project = null;
        	try {
        		project = manager.getProject(projectName, user);
        		if (project == null) {
            		page.add("errorMsg", "Project " + projectName + " not found.");
        		}
        		else {
        			page.add("project", project);
        			page.add("admins", Utils.flattenToString(project.getUsersWithPermission(Type.ADMIN), ","));
        			page.add("permissions", project.getUserPermission(user));
        			
        			
        		}
        		
        	}
        	catch (AccessControlException e) {
        		page.add("errorMsg", e.getMessage());
        	}

        }
        else {
    		page.add("errorMsg", "No project set.");
        }
        page.render();
    }

    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
            Session session) throws ServletException, IOException {
    	if (ServletFileUpload.isMultipartContent(req)) {
    		logger.info("Post is multipart");
    		Map<String, Object> params = multipartParser.parseMultipart(req);
    		if (params.containsKey("action")) {
    			String action = (String)params.get("action");
    			if (action.equals("upload")) {
    				handleUpload(req, resp, params, session);
    			}
    		}
    	}
    	else if (hasParam(req, "action")) {
    		String action = getParam(req, "action");
            if (action.equals("create")) {
            	handleCreate(req, resp, session);
            } 
    	}

    }
    
    private void handleCreate(HttpServletRequest req, HttpServletResponse resp,
            Session session) throws ServletException {
    	
    	String projectName = hasParam(req, "name") ? getParam(req, "name") : null;
    	String projectDescription = hasParam(req, "description") ? getParam(req, "description") : null;
    	logger.info("Create project " + projectName);
    	
    	User user = session.getUser();
    	
    	String status = null;
    	String action = null;
    	String message = null;
    	HashMap<String, Object> params = null;
    	try {
			manager.createProject(projectName, projectDescription, user);
			status = "success";
			action = "redirect";
			String redirect = "manager?project=" + projectName;
			params = new HashMap<String, Object>();
			params.put("path", redirect);
		} catch (ProjectManagerException e) {
			message = e.getMessage();
			status = "error";
		}
 
    	String response = createJsonResponse(status, message, action, params);
    	try {
			Writer write = resp.getWriter();
			write.append(response);
			write.flush();
    	} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    private void handleUpload(HttpServletRequest req, HttpServletResponse resp, Map<String, Object> multipart,
            Session session) throws ServletException, IOException {
    	
    	User user = session.getUser();
    	String projectName = (String) multipart.get("project");
        FileItem item = (FileItem) multipart.get("file");
        String forceStr = (String) multipart.get("force");
        boolean force = forceStr == null ? false : Boolean.parseBoolean(forceStr);
        File projectDir = null;
        if (projectName == null || projectName.isEmpty()) {
        	setErrorMessageInCookie(resp, "No project name found.");
        }
        else if (item == null) {
        	setErrorMessageInCookie(resp, "No file found.");
        }
        else {
	    	try {
	            projectDir = extractFile(item);
	            manager.uploadProject(projectName, projectDir, user, force);
	            setSuccessMessageInCookie(resp, "Project Uploaded");
	    	} 
	    	catch (Exception e) {
	            logger.info("Installation Failed.", e);
	    		setErrorMessageInCookie(resp, "Installation Failed.\n" + e.getMessage());
	        }

	    	if (projectDir != null && projectDir.exists() ) {
	    		FileUtils.deleteDirectory(projectDir);
	    	}
	    	resp.sendRedirect(req.getRequestURI() + "?project=" + projectName);
        }
    }

    private File extractFile(FileItem item) throws IOException, ServletException {
        final String contentType = item.getContentType();
        if (contentType.startsWith("application/zip")) {
            return unzipFile(item);
        }

        throw new ServletException(String.format("Unsupported file type[%s].", contentType));
    }
    
    private File unzipFile(FileItem item) throws ServletException, IOException {
        File temp = File.createTempFile("job-temp", ".zip");
        temp.deleteOnExit();
        OutputStream out = new BufferedOutputStream(new FileOutputStream(temp));
        IOUtils.copy(item.getInputStream(), out);
        out.close();
        ZipFile zipfile = new ZipFile(temp);
        File unzipped = Utils.createTempDir(tempDir);
        Utils.unzip(zipfile, unzipped);
        temp.delete();
        return unzipped;
    }

}
