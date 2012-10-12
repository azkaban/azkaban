/*
 * Copyright 2010 LinkedIn, Inc
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
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import azkaban.executor.ExecutorManager;
import azkaban.fsviewers.HdfsAvroFileViewer;
import azkaban.fsviewers.HdfsFileViewer;
import azkaban.fsviewers.JsonSequenceFileViewer;
import azkaban.fsviewers.TextFileViewer;
import azkaban.project.ProjectManager;
import azkaban.scheduler.ScheduleManager;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.SecurityUtils;
import azkaban.utils.WebUtils;
import azkaban.webapp.session.Session;

/**
 * A servlet that shows the filesystem contents
 * 
 * @author jkreps
 * 
 */

public class HdfsBrowserServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	
    private ArrayList<HdfsFileViewer> _viewers = new ArrayList<HdfsFileViewer>();

    // Default viewer will be a text viewer
    private HdfsFileViewer _defaultViewer = new TextFileViewer();


    
    private static Logger logger = Logger.getLogger(HdfsBrowserServlet.class);

    private Configuration conf;
    
    private Properties property;
    
//    public HdfsBrowserServlet() {
//        super();
//        _viewers.add(new HdfsAvroFileViewer());
//        _viewers.add(new JsonSequenceFileViewer());
//    }

    @Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		
		_viewers.add(new HdfsAvroFileViewer());
		_viewers.add(new JsonSequenceFileViewer());
		
		property = this.getApplication().getAzkabanProps().toProperties();
		
		conf = new Configuration();
		conf.setClassLoader(this.getApplication().getClassLoader());
		
        logger.info("HDFS Browser init");
        logger.info("hadoop.security.authentication set to " + conf.get("hadoop.security.authentication"));
        logger.info("hadoop.security.authorization set to " + conf.get("hadoop.security.authorization"));
        logger.info("DFS name " + conf.get("fs.default.name"));
    }


    
    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
      
    	String user = session.getUser().getUserId();
    	UserGroupInformation ugi = null;
    	try {
    		ugi = SecurityUtils.getProxiedUser(user, this.property, logger, conf);
    	   	FileSystem fs = ugi.doAs(new PrivilegedAction<FileSystem>(){

    	   		@Override	
    	   		public FileSystem run() {
    	   			try {
    	   				return FileSystem.get(conf);
    	   			} catch (IOException e) {
    	   				throw new RuntimeException(e);
    	   			}
    	   		}});

    	   	try {
    	   		handleFSDisplay(fs, user, req, resp, session);
    	   	} catch (IOException e) {
    	   		fs.close();
    	   		throw e;
    	   	}
    	   	fs.close();
    	}
    	catch (Exception e) {
    		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/hdfsbrowserpage.vm");
    		page.add("error_message", e.getMessage());
    		page.add("no_fs", "true");
    		page.render();
    	}
	}


//    private void setCookieInResponse(HttpServletResponse resp, String key, String value) {
//        if (value == null) {
//            Cookie cookie = new Cookie(key, "");
//            cookie.setMaxAge(0);
//            resp.addCookie(cookie);
//        }
//        else {
//            Cookie cookie = new Cookie(key, value);
//            resp.addCookie(cookie);
//        }
//    }
     
//    private String getUserFromRequest(HttpServletRequest req) {
//        Cookie cookie = getCookieByName(req, SESSION_ID_NAME);
//        if (cookie == null) {
//            return null;
//        }
//        return cookie.getValue();
//    }
    
//    @Override
//    protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) 
//            throws ServletException, IOException {
//        if (hasParam(req, "logout")) {
//            setCookieInResponse(resp, SESSION_ID_NAME, null);
//            Page page = newPage(req, resp, "azkaban/web/pages/hdfs_browser_login.vm");
//            page.render();
//        } else if(hasParam(req, "login")) {
//            Props prop = this.getApplication().getAzkabanProps();
//            Properties property = prop.toProperties();
//            
//            String user = getParam(req, "login");
//            logger.info("hadoop.security.authentication set to " + conf.get("hadoop.security.authentication"));
//            logger.info("hadoop.security.authorization set to " + conf.get("hadoop.security.authorization"));
//            
//            UserGroupInformation ugi = SecurityUtils.getProxiedUser(user, property, logger, conf);
//            logger.info("Logging in as " + user);
//            FileSystem fs = ugi.doAs(new PrivilegedAction<FileSystem>(){
//                @Override
//                public FileSystem run() {
//                    try {
//                        return FileSystem.get(conf);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }});
//
//            setCookieInResponse(resp, SESSION_ID_NAME, user);
//            try {
//                handleFSDisplay(fs, user, req, resp);
//            } catch (IOException e) {
//                throw e;
//            }
//            finally {
//                fs.close();
//            }
//        }
//    }
    
    private void handleFSDisplay(FileSystem fs, String user, HttpServletRequest req, HttpServletResponse resp, Session session) throws IOException {
        String prefix = req.getContextPath() + req.getServletPath();
        String fsPath = req.getRequestURI().substring(prefix.length());
        if(fsPath.length() == 0)
            fsPath = "/";

        if(logger.isDebugEnabled())
            logger.debug("path=" + fsPath);

        Path path = new Path(fsPath);
        if(!fs.exists(path)) {
            throw new IllegalArgumentException(path.toUri().getPath() + " does not exist.");
        }
        else if(fs.isFile(path)) {
            displayFile(fs, req, resp, session, path);
        }
        else if(fs.getFileStatus(path).isDir()) {
                displayDir(fs, user, req, resp, session, path);
        } else {
            throw new IllegalStateException("It exists, it is not a file, and it is not a directory, what is it precious?");
        }
    }

    private void displayDir(FileSystem fs, String user, HttpServletRequest req, HttpServletResponse resp, Session session, Path path)
            throws IOException {

        Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/hdfsbrowserpage.vm");

        List<Path> paths = new ArrayList<Path>();
        List<String> segments = new ArrayList<String>();
        Path curr = path;
        while(curr.getParent() != null) {
            paths.add(curr);
            segments.add(curr.getName());
            curr = curr.getParent();
        }

        Collections.reverse(paths);
        Collections.reverse(segments);

        page.add("paths", paths);
        page.add("segments", segments);

        try {
            page.add("subdirs", fs.listStatus(path)); // ??? line
        }
        catch (AccessControlException e) {
            page.add("error_message", "Permission denied. User cannot read file or directory");
        }
        catch (IOException e) {
            page.add("error_message", e.getMessage());
        }
        page.render();

    }

    private void displayFile(FileSystem fs, HttpServletRequest req, HttpServletResponse resp, Session session, Path path)
            throws IOException {
        int startLine = getIntParam(req, "start_line", 1);
        int endLine = getIntParam(req, "end_line", 1000);

        // use registered viewers to show the file content
        boolean outputed = false;
        OutputStream output = resp.getOutputStream();
        for(HdfsFileViewer viewer: _viewers) {
            if(viewer.canReadFile(fs, path)) {
                viewer.displayFile(fs, path, output, startLine, endLine);
                outputed = true;
                break; // don't need to try other viewers
            }
        }

        // use default text viewer
        if(!outputed) {
            if(_defaultViewer.canReadFile(fs, path)) {
                _defaultViewer.displayFile(fs, path, output, startLine, endLine);
            } else {
                output.write(("Sorry, no viewer available for this file. ").getBytes("UTF-8"));
            }
        }
    }

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
	}






}
