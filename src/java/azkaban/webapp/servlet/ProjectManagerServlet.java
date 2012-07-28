package azkaban.webapp.servlet;

import java.awt.geom.Point2D;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.session.Session;
import azkaban.webapp.servlet.MultipartParser;

public class ProjectManagerServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1;
	private static final Logger logger = Logger.getLogger(ProjectManagerServlet.class);
	private static final int DEFAULT_UPLOAD_DISK_SPOOL_SIZE = 20 * 1024 * 1024;
	private static final NodeLevelComparator NODE_LEVEL_COMPARATOR = new NodeLevelComparator();
	
	private ProjectManager manager;
	private MultipartParser multipartParser;
	private File tempDir;
	private static Comparator<Flow> FLOW_ID_COMPARATOR = new Comparator<Flow>() {
		@Override
		public int compare(Flow f1, Flow f2) {
			return f1.getId().compareTo(f2.getId());
		}
	};

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		manager = this.getApplication().getProjectManager();
		tempDir = this.getApplication().getTempDirectory();
		multipartParser = new MultipartParser(DEFAULT_UPLOAD_DISK_SPOOL_SIZE);
	}

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if ( hasParam(req, "project") ) {
			if (hasParam(req, "json")) {
				handleJSONAction(req, resp, session);
			}
			else if (hasParam(req, "permissions")) {
				handlePermissionPage(req, resp, session);
			}
			else if (hasParam(req, "job")) {
				handleJobPage(req, resp, session);
			}
			else if (hasParam(req, "flow")) {
				handleFlowPage(req, resp, session);
			}
			else {
				handleProjectPage(req, resp, session);
			}
			return;
		}
		
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/projectpage.vm");
		page.add("errorMsg", "No project set.");
		page.render();
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
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
	
	private void handleJSONAction(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		String projectName = getParam(req, "project");
		User user = session.getUser();
		
		HashMap<String, Object> ret = new HashMap<String, Object>();
		ret.put("project", projectName);
		
		Project project = null;
		try {
			project = manager.getProject(projectName, user);
		} catch (Exception e) {
			ret.put("error", e.getMessage());
			this.writeJSON(resp, ret);
			return;
		}

		String jsonName = getParam(req, "json");
		if (jsonName.equals("fetchflowjobs")) {
			if (handleJsonPermission(project, user, Type.READ, ret)) {
				jsonFetchFlow(project, ret, req, resp);
			}
		}
		else if (jsonName.equals("fetchflowgraph")) {
			if (handleJsonPermission(project, user, Type.READ, ret)) {
				jsonFetchFlowGraph(project, ret, req, resp);
			}
		}
		else if (jsonName.equals("fetchprojectflows")) {
			if (handleJsonPermission(project, user, Type.READ, ret)) {
				jsonFetchProjectFlows(project, ret, req, resp);
			}
		}
		else if (jsonName.equals("changeDescription")) {
			if (handleJsonPermission(project, user, Type.WRITE, ret)) {
				jsonChangeDescription(project, ret, req, resp);
			}
		}
		
		this.writeJSON(resp, ret);
	}
	
	private boolean handleJsonPermission(Project project, User user, Type type, Map<String, Object> ret) {
		if (project.hasPermission(user, type)) {
			return true;
		}
		
		ret.put("error", "Permission denied. Need " + type.toString() + " access.");
		return false;
	}
	
	private void jsonChangeDescription(Project project, HashMap<String, Object> ret, HttpServletRequest req, HttpServletResponse resp) throws ServletException {
		String description = getParam(req, "description");
		project.setDescription(description);
		
		try {
			manager.commitProject(project.getName());
		} catch (ProjectManagerException e) {
			ret.put("error", e.getMessage());
		}
	}
	
	private void jsonFetchProjectFlows(Project project, HashMap<String, Object> ret, HttpServletRequest req, HttpServletResponse resp) throws ServletException {
		ArrayList<Map<String,Object>> flowList = new ArrayList<Map<String,Object>>();
		for (Flow flow: project.getFlows()) {
			HashMap<String, Object> flowObj = new HashMap<String, Object>();
			flowObj.put("flowId", flow.getId());
			flowList.add(flowObj);
		}
		
		ret.put("flows", flowList); 
	}
	
	private void jsonFetchFlowGraph(Project project, HashMap<String, Object> ret, HttpServletRequest req, HttpServletResponse resp) throws ServletException {
		String flowId = getParam(req, "flow");
		Flow flow = project.getFlow(flowId);
		
		//Collections.sort(flowNodes, NODE_LEVEL_COMPARATOR);
		ArrayList<Map<String, Object>> nodeList = new ArrayList<Map<String, Object>>();
		for (Node node: flow.getNodes()) {
			HashMap<String, Object> nodeObj = new HashMap<String,Object>();
			nodeObj.put("id", node.getId());
			nodeObj.put("x", node.getPosition().getX());
			nodeObj.put("y", node.getPosition().getY());
			nodeObj.put("level", node.getLevel());
			if (node.getState() != Node.State.WAITING) {
				nodeObj.put("state", node.getState());
			}
			nodeList.add(nodeObj);
		}
		
		ArrayList<Map<String, Object>> edgeList = new ArrayList<Map<String, Object>>();
		for (Edge edge: flow.getEdges()) {
			HashMap<String, Object> edgeObj = new HashMap<String,Object>();
			edgeObj.put("from", edge.getSourceId());
			edgeObj.put("target", edge.getTargetId());
			
			if (edge.hasError()) {
				edgeObj.put("error", edge.getError());
			}
			if (edge.getGuideValues() != null) {
				List<Point2D> guides = edge.getGuideValues();
				ArrayList<Object> guideOutput = new ArrayList<Object>();
				for (Point2D guide: guides) {
					double x = guide.getX();
					double y = guide.getY();
					HashMap<String, Double> point = new HashMap<String, Double>();
					point.put("x", x);
					point.put("y", y);
					guideOutput.add(point);
				}
				
				edgeObj.put("guides", guideOutput);
			}
			
			edgeList.add(edgeObj);
		}
		
		ret.put("flowId", flowId);
		ret.put("nodes", nodeList);
		ret.put("edges", edgeList);
	}
	
	private void jsonFetchFlow(Project project, HashMap<String, Object> ret, HttpServletRequest req, HttpServletResponse resp) throws ServletException {
		String flowId = getParam(req, "flow");
		Flow flow = project.getFlow(flowId);

		ArrayList<Node> flowNodes = new ArrayList<Node>(flow.getNodes());
		Collections.sort(flowNodes, NODE_LEVEL_COMPARATOR);

		ArrayList<Object> nodeList = new ArrayList<Object>();
		for (Node node: flowNodes) {
			HashMap<String, Object> nodeObj = new HashMap<String, Object>();
			nodeObj.put("id", node.getId());
			
			ArrayList<String> dependencies = new ArrayList<String>();
			Collection<Edge> collection = flow.getInEdges(node.getId());
			if (collection != null) {
				for (Edge edge: collection) {
					dependencies.add(edge.getSourceId());
				}
			}
			
			ArrayList<String> dependents = new ArrayList<String>();
			collection = flow.getOutEdges(node.getId());
			if (collection != null) {
				for (Edge edge: collection) {
					dependents.add(edge.getTargetId());
				}
			}
			
			nodeObj.put("dependencies", dependencies);
			nodeObj.put("dependents", dependents);
			nodeObj.put("level", node.getLevel());
			nodeList.add(nodeObj);
		}
		
		ret.put("flowId", flowId);
		ret.put("nodes", nodeList);
	}
	
	private void handlePermissionPage(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/permissionspage.vm");
		String projectName = getParam(req, "project");
		User user = session.getUser();
		
		Project project = null;
		try {
			project = manager.getProject(projectName, user);
			if (project == null) {
				page.add("errorMsg", "Project " + projectName + " not found.");
			}
			else {
				page.add("project", project);
				
				page.add("admins", Utils.flattenToString(project.getUsersWithPermission(Type.ADMIN), ","));
				page.add("userpermission", project.getUserPermission(user));
				page.add("permissions", project.getUserPermissions());
			}
		}
		catch(AccessControlException e) {
			page.add("errorMsg", e.getMessage());
		}
		
		page.render();
	}
	
	private void handleJobPage(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/jobpage.vm");
		String projectName = getParam(req, "project");
		String flowName = getParam(req, "flow");
		String jobName = getParam(req, "job");
		
		User user = session.getUser();
		Project project = null;
		Flow flow = null;
		try {
			project = manager.getProject(projectName, user);
			if (project == null) {
				page.add("errorMsg", "Project " + projectName + " not found.");
			}
			else {
				page.add("project", project);
				
				flow = project.getFlow(flowName);
				if (flow == null) {
					page.add("errorMsg", "Flow " + flowName + " not found.");
				}
				else {
					page.add("flowid", flow.getId());
					
					Node node = flow.getNode(jobName);
					
					if (node == null) {
						page.add("errorMsg", "Job " + jobName + " not found.");
					}
					else {
						Props prop = manager.getProperties(projectName, node.getJobSource(), user);
						page.add("jobid", node.getId());
						page.add("jobtype", node.getType());
						
						ArrayList<String> dependencies = new ArrayList<String>();
						Set<Edge> inEdges = flow.getInEdges(node.getId());
						if (inEdges != null) {
							for ( Edge dependency: inEdges ) {
								dependencies.add(dependency.getSourceId());
							}
						}
						if (!dependencies.isEmpty()) {
							page.add("dependencies", dependencies);
						}
						
						ArrayList<String> dependents = new ArrayList<String>();
						Set<Edge> outEdges = flow.getOutEdges(node.getId());
						if (outEdges != null) {
							for ( Edge dependent: outEdges ) {
								dependents.add(dependent.getTargetId());
							}
						}
						if (!dependents.isEmpty()) {
							page.add("dependents", dependents);
						}
						
						// Resolve property dependencies
						String source = node.getPropsSource();
						page.add("properties", source);

						ArrayList<Pair<String,String>> parameters = new ArrayList<Pair<String, String>>();
						// Parameter
						for (String key : prop.getKeySet()) {
							String value = prop.get(key);
							parameters.add(new Pair<String,String>(key, value));
						}
						
						page.add("parameters", parameters);
					}
				}
			}
		}
		catch (AccessControlException e) {
			page.add("errorMsg", e.getMessage());
		} catch (ProjectManagerException e) {
			page.add("errorMsg", e.getMessage());
		}
		
		page.render();
	}
	
	private void handleFlowPage(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/flowpage.vm");
		String projectName = getParam(req, "project");
		String flowName = getParam(req, "flow");

		User user = session.getUser();
		Project project = null;
		Flow flow = null;
		try {
			project = manager.getProject(projectName, user);
			if (project == null) {
				page.add("errorMsg", "Project " + projectName + " not found.");
			}
			else {
				page.add("project", project);
				
				flow = project.getFlow(flowName);
				if (flow == null) {
					page.add("errorMsg", "Flow " + flowName + " not found.");
				}
				
				page.add("flowid", flow.getId());
			}
		}
		catch (AccessControlException e) {
			page.add("errorMsg", e.getMessage());
		}
		
		page.render();
	}

	private void handleProjectPage(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/projectpage.vm");
		String projectName = getParam(req, "project");
		
		User user = session.getUser();
		Project project = null;
		try {
			project = manager.getProject(projectName, user);
			if (project == null) {
				page.add("errorMsg", "Project " + projectName + " not found.");
			}
			else {
				if (project.hasPermission(user, Type.ADMIN)) {
					page.add("admin", true);
				}
				page.add("project", project);
				page.add("admins", Utils.flattenToString(project.getUsersWithPermission(Type.ADMIN), ","));
				page.add("userpermission", project.getUserPermission(user));
	
				List<Flow> flows = project.getFlows();
				if (!flows.isEmpty()) {
					Collections.sort(flows, FLOW_ID_COMPARATOR);
					page.add("flows", flows);
				}
			}
		}
		catch (AccessControlException e) {
			page.add("errorMsg", e.getMessage());
		}
		page.render();
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

	private static class NodeLevelComparator implements Comparator<Node> {
		@Override
		public int compare(Node node1, Node node2) {
			return node1.getLevel() - node2.getLevel();
		}
	}
}
