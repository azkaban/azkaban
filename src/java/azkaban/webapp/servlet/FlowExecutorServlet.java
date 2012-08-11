package azkaban.webapp.servlet;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.user.User;
import azkaban.user.Permission.Type;
import azkaban.utils.Props;
import azkaban.webapp.session.Session;

public class FlowExecutorServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	private ProjectManager projectManager;
	private ExecutorManager executorManager;
	private File tempDir;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		projectManager = this.getApplication().getProjectManager();
		executorManager = this.getApplication().getExecutorManager();
		tempDir = this.getApplication().getTempDirectory();
	}

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
	}

	private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		String projectName = getParam(req, "project");

		HashMap<String, Object> ret = new HashMap<String, Object>();
		ret.put("project", projectName);
		
		String ajaxName = getParam(req, "ajax");
		if (ajaxName.equals("executeFlow")) {
			ajaxExecuteFlow(req, resp, ret, session.getUser());
		}
		
		this.writeJSON(resp, ret);
	}
	
	private void ajaxExecuteFlow(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user) throws ServletException {
		String projectId = getParam(req, "project");
		String flowId = getParam(req, "flow");
		
		ret.put("flow", flowId);
		
		Project project = projectManager.getProject(projectId, user);
		if (project == null) {
			ret.put("error", "Project " + projectId + " does not exist");
			return;
		}
		
		if (!project.hasPermission(user, Type.EXECUTE)) {
			ret.put("error", "Permission denied. Cannot execute " + flowId);
			return;
		}

		Flow flow = project.getFlow(flowId);
		if (flow == null) {
			ret.put("error", "Flow " + flowId + " cannot be found in project " + project);
			return;
		}
		
		HashMap<String, Props> sources;
		try {
			sources = projectManager.getAllFlowProperties(project, flowId, user);
		}
		catch (ProjectManagerException e) {
			ret.put("error", e.getMessage());
			return;
		}
		
		// Create ExecutableFlow
		ExecutableFlow exflow = executorManager.createExecutableFlow(flow);
		Map<String, String> paramGroup = this.getParamGroup(req, "disabled");
		for (Map.Entry<String, String> entry: paramGroup.entrySet()) {
			boolean nodeDisabled = Boolean.parseBoolean(entry.getValue());
			exflow.setStatus(entry.getKey(), nodeDisabled ? Status.IGNORED : Status.READY);
		}
		
		// Create directory
		try {
			executorManager.setupExecutableFlow(exflow);
		} catch (ExecutorManagerException e) {
			try {
				executorManager.cleanupAll(exflow);
			} catch (ExecutorManagerException e1) {
				e1.printStackTrace();
			}
			ret.put("error", e.getMessage());
			return;
		}

		// Copy files to the source.
		File executionDir = new File(exflow.getExecutionPath());
		try {
			projectManager.copyProjectSourceFilesToDirectory(project, executionDir);
		} catch (ProjectManagerException e) {
			try {
				executorManager.cleanupAll(exflow);
			} catch (ExecutorManagerException e1) {
				e1.printStackTrace();
			}
			ret.put("error", e.getMessage());
			return;
		}
		
		try {
			executorManager.executeFlow(exflow);
		} catch (ExecutorManagerException e) {
			try {
				executorManager.cleanupAll(exflow);
			} catch (ExecutorManagerException e1) {
				e1.printStackTrace();
			}
			
			ret.put("error", e.getMessage());
			return;
		}
		String execId = exflow.getExecutionId();
		
		// The following is just a test for cleanup
//		try {
//			executorManager.cleanupUnusedFiles(exflow);
//		} catch (ExecutorManagerException e) {
//			e.printStackTrace();
//		}
//		
		ret.put("execid", execId);
	}
}
