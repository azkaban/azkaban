package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.user.Permission.Type;
import azkaban.webapp.session.Session;

public class ExecutionServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	private ProjectManager projectManager;
	private ExecutorManager executorManager;
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		projectManager = this.getApplication().getProjectManager();
		executorManager = this.getApplication().getExecutorManager();
	}
	
	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "execid")) {
			handleExecutionFlowPage(req, resp, session);
		}
		else {
			handleExecutionsPage(req, resp, session);
		}
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {

	}
	
	private void handleExecutionsPage(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/executionspage.vm");

		List<ExecutableFlow> runningFlows = executorManager.getRunningFlows();
		page.add("runningFlows", runningFlows.isEmpty() ? null : runningFlows);
		
		List<ExecutableFlow> finishedFlows = executorManager.getRecentlyFinishedFlows();
		page.add("recentlyFinished", finishedFlows.isEmpty() ? null : finishedFlows);
		page.render();
	}
	
	private void handleExecutionFlowPage(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/executingflowpage.vm");
		User user = session.getUser();
		String execId = getParam(req, "execid");
		page.add("execid", execId);

		ExecutableFlow flow = null;
		try {
			flow = executorManager.getExecutableFlow(execId);
			if (flow == null) {
				page.add("errorMsg", "Error loading executing flow " + execId + " not found.");
				page.render();
				return;
			}
		} catch (ExecutorManagerException e) {
			page.add("errorMsg", "Error loading executing flow: " + e.getMessage());
			page.render();
			return;
		}
		
		String projectId = flow.getProjectId();
		Project project = getProjectPageByPermission(page, flow.getProjectId(), user, Type.READ);
		if(project == null) {
			page.render();
			return;
		}
		
		page.add("projectName", projectId);
		page.add("flowid", flow.getFlowId());
		
		page.render();
	}
	
	protected Project getProjectPageByPermission(Page page, String projectId, User user, Permission.Type type) {
		Project project = projectManager.getProject(projectId);
		
		if (project == null) {
			page.add("errorMsg", "Project " + project + " not found.");
		}
		else if (!project.hasPermission(user, type)) {
			page.add("errorMsg", "User " + user.getUserId() + " doesn't have " + type.name() + " permissions on " + projectId);
		}
		else {
			return project;
		}
		
		return null;
	}
}
