/*
 * Copyright 2012 LinkedIn, Inc
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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutableFlow.FailureAction;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduledFlow;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.user.Permission.Type;
import azkaban.utils.Utils;
import azkaban.webapp.session.Session;

public class ExecutorServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	private ProjectManager projectManager;
	private ExecutorManager executorManager;
	private ScheduleManager scheduleManager;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		projectManager = this.getApplication().getProjectManager();
		executorManager = this.getApplication().getExecutorManager();
		scheduleManager = this.getApplication().getScheduleManager();
	}

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
		else if (hasParam(req, "execid")) {
			if (hasParam(req, "job")) {
				handleExecutionJobPage(req, resp, session);
			}
			else {
				handleExecutionFlowPage(req, resp, session);
			}
		}
		else {
			handleExecutionsPage(req, resp, session);
		}
	}
	
	private void handleExecutionJobPage(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/joblogpage.vm");
		User user = session.getUser();
		String execId = getParam(req, "execid");
		String jobId = getParam(req, "job");
		page.add("execid", execId);
		page.add("jobid", jobId);
		
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
		if (project == null) {
			page.render();
			return;
		}
		
		page.add("projectName", projectId);
		page.add("flowid", flow.getFlowId());
		
		page.render();
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
	
	protected Project getProjectAjaxByPermission(Map<String, Object> ret, String projectId, User user, Permission.Type type) {
		Project project = projectManager.getProject(projectId);
		
		if (project == null) {
			ret.put("error", "Project '" + project + "' not found.");
		}
		else if (!project.hasPermission(user, type)) {
			ret.put("error", "User '" + user.getUserId() + "' doesn't have " + type.name() + " permissions on " + projectId);
		}
		else {
			return project;
		}
		
		return null;
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
	}

	private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String ajaxName = getParam(req, "ajax");
		
		if (hasParam(req, "execid")) {
			String execid = getParam(req, "execid");
			ExecutableFlow exFlow = null;

			try {
				exFlow = executorManager.getExecutableFlow(execid);
			} catch (ExecutorManagerException e) {
				ret.put("error", "Error fetching execution '" + execid + "': " + e.getMessage());
			}

			if (exFlow == null) {
				ret.put("error", "Cannot find execution '" + execid + "'");
			}
			else {
				if (ajaxName.equals("fetchexecflow")) {
					ajaxFetchExecutableFlow(req, resp, ret, session.getUser(), exFlow);
				}
				else if (ajaxName.equals("fetchexecflowupdate")) {
					ajaxFetchExecutableFlowUpdate(req, resp, ret, session.getUser(), exFlow);
				}
				else if (ajaxName.equals("cancelFlow")) {
					ajaxCancelFlow(req, resp, ret, session.getUser(), exFlow);
				}
				else if (ajaxName.equals("restartFlow")) {
					ajaxRestartFlow(req, resp, ret, session.getUser(), exFlow);
				}
				else if (ajaxName.equals("pauseFlow")) {
					ajaxPauseFlow(req, resp, ret, session.getUser(), exFlow);
				}
				else if (ajaxName.equals("resumeFlow")) {
					ajaxResumeFlow(req, resp, ret, session.getUser(), exFlow);
				}
				else if (ajaxName.equals("fetchExecFlowLogs")) {
					ajaxFetchExecFlowLogs(req, resp, ret, session.getUser(), exFlow);
					ret = null;
				}
				else if (ajaxName.equals("fetchExecJobLogs")) {
					ajaxFetchJobLogs(req, resp, ret, session.getUser(), exFlow);
					ret = null;
				}
				else if (ajaxName.equals("flowInfo")) {
					String projectName = getParam(req, "project");
					String flowName = getParam(req, "flow");
					ajaxFetchExecutableFlowInfo(req, resp, ret, session.getUser(), projectName, flowName, exFlow);
				}
			}
		}
		else if (ajaxName.equals("isRunning")) {
			String projectName = getParam(req, "project");
			String flowName = getParam(req, "flow");
			ajaxIsFlowRunning(req, resp, ret, session.getUser(), projectName, flowName);
		}
		else if (ajaxName.equals("flowInfo")) {
			String projectName = getParam(req, "project");
			String flowName = getParam(req, "flow");
			
			ajaxFetchFlowInfo(req, resp, ret, session.getUser(), projectName, flowName);
		}
		else {
			String projectName = getParam(req, "project");
			
			ret.put("project", projectName);
			if (ajaxName.equals("executeFlow")) {
				ajaxExecuteFlow(req, resp, ret, session.getUser());
			}
		}
		if (ret != null) {
			this.writeJSON(resp, ret);
		}
	}

	/**
	 * Gets the logs through plain text stream to reduce memory overhead.
	 * 
	 * @param req
	 * @param resp
	 * @param user
	 * @param exFlow
	 * @throws ServletException
	 */
	private void ajaxFetchExecFlowLogs(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret,  User user, ExecutableFlow exFlow) throws ServletException {
		Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
		if (project == null) {
			return;
		}
		
		int startChar = this.getIntParam(req, "current");
		int maxSize = this.getIntParam(req, "max");
		
		resp.setContentType("text/plain");
		resp.setCharacterEncoding("utf-8");
		PrintWriter writer;
		try {
			writer = resp.getWriter();
		} catch (IOException e) {
			throw new ServletException(e);
		}

		try {
			long character = executorManager.getExecutableFlowLog(exFlow, writer, startChar, maxSize);
			writer.write("\n");
			writer.write(Long.toString(character));
		} catch (ExecutorManagerException e) {
			throw new ServletException(e);
		}
		finally {
			writer.close();
		}
	}
	
	/**
	 * Gets the logs through ajax plain text stream to reduce memory overhead.
	 * 
	 * @param req
	 * @param resp
	 * @param user
	 * @param exFlow
	 * @throws ServletException
	 */
	private void ajaxFetchJobLogs(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, ExecutableFlow exFlow) throws ServletException {
		Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
		if (project == null) {
			return;
		}
		
		int startChar = this.getIntParam(req, "current");
		int maxSize = this.getIntParam(req, "max");
		String jobId = this.getParam(req, "job");
		
		PrintWriter writer;
		try {
			writer = resp.getWriter();
		} catch (IOException e) {
			throw new ServletException(e);
		}
		
		try {
			long character = executorManager.getExecutionJobLog(exFlow, jobId, writer, startChar, maxSize);
			writer.write("\n");
			writer.write(Long.toString(character));
		} catch (ExecutorManagerException e) {
			e.printStackTrace();
			ret.put("error", e.getMessage());
		}
	}

	private void ajaxFetchFlowInfo(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, String projectId, String flowId) throws ServletException {
		Project project = getProjectAjaxByPermission(ret, projectId, user, Type.READ);
		if (project == null) {
			return;
		}
		
		Flow flow = project.getFlow(flowId);
		if (flow == null) {
			ret.put("error", "Error loading flow. Flow " + flowId + " doesn't exist in " + projectId);
			return;
		}
		
		ret.put("successEmails", flow.getSuccessEmails());
		ret.put("failureEmails", flow.getFailureEmails());
		ret.put("running", executorManager.getFlowRunningFlows(projectId, flowId));
		
		ScheduledFlow sflow = null;
		for (ScheduledFlow schedFlow: scheduleManager.getSchedule()) {
			if (schedFlow.getProjectId().equals(projectId) && schedFlow.getFlowId().equals(flowId)) {
				sflow = schedFlow;
				break;
			}
		}
		
		if (sflow != null) {
			ret.put("scheduled", sflow.getNextExecTime().getMillis());
		}
	}

	private void ajaxFetchExecutableFlowInfo(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, String projectId, String flowId, ExecutableFlow exflow) throws ServletException {
		Project project = getProjectAjaxByPermission(ret, projectId, user, Type.READ);
		if (project == null) {
			return;
		}
		
		Flow flow = project.getFlow(flowId);
		if (flow == null) {
			ret.put("error", "Error loading flow. Flow " + flowId + " doesn't exist in " + projectId);
			return;
		}
		
		ret.put("successEmails", exflow.getSuccessEmails());
		ret.put("failureEmails", flow.getFailureEmails());
		ret.put("flowParam", exflow.getFlowParameters());
		
		FailureAction action = exflow.getFailureAction();
		String failureAction = null;
		switch (action) {
			case FINISH_CURRENTLY_RUNNING:
				failureAction = "finishCurrent";
				break;
			case CANCEL_ALL:
				failureAction = "cancelImmediately";
				break;
			case FINISH_ALL_POSSIBLE:
				failureAction = "finishPossible";
				break;
		}
		ret.put("failureAction", failureAction);
		ret.put("notifyFailureFirst", exflow.getNotifyOnFirstFailure());
		ret.put("notifyFailureLast", exflow.getNotifyOnLastFailure());
		ret.put("running", executorManager.getFlowRunningFlows(projectId, flowId));
		
		ScheduledFlow sflow = null;
		for (ScheduledFlow schedFlow: scheduleManager.getSchedule()) {
			if (schedFlow.getProjectId().equals(projectId) && schedFlow.getFlowId().equals(flowId)) {
				sflow = schedFlow;
				break;
			}
		}
		
		if (sflow != null) {
			ret.put("scheduled", sflow.getNextExecTime().getMillis());
		}
	}
	
	private void ajaxCancelFlow(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, ExecutableFlow exFlow) throws ServletException{
		Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.EXECUTE);
		if (project == null) {
			return;
		}
		
		try {
			executorManager.cancelFlow(exFlow.getExecutionId(), user.getUserId());
		} catch (ExecutorManagerException e) {
			ret.put("error", e.getMessage());
		}
	}

	private void ajaxIsFlowRunning(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, String projectId, String flowId) throws ServletException{
		Project project = getProjectAjaxByPermission(ret, projectId, user, Type.EXECUTE);
		if (project == null) {
			return;
		}
		
		ret.put("running", executorManager.getFlowRunningFlows(projectId, flowId));
	}
	
	private void ajaxRestartFlow(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, ExecutableFlow exFlow) throws ServletException{
		Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.EXECUTE);
		if (project == null) {
			return;
		}
	}

	private void ajaxPauseFlow(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, ExecutableFlow exFlow) throws ServletException{
		Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.EXECUTE);
		if (project == null) {
			return;
		}

		try {
			executorManager.pauseFlow(exFlow.getExecutionId(), user.getUserId());
		} catch (ExecutorManagerException e) {
			ret.put("error", e.getMessage());
		}
	}

	private void ajaxResumeFlow(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, ExecutableFlow exFlow) throws ServletException{
		Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.EXECUTE);
		if (project == null) {
			return;
		}

		try {
			executorManager.resumeFlow(exFlow.getExecutionId(), user.getUserId());
		} catch (ExecutorManagerException e) {
			ret.put("resume", e.getMessage());
		}
	}
	
	private void ajaxFetchExecutableFlowUpdate(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, ExecutableFlow exFlow) throws ServletException{
		Long lastUpdateTime = Long.parseLong(getParam(req, "lastUpdateTime"));
		
		System.out.println("Fetching " + exFlow.getExecutionId());
		
		Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
		if (project == null) {
			return;
		}

		// Just update the nodes and flow states
		ArrayList<Map<String, Object>> nodeList = new ArrayList<Map<String, Object>>();
		for (ExecutableNode node : exFlow.getExecutableNodes()) {
			if (node.getStartTime() < lastUpdateTime && node.getEndTime() < lastUpdateTime) {
				continue;
			}
			
			HashMap<String, Object> nodeObj = new HashMap<String,Object>();
			nodeObj.put("id", node.getId());
			nodeObj.put("status", node.getStatus());
			nodeObj.put("startTime", node.getStartTime());
			nodeObj.put("endTime", node.getEndTime());
			
			nodeList.add(nodeObj);
		}

		ret.put("nodes", nodeList);
		ret.put("status", exFlow.getStatus().toString());
		ret.put("startTime", exFlow.getStartTime());
		ret.put("endTime", exFlow.getEndTime());
		ret.put("submitTime", exFlow.getSubmitTime());
	}
	
	private void ajaxFetchExecutableFlow(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user, ExecutableFlow exFlow) throws ServletException {
		System.out.println("Fetching " + exFlow.getExecutionId());

		Project project = getProjectAjaxByPermission(ret, exFlow.getProjectId(), user, Type.READ);
		if (project == null) {
			return;
		}
		
		ArrayList<Map<String, Object>> nodeList = new ArrayList<Map<String, Object>>();
		ArrayList<Map<String, Object>> edgeList = new ArrayList<Map<String,Object>>();
		for (ExecutableNode node : exFlow.getExecutableNodes()) {
			HashMap<String, Object> nodeObj = new HashMap<String,Object>();
			nodeObj.put("id", node.getId());
			nodeObj.put("level", node.getLevel());
			nodeObj.put("status", node.getStatus());
			nodeObj.put("startTime", node.getStartTime());
			nodeObj.put("endTime", node.getEndTime());
			
			nodeList.add(nodeObj);
			
			// Add edges
			for (String out: node.getOutNodes()) {
				HashMap<String, Object> edgeObj = new HashMap<String,Object>();
				edgeObj.put("from", node.getId());
				edgeObj.put("target", out);
				edgeList.add(edgeObj);
			}
		}

		ret.put("nodes", nodeList);
		ret.put("edges", edgeList);
		ret.put("status", exFlow.getStatus().toString());
		ret.put("startTime", exFlow.getStartTime());
		ret.put("endTime", exFlow.getEndTime());
		ret.put("submitTime", exFlow.getSubmitTime());
		ret.put("submitUser", exFlow.getSubmitUser());
	}
	
	private void ajaxExecuteFlow(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user) throws ServletException {
		String projectId = getParam(req, "project");
		String flowId = getParam(req, "flow");
		
		ret.put("flow", flowId);
		
		Project project = getProjectAjaxByPermission(ret, projectId, user, Type.EXECUTE);
		if (project == null) {
			return;
		}

		Flow flow = project.getFlow(flowId);
		if (flow == null) {
			ret.put("error", "Flow '" + flowId + "' cannot be found in project " + project);
			return;
		}
		
		// Create ExecutableFlow
		ExecutableFlow exflow = executorManager.createExecutableFlow(flow);
		exflow.setSubmitUser(user.getUserId());
		if (hasParam(req, "failureAction")) {
			String option = getParam(req, "failureAction");
			if (option.equals("finishCurrent") ) {
				exflow.setFailureAction(FailureAction.FINISH_CURRENTLY_RUNNING);
			}
			else if (option.equals("cancelImmediately")) {
				exflow.setFailureAction(FailureAction.CANCEL_ALL);
			}
			else if (option.equals("finishPossible")) {
				exflow.setFailureAction(FailureAction.FINISH_ALL_POSSIBLE);
			}
		}
		if (hasParam(req, "failureEmails")) {
			String emails = getParam(req, "failureEmails");
			String[] emailSplit = emails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
			exflow.setFailureEmails(Arrays.asList(emailSplit));
		}
		if (hasParam(req, "successEmails")) {
			String emails = getParam(req, "successEmails");
			String[] emailSplit = emails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
			exflow.setSuccessEmails(Arrays.asList(emailSplit));
		}
		if (hasParam(req, "notifyFailureFirst")) {
			exflow.setNotifyOnFirstFailure(Boolean.parseBoolean(getParam(req, "notifyFailureFirst")));
		}
		if (hasParam(req, "notifyFailureLast")) {
			exflow.setNotifyOnLastFailure(Boolean.parseBoolean(getParam(req, "notifyFailureLast")));
		}
		if (hasParam(req, "executingJobOption")) {
			String option = getParam(req, "jobOption");
			// Not set yet
		}
		
		Map<String, String> flowParamGroup = this.getParamGroup(req, "flowOverride");
		exflow.addFlowParameters(flowParamGroup);
		
		// Setup disabled
		Map<String, String> paramGroup = this.getParamGroup(req, "disable");
		for (Map.Entry<String, String> entry: paramGroup.entrySet()) {
			boolean nodeDisabled = Boolean.parseBoolean(entry.getValue());
			exflow.setStatus(entry.getKey(), nodeDisabled ? Status.DISABLED : Status.READY);
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
			project.info("User '" + user.getUserId() + "' executed flow '" + exflow.getExecutionId() + "'");
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

		ret.put("execid", execId);
	}
}
