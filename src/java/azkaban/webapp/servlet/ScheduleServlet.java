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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.swing.text.StyledEditorKit.BoldAction;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Hours;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.Permission.Type;
import azkaban.user.UserManager;
import azkaban.utils.Pair;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.session.Session;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.sla.FlowRule;
import azkaban.sla.JobRule;
import azkaban.sla.SLA;
import azkaban.sla.SLAManager;
import azkaban.sla.SLA.SlaAction;

public class ScheduleServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(ScheduleServlet.class);
	private ProjectManager projectManager;
	private ScheduleManager scheduleManager;
	private SLAManager slaManager;
	private UserManager userManager;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		AzkabanWebServer server = (AzkabanWebServer)getApplication();
		projectManager = server.getProjectManager();
		scheduleManager = server.getScheduleManager();
		userManager = server.getUserManager();
		slaManager = server.getSLAManager();
	}
	
	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
		else {
			handleGetAllSchedules(req, resp, session);
		}
	}
	
	private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String ajaxName = getParam(req, "ajax");
		
		if (ajaxName.equals("schedInfo")) {
			ajaxSchedInfo(req, resp, ret, session.getUser());
		}
		else if(ajaxName.equals("setSla")) {
			ajaxSetSla(req, resp, ret, session.getUser());
		}

		if (ret != null) {
			this.writeJSON(resp, ret);
		}
	}

	private void ajaxSetSla(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user) {
		try {
			
			int projectId = getIntParam(req, "projectId");
			String flowName = getParam(req, "flowName");
			
			Project project = projectManager.getProject(projectId);
			if(!hasPermission(project, user, Permission.Type.SCHEDULE)) {
				ret.put("error", "User " + user + " does not have permission to set SLA for this flow.");
				return;
			}
			
			String slaEmals = getParam(req, "slaEmails");
			System.out.println(slaEmals);			

			String flowRules = getParam(req, "flowRules");
			FlowRule flowRule = parseFlowRule(flowRules);
			
			List<JobRule> jobRule = new ArrayList<JobRule>();
			Map<String, String> jobRules = getParamGroup(req, "jobRules");
			System.out.println(jobRules);
			for(String job : jobRules.keySet()) {
				JobRule jr = parseJobRule(job, jobRules.get(job));
				jobRule.add(jr);
			}
			Map<String, Object> options= new HashMap<String, Object>();
			options.put("slaEmails", slaEmals);
			options.put("flowRules", flowRules);
			options.put("jobRules", jobRule);
			Schedule sched = scheduleManager.getSchedule(new Pair<Integer, String>(projectId, flowName));
			//slaManager.addFlowSLA(projectId, project.getName(), flowName, "ready", sched.getFirstSchedTime(), sched.getTimezone(), sched.getPeriod(), DateTime.now(), DateTime.now(), DateTime.now(), user, options);
		
		} catch (ServletException e) {
			ret.put("error", e);
		}
		
	}

	
	private FlowRule parseFlowRule(String flowRules) {
		String[] parts = flowRules.split(",");
		String duration = parts[0];
		String emailAction = parts[1];
		String killAction = parts[2];
		if(emailAction.equals("on") || killAction.equals("on")) {
			if(!duration.equals("")) {
				FlowRule r = new FlowRule();
				ReadablePeriod dur = parseDuration(duration);
				r.setDuration(dur);
				List<SlaAction> actions = new ArrayList<SLA.SlaAction>();
				if(emailAction.equals("on")) {
					actions.add(SlaAction.SENDEMAIL);
				}
				if(killAction.equals("on")) {
					actions.add(SlaAction.KILL);
				}
				r.setActions(actions);
				return r;
			}
		}		
		return null;
	}

	private JobRule parseJobRule(String job, String jobRule) {
		String[] parts = jobRule.split(",");
		String duration = parts[0];
		String emailAction = parts[1];
		String killAction = parts[2];
		if(emailAction.equals("on") || killAction.equals("on")) {
			if(!duration.equals("")) {
				JobRule r = new JobRule();
				r.setJobId(job);
				ReadablePeriod dur = parseDuration(duration);
				r.setDuration(dur);
				List<SlaAction> actions = new ArrayList<SLA.SlaAction>();
				if(emailAction.equals("on")) {
					actions.add(SlaAction.SENDEMAIL);
				}
				if(killAction.equals("on")) {
					actions.add(SlaAction.KILL);
				}
				r.setActions(actions);
				return r;
			}
		}	
		return null;
	}

	private ReadablePeriod parseDuration(String duration) {
		int hour = Integer.parseInt(duration.split(",")[0]);
		int min = Integer.parseInt(duration.split(",")[1]);
		return Hours.hours(hour).toPeriod().plus(Minutes.minutes(min).toPeriod());
	}

	@SuppressWarnings("unchecked")
	private void ajaxSchedInfo(HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> ret, User user) {
		int projId;
		try {
			projId = getIntParam(req, "projId");
			String flowName = getParam(req, "flowName");
			
			Project project = getProjectAjaxByPermission(ret, projId, user, Type.READ);
			if (project == null) {
				ret.put("error", "Error loading project. Project " + projId + " doesn't exist");
				return;
			}
			
			Flow flow = project.getFlow(flowName);
			if (flow == null) {
				ret.put("error", "Error loading flow. Flow " + flowName + " doesn't exist in " + projId);
				return;
			}
			
			SLA sla = slaManager.getSLA(new Pair<Integer, String>(projId, flowName));
			
			if(sla != null) {
				ret.put("slaEmails", (List<String>)sla.getSlaOptions().get("slaEmails"));
				List<String> allJobs = new ArrayList<String>();
				for(Node n : flow.getNodes()) {
					allJobs.add(n.getId());
				}
				ret.put("allJobs", allJobs);
				if(sla.getFlowRules() != null) {
					ret.put("flowRules", sla.getFlowRules());
				}
				if(sla.getJobRules() != null) {
					ret.put("jobRules", sla.getJobRules());
				}
			}
			else {
				ret.put("slaEmails", flow.getFailureEmails());
				List<String> allJobs = new ArrayList<String>();
				Schedule sched = scheduleManager.getSchedule(new Pair<Integer, String>(projId, flowName));
				List<String> disabled = sched.getDisabledJobs(); 
				for(Node n : flow.getNodes()) {
					if(!disabled.contains(n.getId())) {
						allJobs.add(n.getId());
					}
				}
				ret.put("allJobs", allJobs);
				
				
			}
		} catch (ServletException e) {
			ret.put("error", e);
		}
		
	}

	protected Project getProjectAjaxByPermission(Map<String, Object> ret, int projectId, User user, Permission.Type type) {
		Project project = projectManager.getProject(projectId);
		
		if (project == null) {
			ret.put("error", "Project '" + project + "' not found.");
		}
		else if (!hasPermission(project, user, type)) {
			ret.put("error", "User '" + user.getUserId() + "' doesn't have " + type.name() + " permissions on " + project.getName());
		}
		else {
			return project;
		}
		
		return null;
	}
	
	private void handleGetAllSchedules(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException{
		
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/scheduledflowpage.vm");
		
		List<Schedule> schedules = scheduleManager.getSchedules();
		page.add("schedules", schedules);
		
		List<SLA> slas = slaManager.getSLAs();
		page.add("slas", slas);

		page.render();
	}
	
	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		if (hasParam(req, "action")) {
			String action = getParam(req, "action");
			if (action.equals("scheduleFlow")) {
				ajaxScheduleFlow(req, ret, session.getUser());
			}
			else if(action.equals("removeSched")){
				ajaxRemoveSched(req, ret, session.getUser());
			}
		}
		
		if(ret.get("status") == ("success"))
			setSuccessMessageInCookie(resp, (String) ret.get("message"));
		else
			setErrorMessageInCookie(resp, (String) ret.get("message"));
		
		this.writeJSON(resp, ret);
	}

//	private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
//		HashMap<String, Object> ret = new HashMap<String, Object>();
//		String ajaxName = getParam(req, "ajax");
//		
//		if (ajaxName.equals("scheduleFlow")) {
//				ajaxScheduleFlow(req, ret, session.getUser());
//		}
////		}
//		this.writeJSON(resp, ret);
//	}
//	
	
	private void ajaxRemoveSched(HttpServletRequest req, Map<String, Object> ret, User user) throws ServletException{
		int projectId = getIntParam(req, "projectId");
		String flowName = getParam(req, "flowName");
		Pair<Integer, String> scheduleId = new Pair<Integer, String>(projectId, flowName);
		Schedule sched = scheduleManager.getSchedule(scheduleId);

//		int projectId = sched.getProjectId();

		Project project = projectManager.getProject(projectId);
		
		if (project == null) {
			ret.put("message", "Project " + projectId + " does not exist");
			ret.put("status", "error");
			return;
		}
		
		if(!hasPermission(project, user, Type.SCHEDULE)) {
			ret.put("status", "error");
			ret.put("message", "Permission denied. Cannot remove schedule " + projectId + "."  + flowName);
			return;
		}

		scheduleManager.removeSchedule(scheduleId);
		logger.info("User '" + user.getUserId() + " has removed schedule " + sched.getScheduleName());
		projectManager.postProjectEvent(project, EventType.SCHEDULE, user.getUserId(), "Schedule " + sched.toString() + " has been removed.");
		
		ret.put("status", "success");
		ret.put("message", "flow " + scheduleId.getSecond() + " removed from Schedules.");
		return;
	}
	
	private void ajaxScheduleFlow(HttpServletRequest req, Map<String, Object> ret, User user) throws ServletException {
		String projectName = getParam(req, "projectName");
		String flowName = getParam(req, "flowName");
		int projectId = getIntParam(req, "projectId");
		
		Project project = projectManager.getProject(projectId);
			
		if (project == null) {
			ret.put("message", "Project " + projectName + " does not exist");
			ret.put("status", "error");
			return;
		}
		
		if (!hasPermission(project, user, Type.SCHEDULE)) {
			ret.put("status", "error");
			ret.put("message", "Permission denied. Cannot execute " + flowName);
			return;
		}

		Flow flow = project.getFlow(flowName);
		if (flow == null) {
			ret.put("status", "error");
			ret.put("message", "Flow " + flowName + " cannot be found in project " + project);
			return;
		}
		
		int hour = getIntParam(req, "hour");
		int minutes = getIntParam(req, "minutes");
		boolean isPm = getParam(req, "am_pm").equalsIgnoreCase("pm");
		
		DateTimeZone timezone = getParam(req,  "timezone").equals("UTC") ? DateTimeZone.UTC : DateTimeZone.forID("America/Los_Angeles");

		String scheduledDate = req.getParameter("date");
		DateTime day = null;
		if(scheduledDate == null || scheduledDate.trim().length() == 0) {
			day = new LocalDateTime().toDateTime();
		} else {
		    try {
		    	day = DateTimeFormat.forPattern("MM/dd/yyyy").withZone(timezone).parseDateTime(scheduledDate);
		    } catch(IllegalArgumentException e) {
		      	ret.put("error", "Invalid date: '" + scheduledDate + "'");
		      	return;
		      }
		}

		ReadablePeriod thePeriod = null;
		try {
			if(hasParam(req, "is_recurring"))
			    thePeriod = Schedule.parsePeriodString(getParam(req, "period")+getParam(req,"period_units"));	
		}
		catch(Exception e){
			ret.put("error", e.getMessage());
		}

		if(isPm && hour < 12)
		    hour += 12;
		hour %= 24;

//		String submitUser = user.getUserId();
//		String userExec = userSubmit;//getParam(req, "userExec");
//		String scheduleId = projectId + "." + flowName;
		DateTime submitTime = new DateTime();
		DateTime firstSchedTime = day.withHourOfDay(hour).withMinuteOfHour(minutes).withSecondOfMinute(0);
		
		//ScheduledFlow schedFlow = scheduleManager.schedule(scheduleId, projectId, flowId, userExec, userSubmit, submitTime, firstSchedTime, thePeriod);
		//project.info("User '" + user.getUserId() + "' has scheduled " + flow.getId() + "[" + schedFlow.toNiceString() + "].");
		Schedule schedule = scheduleManager.scheduleFlow(projectId, projectName, flowName, "ready", firstSchedTime.getMillis(), timezone, thePeriod, submitTime.getMillis(), firstSchedTime.getMillis(), firstSchedTime.getMillis(), user.getUserId(), null);
		logger.info("User '" + user.getUserId() + "' has scheduled " + "[" + projectName + flowName +  " (" + projectId +")" + "].");
		projectManager.postProjectEvent(project, EventType.SCHEDULE, user.getUserId(), "Schedule " + schedule.getScheduleName() + " has been added.");
		
		ret.put("status", "success");
		ret.put("message", projectName + "." + flowName + " scheduled.");
	}
				
//	private ReadablePeriod parsePeriod(HttpServletRequest req) throws ServletException {
//			int period = getIntParam(req, "period");
//			String periodUnits = getParam(req, "period_units");
//			if("M".equals(periodUnits))
//				return Months.months(period);
//			else if("w".equals(periodUnits))
//				return Weeks.weeks(period);
//			else if("d".equals(periodUnits))
//				return Days.days(period);
//			else if("h".equals(periodUnits))
//				return Hours.hours(period);
//			else if("m".equals(periodUnits))
//				return Minutes.minutes(period);
//			else if("s".equals(periodUnits))
//				return Seconds.seconds(period);
//			else
//				throw new ServletException("Unknown period unit: " + periodUnits);
//	}

	private boolean hasPermission(Project project, User user, Permission.Type type) {
		if (project.hasPermission(user, type)) {
			return true;
		}
		
		for(String roleName: user.getRoles()) {
			Role role = userManager.getRole(roleName);
			if (role.getPermission().isPermissionSet(type) || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
				return true;
			}
		}
		
		return false;
	}
}
