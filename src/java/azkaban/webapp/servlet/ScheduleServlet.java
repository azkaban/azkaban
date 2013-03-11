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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;

import azkaban.executor.ExecutableFlow.FailureAction;
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
import azkaban.scheduler.Schedule.FlowOptions;
import azkaban.scheduler.Schedule.SlaOptions;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.sla.SLA;
import azkaban.sla.SLA.SlaRule;
import azkaban.sla.SLA.SlaAction;
import azkaban.sla.SLA.SlaSetting;

public class ScheduleServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(ScheduleServlet.class);
	private ProjectManager projectManager;
	private ScheduleManager scheduleManager;
	private UserManager userManager;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		AzkabanWebServer server = (AzkabanWebServer)getApplication();
		projectManager = server.getProjectManager();
		scheduleManager = server.getScheduleManager();
		userManager = server.getUserManager();
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
		
		if (ajaxName.equals("slaInfo")) {
			ajaxSlaInfo(req, ret, session.getUser());
		}
		else if(ajaxName.equals("setSla")) {
			ajaxSetSla(req, ret, session.getUser());
		}
		else if(ajaxName.equals("advSchedule")) {
			ajaxAdvSchedule(req, ret, session.getUser());
		}

		if (ret != null) {
			this.writeJSON(resp, ret);
		}
	}

	private void ajaxSetSla(HttpServletRequest req, HashMap<String, Object> ret, User user) {
		try {
			
			int projectId = getIntParam(req, "projectId");
			String flowName = getParam(req, "flowName");
			
			Project project = projectManager.getProject(projectId);
			if(!hasPermission(project, user, Permission.Type.SCHEDULE)) {
				ret.put("error", "User " + user + " does not have permission to set SLA for this flow.");
				return;
			}
			
			Schedule sched = scheduleManager.getSchedule(new Pair<Integer, String>(projectId, flowName));
			
			SlaOptions slaOptions= new SlaOptions();
			
			String slaEmails = getParam(req, "slaEmails");
			String[] emailSplit = slaEmails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
			
			Map<String, String> settings = getParamGroup(req, "settings");
			List<SlaSetting> slaSettings = new ArrayList<SlaSetting>();
			for(String set : settings.keySet()) {
				SlaSetting s;
				try {
				s = parseSlaSetting(settings.get(set));
				}
				catch (Exception e) {
					throw new ServletException(e);
				}
				if(s != null) {
					slaSettings.add(s);
				}
			}
			
			if(slaSettings.size() > 0) {
				if(slaEmails.equals("")) {
					ret.put("error", "Please put correct email settings for your SLA actions");
					return;
				}
				slaOptions.setSlaEmails(Arrays.asList(emailSplit));
				slaOptions.setSettings(slaSettings);
			}
			else {
				slaOptions = null;
			}
			sched.setSlaOptions(slaOptions);
			scheduleManager.insertSchedule(sched);

			if(slaOptions != null) {
				projectManager.postProjectEvent(project, EventType.SLA, user.getUserId(), "SLA for flow " + flowName + " has been added/changed.");
			}
			
		} catch (ServletException e) {
			ret.put("error", e.getMessage());
		}
		
	}

	
	private SlaSetting parseSlaSetting(String set) throws ScheduleManagerException {
		// "" + Duration + EmailAction + KillAction
		String[] parts = set.split(",", -1);
		String id = parts[0];
		String rule = parts[1];
		String duration = parts[2];
		String emailAction = parts[3];
		String killAction = parts[4];
		if(emailAction.equals("true") || killAction.equals("true")) {
			SlaSetting r = new SlaSetting();			
			r.setId(id);
			r.setRule(SlaRule.valueOf(rule));
			ReadablePeriod dur;
			try {
				dur = parseDuration(duration);
			}
			catch (Exception e) {
				throw new ScheduleManagerException("Unable to parse duration for a SLA that needs to take actions!", e);
			}
			r.setDuration(dur);
			List<SlaAction> actions = new ArrayList<SLA.SlaAction>();
			if(emailAction.equals("true")) {
				actions.add(SlaAction.EMAIL);
			}
			if(killAction.equals("true")) {
				actions.add(SlaAction.KILL);
			}
			r.setActions(actions);
			return r;
		}
		return null;
	}

	private ReadablePeriod parseDuration(String duration) {
		int hour = Integer.parseInt(duration.split(":")[0]);
		int min = Integer.parseInt(duration.split(":")[1]);
		return Minutes.minutes(min+hour*60).toPeriod();
	}

	@SuppressWarnings("unchecked")
	private void ajaxSlaInfo(HttpServletRequest req, HashMap<String, Object> ret, User user) {
		int projId;
		String flowName;
		try {
			projId = getIntParam(req, "projId");
			flowName = getParam(req, "flowName");
			
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
			
			Schedule sched = scheduleManager.getSchedule(new Pair<Integer, String>(projId, flowName));
			
			SlaOptions slaOptions = sched.getSlaOptions();
			FlowOptions flowOptions = sched.getFlowOptions();
			
			if(slaOptions != null) {
				ret.put("slaEmails", slaOptions.getSlaEmails());
				List<SlaSetting> settings = slaOptions.getSettings();
				List<Object> setObj = new ArrayList<Object>();
				for(SlaSetting set: settings) {
					setObj.add(set.toObject());
				}
				ret.put("settings", setObj);
			}
			else if (flowOptions != null) {
				if(flowOptions.getFailureEmails() != null) {
					List<String> emails = flowOptions.getFailureEmails();
					if(emails.size() > 0) {
						ret.put("slaEmails", emails);
					}
				}
			}
			else {
				if(flow.getFailureEmails() != null) {
					List<String> emails = flow.getFailureEmails();
					if(emails.size() > 0) {
						ret.put("slaEmails", emails);
					}
				}
			}
			
			List<String> disabledJobs;
			if(flowOptions != null) {
				disabledJobs = flowOptions.getDisabledJobs() == null ? new ArrayList<String>() : flowOptions.getDisabledJobs();
			}
			else {
				disabledJobs = new ArrayList<String>();
			}
				
			List<String> allJobs = new ArrayList<String>();
			for(Node n : flow.getNodes()) {
				if(!disabledJobs.contains(n.getId())) {
					allJobs.add(n.getId());
				}
			}
			ret.put("allJobNames", allJobs);
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
//		
//		List<SLA> slas = slaManager.getSLAs();
//		page.add("slas", slas);

		page.render();
	}
	
	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
		else {
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
	}
	
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

		DateTime submitTime = new DateTime();
		DateTime firstSchedTime = day.withHourOfDay(hour).withMinuteOfHour(minutes).withSecondOfMinute(0);
		
		Schedule sched = scheduleManager.getSchedule(new Pair<Integer, String>(projectId, flowName));
		FlowOptions flowOptions = null;
		SlaOptions slaOptions = null;
		if(sched != null) {
			if(sched.getFlowOptions() != null) {
				flowOptions = sched.getFlowOptions();
			}
			if(sched.getSlaOptions() != null) {
				slaOptions = sched.getSlaOptions();
			}
		}
		Schedule schedule = scheduleManager.scheduleFlow(projectId, projectName, flowName, "ready", firstSchedTime.getMillis(), timezone, thePeriod, submitTime.getMillis(), firstSchedTime.getMillis(), firstSchedTime.getMillis(), user.getUserId(), flowOptions, slaOptions);
		logger.info("User '" + user.getUserId() + "' has scheduled " + "[" + projectName + flowName +  " (" + projectId +")" + "].");
		projectManager.postProjectEvent(project, EventType.SCHEDULE, user.getUserId(), "Schedule " + schedule.toString() + " has been added.");
		
		ret.put("status", "success");
		ret.put("message", projectName + "." + flowName + " scheduled.");
	}

	private void ajaxAdvSchedule(HttpServletRequest req, HashMap<String, Object> ret, User user) throws ServletException {
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
		
		String scheduleTime = getParam(req, "scheduleTime");
		String scheduleDate = getParam(req, "scheduleDate");
		DateTime firstSchedTime;
		try {
			firstSchedTime = parseDateTime(scheduleDate, scheduleTime);
		}
		catch (Exception e) {
			ret.put("error", "Invalid date and/or time '" + scheduleDate + " " + scheduleTime);
	      	return;
		}

		ReadablePeriod thePeriod = null;
		try {
			if(hasParam(req, "is_recurring") && getParam(req, "is_recurring").equals("on")) {
			    thePeriod = Schedule.parsePeriodString(getParam(req, "period"));	
			}
		}
		catch(Exception e){
			ret.put("error", e.getMessage());
		}
		
		Schedule sched = scheduleManager.getSchedule(new Pair<Integer, String>(projectId, flowName));
		FlowOptions flowOptions = null;
		try {
			flowOptions = parseFlowOptions(req);
		}
		catch (Exception e) {
			ret.put("error", e.getMessage());
		}
		SlaOptions slaOptions = null;
		if(sched != null) {
			if(sched.getSlaOptions() != null) {
				slaOptions = sched.getSlaOptions();
			}
		}
		Schedule schedule = scheduleManager.scheduleFlow(projectId, projectName, flowName, "ready", firstSchedTime.getMillis(), firstSchedTime.getZone(), thePeriod, DateTime.now().getMillis(), firstSchedTime.getMillis(), firstSchedTime.getMillis(), user.getUserId(), flowOptions, slaOptions);
		logger.info("User '" + user.getUserId() + "' has scheduled " + "[" + projectName + flowName +  " (" + projectId +")" + "].");
		projectManager.postProjectEvent(project, EventType.SCHEDULE, user.getUserId(), "Schedule " + schedule.toString() + " has been added.");

		ret.put("status", "success");
		ret.put("message", projectName + "." + flowName + " scheduled.");
	}
	
	private FlowOptions parseFlowOptions(HttpServletRequest req) throws ServletException {
		FlowOptions flowOptions = new FlowOptions();
		if (hasParam(req, "failureAction")) {
			String option = getParam(req, "failureAction");
			if (option.equals("finishCurrent") ) {
				flowOptions.setFailureAction(FailureAction.FINISH_CURRENTLY_RUNNING);
			}
			else if (option.equals("cancelImmediately")) {
				flowOptions.setFailureAction(FailureAction.CANCEL_ALL);
			}
			else if (option.equals("finishPossible")) {
				flowOptions.setFailureAction(FailureAction.FINISH_ALL_POSSIBLE);
			}
		}

		if (hasParam(req, "failureEmails")) {
			String emails = getParam(req, "failureEmails");
			String[] emailSplit = emails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
			flowOptions.setFailureEmails(Arrays.asList(emailSplit));
		}
		if (hasParam(req, "successEmails")) {
			String emails = getParam(req, "successEmails");
			String[] emailSplit = emails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
			flowOptions.setSuccessEmails(Arrays.asList(emailSplit));
		}
		if (hasParam(req, "notifyFailureFirst")) {
			flowOptions.setNotifyOnFirstFailure(Boolean.parseBoolean(getParam(req, "notifyFailureFirst")));
		}
		if (hasParam(req, "notifyFailureLast")) {
			flowOptions.setNotifyOnLastFailure(Boolean.parseBoolean(getParam(req, "notifyFailureLast")));
		}
		if (hasParam(req, "executingJobOption")) {
			//String option = getParam(req, "jobOption");
			// Not set yet
		}
		
		Map<String, String> flowParamGroup = this.getParamGroup(req, "flowOverride");
		flowOptions.setFlowOverride(flowParamGroup);
		
		if (hasParam(req, "disabledJobs")) {
			String disable = getParam(req, "disabledJobs");
			String[] disableSplit = disable.split("\\s*,\\s*|\\s*;\\s*|\\s+");
			List<String> jobs = (List<String>) Arrays.asList(disableSplit);
			flowOptions.setDisabledJobs(jobs.subList(1, jobs.size()));
		}
		return flowOptions;
	}

	private DateTime parseDateTime(String scheduleDate, String scheduleTime) {
		// scheduleTime: 12,00,pm,PDT
		String[] parts = scheduleTime.split(",", -1);
		int hour = Integer.parseInt(parts[0]);
		int minutes = Integer.parseInt(parts[1]);
		boolean isPm = parts[2].equalsIgnoreCase("pm");
		
		DateTimeZone timezone = parts[3].equals("UTC") ? DateTimeZone.UTC : DateTimeZone.forID("America/Los_Angeles");

		// scheduleDate: 02/10/2013
		DateTime day = null;
		if(scheduleDate == null || scheduleDate.trim().length() == 0) {
			day = new LocalDateTime().toDateTime();
		} else {
			day = DateTimeFormat.forPattern("MM/dd/yyyy").withZone(timezone).parseDateTime(scheduleDate);
		}
		
		if(isPm && hour < 12)
		    hour += 12;
		hour %= 24;

		DateTime firstSchedTime = day.withHourOfDay(hour).withMinuteOfHour(minutes).withSecondOfMinute(0);

		return firstSchedTime;
	}

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
