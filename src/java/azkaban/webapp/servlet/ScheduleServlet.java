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
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;

import azkaban.flow.Flow;
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
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/scheduledflowpage.vm");
		
		List<Schedule> schedules = scheduleManager.getSchedules();
		page.add("schedules", schedules);

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
		Pair scheduleId = new Pair(projectId, flowName);
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

		String submitUser = user.getUserId();
//		String userExec = userSubmit;//getParam(req, "userExec");
//		String scheduleId = projectId + "." + flowName;
		DateTime submitTime = new DateTime();
		DateTime firstSchedTime = day.withHourOfDay(hour).withMinuteOfHour(minutes).withSecondOfMinute(0);
		
		//ScheduledFlow schedFlow = scheduleManager.schedule(scheduleId, projectId, flowId, userExec, userSubmit, submitTime, firstSchedTime, thePeriod);
		//project.info("User '" + user.getUserId() + "' has scheduled " + flow.getId() + "[" + schedFlow.toNiceString() + "].");
		Schedule schedule = scheduleManager.scheduleFlow(projectId, projectName, flowName, "ready", firstSchedTime.getMillis(), timezone, thePeriod, submitTime.getMillis(), firstSchedTime.getMillis(), firstSchedTime.getMillis(), user.getUserId());
		logger.info("User '" + user.getUserId() + "' has scheduled " + "[" + projectName + flowName +  " (" + projectId +")" + "].");
		projectManager.postProjectEvent(project, EventType.SCHEDULE, user.getUserId(), "Schedule " + schedule.toString() + " has been added.");
		
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
			if (role.getPermission().isPermissionSet(type)) {
				return true;
			}
		}
		
		return true;
	}
}
