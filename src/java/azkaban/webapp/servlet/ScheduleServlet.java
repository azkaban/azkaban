package azkaban.webapp.servlet;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;

import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.user.User;
import azkaban.user.Permission.Type;
import azkaban.webapp.session.Session;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduledFlow;

public class ScheduleServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	private ProjectManager projectManager;
	private ScheduleManager scheduleManager;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		projectManager = this.getApplication().getProjectManager();
		scheduleManager = this.getApplication().getScheduleManager();
	}
	
	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/scheduledflowpage.vm");
		
		List<ScheduledFlow> schedules = scheduleManager.getSchedule();
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
		String scheduleId = getParam(req, "scheduleId");
		ScheduledFlow schedFlow = scheduleManager.getSchedule(scheduleId);
		String projectId = schedFlow.getProjectId();
		Project project = projectManager.getProject(projectId);
		
		if (project == null) {
			ret.put("message", "Project " + projectId + " does not exist");
			ret.put("status", "error");
			return;
		}
		
		if (!project.hasPermission(user, Type.SCHEDULE)) {
			ret.put("status", "error");
			ret.put("message", "Permission denied. Cannot remove schedule " + scheduleId);
			return;
		}
		
		scheduleManager.removeScheduledFlow(scheduleId);
		project.info("User '" + user.getUserId() + " has removed schedule " + schedFlow.toNiceString());

		ret.put("status", "success");
		ret.put("message", scheduleId + " removed.");
		return;
	}
	
	private void ajaxScheduleFlow(HttpServletRequest req, Map<String, Object> ret, User user) throws ServletException {
		String projectId = getParam(req, "projectId");
		String flowId = getParam(req, "flowId");
		
		Project project = projectManager.getProject(projectId);
			
		if (project == null) {
			ret.put("message", "Project " + projectId + " does not exist");
			ret.put("status", "error");
			return;
		}
		
		if (!project.hasPermission(user, Type.SCHEDULE)) {
			ret.put("status", "error");
			ret.put("message", "Permission denied. Cannot execute " + flowId);
			return;
		}

		Flow flow = project.getFlow(flowId);
		if (flow == null) {
			ret.put("status", "error");
			ret.put("message", "Flow " + flowId + " cannot be found in project " + project);
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
			    thePeriod = parsePeriod(req);	
		}
		catch(Exception e){
			ret.put("error", e.getMessage());
		}

		if(isPm && hour < 12)
		    hour += 12;
		hour %= 24;

		String userSubmit = user.getUserId();
		String userExec = userSubmit;//getParam(req, "userExec");
		String scheduleId = projectId + "." + flowId;
		DateTime submitTime = new DateTime().withZone(timezone);
		DateTime firstSchedTime = day.withHourOfDay(hour).withMinuteOfHour(minutes).withSecondOfMinute(0);
		
		ScheduledFlow schedFlow = scheduleManager.schedule(scheduleId, projectId, flowId, userExec, userSubmit, submitTime, firstSchedTime, thePeriod);
		project.info("User '" + user.getUserId() + "' has scheduled " + flow.getId() + "[" + schedFlow.toNiceString() + "].");
		
		ret.put("status", "success");
		ret.put("message", scheduleId + " scheduled.");
	}
				
	private ReadablePeriod parsePeriod(HttpServletRequest req) throws ServletException {
			int period = getIntParam(req, "period");
			String periodUnits = getParam(req, "period_units");
			if("M".equals(periodUnits))
				return Months.months(period);
			else if("w".equals(periodUnits))
				return Weeks.weeks(period);
			else if("d".equals(periodUnits))
				return Days.days(period);
			else if("h".equals(periodUnits))
				return Hours.hours(period);
			else if("m".equals(periodUnits))
				return Minutes.minutes(period);
			else if("s".equals(periodUnits))
				return Seconds.seconds(period);
			else
				throw new ServletException("Unknown period unit: " + periodUnits);
	}

}
