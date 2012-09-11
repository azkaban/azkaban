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
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;

import azkaban.executor.ExecutorManager.ExecutionReference;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.user.User;
import azkaban.user.Permission.Type;
import azkaban.webapp.servlet.HistoryServlet.PageSelection;
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

//	public class PageSelection {
//		private int page;
//		private int size;
//		private boolean disabled;
//		private boolean selected;
//		
//		public PageSelection(int page, int size, boolean disabled, boolean selected) {
//			this.page = page;
//			this.size = size;
//			this.disabled = disabled;
//			this.setSelected(selected);
//		}
//		
//		public int getPage() {
//			return page;
//		}
//		
//		public int getSize() {
//			return size;
//		}
//		
//		public boolean getDisabled() {
//			return disabled;
//		}
//
//		public boolean isSelected() {
//			return selected;
//		}
//
//		public void setSelected(boolean selected) {
//			this.selected = selected;
//		}
//	}
	
	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/scheduledflowpage.vm");
//		int pageNum = getIntParam(req, "page", 1);
//		int pageSize = getIntParam(req, "size", 16);
		
//		if (pageNum < 0) {
//			pageNum = 1;
//		}
		
		List<ScheduledFlow> schedules = scheduleManager.getSchedule();
		page.add("schedules", schedules);
//		page.add("size", pageSize);
//		page.add("page", pageNum);
		
//		if (pageNum == 1) {
//			page.add("previous", new PageSelection(1, pageSize, true, false));
//		}
//		page.add("next", new PageSelection(pageNum + 1, pageSize, false, false));
		// Now for the 5 other values.
//		int pageStartValue = 1;
//		if (pageNum > 3) {
//			pageStartValue = pageNum - 2;
//		}
//		
//		page.add("page1", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
//		pageStartValue++;
//		page.add("page2", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
//		pageStartValue++;
//		page.add("page3", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
//		pageStartValue++;
//		page.add("page4", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
//		pageStartValue++;
//		page.add("page5", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
//		pageStartValue++;
//		
		page.render();
	}
	
//	@Override
//	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
//		if (hasParam(req, "ajax")) {
//			handleAJAXAction(req, resp, session);
//		}
////		else if (hasParam(req, "execid")) {
////			handleExecutionFlowPage(req, resp, session);
////		}
//	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		if (hasParam(req, "action")) {
			String action = getParam(req, "action");
			if (action.equals("scheduleFlow")) {
				ajaxScheduleFlow(req, ret, session.getUser());
			}
		}
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


		String scheduledDate = req.getParameter("date");
		DateTime day = null;
		if(scheduledDate == null || scheduledDate.trim().length() == 0) {
			day = new LocalDateTime().toDateTime();
		} else {
		    try {
		    	day = DateTimeFormat.forPattern("MM/dd/yyyy").parseDateTime(scheduledDate);
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
		DateTime submitTime = new DateTime();
		DateTime firstSchedTime = day.withHourOfDay(hour).withMinuteOfHour(minutes).withSecondOfMinute(0);
		
		scheduleManager.schedule(scheduleId,userExec, userSubmit, submitTime, firstSchedTime, thePeriod);
		
		

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
