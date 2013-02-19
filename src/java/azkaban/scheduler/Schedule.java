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

package azkaban.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.Weeks;

import azkaban.executor.ExecutableFlow.FailureAction;
import azkaban.scheduler.Schedule.FlowOptions;
import azkaban.scheduler.Schedule.SlaOptions;
import azkaban.sla.SLA.SlaSetting;
import azkaban.utils.Pair;

public class Schedule{
	
	
	public static class FlowOptions {
		
		public List<String> getFailureEmails() {
			return failureEmails;
		}
		public void setFailureEmails(List<String> failureEmails) {
			this.failureEmails = failureEmails;
		}
		public List<String> getSuccessEmails() {
			return successEmails;
		}
		public void setSuccessEmails(List<String> successEmails) {
			this.successEmails = successEmails;
		}
		public FailureAction getFailureAction() {
			return failureAction;
		}
		public void setFailureAction(FailureAction failureAction) {
			this.failureAction = failureAction;
		}
		public boolean isnotifyOnFirstFailure() {
			return notifyOnFirstFailure;
		}
		public void setNotifyOnFirstFailure(boolean notifyOnFirstFailure) {
			this.notifyOnFirstFailure = notifyOnFirstFailure;
		}
		public boolean isnotifyOnLastFailure() {
			return notifyOnLastFailure;
		}
		public void setNotifyOnLastFailure(boolean notifyOnLastFailure) {
			this.notifyOnLastFailure = notifyOnLastFailure;
		}
		public Map<String, String> getFlowOverride() {
			return flowOverride;
		}
		public void setFlowOverride(Map<String, String> flowOverride) {
			this.flowOverride = flowOverride;
		}
		public List<String> getDisabledJobs() {
			return disabledJobs;
		}
		public void setDisabledJobs(List<String> disabledJobs) {
			this.disabledJobs = disabledJobs;
		}
		private List<String> failureEmails;
		private List<String> successEmails;
		private FailureAction failureAction = FailureAction.FINISH_CURRENTLY_RUNNING;
		private boolean notifyOnFirstFailure;
		private boolean notifyOnLastFailure;
		Map<String, String> flowOverride;
		private List<String> disabledJobs;
		public Object toObject() {
			Map<String, Object> obj = new HashMap<String, Object>();
			obj.put("failureEmails", failureEmails);
			obj.put("successEmails", successEmails);
			obj.put("failureAction", failureAction.toString());
			obj.put("notifyOnFirstFailure", notifyOnFirstFailure);
			obj.put("notifyOnLastFailure", notifyOnLastFailure);
			obj.put("flowOverride", flowOverride);
			obj.put("disabledJobs", disabledJobs);
			return obj;
		}
		@SuppressWarnings("unchecked")
		public static FlowOptions fromObject(Object object) {
			if(object != null) {
				FlowOptions flowOptions = new FlowOptions();
				Map<String, Object> obj = (HashMap<String, Object>) object;
				if(obj.containsKey("failureEmails")) {
					flowOptions.setFailureEmails((List<String>) obj.get("failureEmails"));
				}
				if(obj.containsKey("successEmails")) {
					flowOptions.setSuccessEmails((List<String>) obj.get("SuccessEmails"));
				}
				if(obj.containsKey("failureAction")) {
					flowOptions.setFailureAction(FailureAction.valueOf((String)obj.get("failureAction")));
				}
				if(obj.containsKey("notifyOnFirstFailure")) {
					flowOptions.setNotifyOnFirstFailure((Boolean)obj.get("notifyOnFirstFailure"));
				}
				if(obj.containsKey("notifyOnLastFailure")) {
					flowOptions.setNotifyOnFirstFailure((Boolean)obj.get("notifyOnLastFailure"));
				}
				if(obj.containsKey("flowOverride")) {
					flowOptions.setFlowOverride((Map<String, String>) obj.get("flowOverride"));
				}
				if(obj.containsKey("disabledJobs")) {
					flowOptions.setDisabledJobs((List<String>) obj.get("disabledJobs"));
				}
				return flowOptions;
			}
			return null;
		}
	}

	public static class SlaOptions {

		public List<String> getSlaEmails() {
			return slaEmails;
		}
		public void setSlaEmails(List<String> slaEmails) {
			this.slaEmails = slaEmails;
		}
		public List<SlaSetting> getSettings() {
			return settings;
		}
		public void setSettings(List<SlaSetting> settings) {
			this.settings = settings;
		}
		private List<String> slaEmails;
		private List<SlaSetting> settings;
		public Object toObject() {
			Map<String, Object> obj = new HashMap<String, Object>();
			obj.put("slaEmails", slaEmails);
			List<Object> slaSettings = new ArrayList<Object>();
			for(SlaSetting s : settings) {
				slaSettings.add(s.toObject());
			}
			obj.put("settings", slaSettings);
			return obj;
		}
		@SuppressWarnings("unchecked")
		public static SlaOptions fromObject(Object object) {
			if(object != null) {
				SlaOptions slaOptions = new SlaOptions();
				Map<String, Object> obj = (HashMap<String, Object>) object;
				slaOptions.setSlaEmails((List<String>) obj.get("slaEmails"));
				List<SlaSetting> slaSets = new ArrayList<SlaSetting>();
				for(Object set: (List<Object>)obj.get("settings")) {
					slaSets.add(SlaSetting.fromObject(set));
				}
				slaOptions.setSettings(slaSets);
				return slaOptions;
			}
			return null;			
		}
		
	}
	
//	private long projectGuid;
//	private long flowGuid;
	
//	private String scheduleId;
	
	private int projectId;
	private String projectName;
	private String flowName;
	private long firstSchedTime;
	private DateTimeZone timezone;
	private long lastModifyTime;
	private ReadablePeriod period;
	private long nextExecTime;
	private String submitUser;
	private String status;
	private long submitTime;
	
	private FlowOptions flowOptions;
	private SlaOptions slaOptions;
	
	public Schedule(
						int projectId,
						String projectName,
						String flowName,
						String status,
						long firstSchedTime,
						DateTimeZone timezone,
						ReadablePeriod period,
						long lastModifyTime,						
						long nextExecTime,						
						long submitTime,
						String submitUser
						) {
		this.projectId = projectId;
		this.projectName = projectName;
		this.flowName = flowName;
		this.firstSchedTime = firstSchedTime;
		this.timezone = timezone;
		this.lastModifyTime = lastModifyTime;
		this.period = period;
		this.nextExecTime = nextExecTime;
		this.submitUser = submitUser;
		this.status = status;
		this.submitTime = submitTime;
		this.flowOptions = null;
		this.slaOptions = null;
	}

	public Schedule(
						int projectId,
						String projectName,
						String flowName,
						String status,
						long firstSchedTime,
						String timezoneId,
						String period,
						long lastModifyTime,						
						long nextExecTime,						
						long submitTime,
						String submitUser,
						FlowOptions flowOptions,
						SlaOptions slaOptions
			) {
		this.projectId = projectId;
		this.projectName = projectName;
		this.flowName = flowName;
		this.firstSchedTime = firstSchedTime;
		this.timezone = DateTimeZone.forID(timezoneId);
		this.lastModifyTime = lastModifyTime;
		this.period = parsePeriodString(period);
		this.nextExecTime = nextExecTime;
		this.submitUser = submitUser;
		this.status = status;
		this.submitTime = submitTime;
		this.flowOptions = flowOptions;
		this.slaOptions = slaOptions;
	}

	public Schedule(
						int projectId,
						String projectName,
						String flowName,
						String status,
						long firstSchedTime,
						DateTimeZone timezone,
						ReadablePeriod period,
						long lastModifyTime,						
						long nextExecTime,						
						long submitTime,
						String submitUser,
						FlowOptions flowOptions,
						SlaOptions slaOptions
						) {
		this.projectId = projectId;
		this.projectName = projectName;
		this.flowName = flowName;
		this.firstSchedTime = firstSchedTime;
		this.timezone = timezone;
		this.lastModifyTime = lastModifyTime;
		this.period = period;
		this.nextExecTime = nextExecTime;
		this.submitUser = submitUser;
		this.status = status;
		this.submitTime = submitTime;
		this.flowOptions = flowOptions;
		this.slaOptions = slaOptions;
	}

	public FlowOptions getFlowOptions() {
		return flowOptions;
	}

	public void setFlowOptions(FlowOptions flowOptions) {
		this.flowOptions = flowOptions;
	}

	public SlaOptions getSlaOptions() {
		return slaOptions;
	}

	public void setSlaOptions(SlaOptions slaOptions) {
		this.slaOptions = slaOptions;
	}

	public String getScheduleName() {
		return projectName + "." + flowName + " (" + projectId + ")";
	}
	
	public String toString() {
		return projectName + "." + flowName + " (" + projectId + ")" + " to be run at (starting) " + 
				new DateTime(firstSchedTime).toDateTimeISO() + " with recurring period of " + (period == null ? "non-recurring" : createPeriodString(period));
	}
	
	public Pair<Integer, String> getScheduleId() {
		return new Pair<Integer, String>(getProjectId(), getFlowName());
	}
	
	public int getProjectId() {
		return projectId;
	}

	public String getProjectName() {
		return projectName;
	}

	public String getFlowName() {
		return flowName;
	}

	public long getFirstSchedTime() {
		return firstSchedTime;
	}

	public DateTimeZone getTimezone() {
		return timezone;
	}

	public long getLastModifyTime() {
		return lastModifyTime;
	}

	public ReadablePeriod getPeriod() {
		return period;
	}

	public long getNextExecTime() {
		return nextExecTime;
	}

	public String getSubmitUser() {
		return submitUser;
	}

	public String getStatus() {
		return status;
	}

	public long getSubmitTime() {
		return submitTime;
	}

	public boolean updateTime() {
		if (new DateTime(nextExecTime).isAfterNow()) {
			return true;
		}

		if (period != null) {
			DateTime nextTime = getNextRuntime(nextExecTime, timezone, period);

			this.nextExecTime = nextTime.getMillis();
			return true;
		}

		return false;
	}
	
	private DateTime getNextRuntime(long scheduleTime, DateTimeZone timezone, ReadablePeriod period) {
		DateTime now = new DateTime();
		DateTime date = new DateTime(scheduleTime).withZone(timezone);
		int count = 0;
		while (!now.isBefore(date)) {
			if (count > 100000) {
				throw new IllegalStateException(
						"100000 increments of period did not get to present time.");
			}

			if (period == null) {
				break;
			} else {
				date = date.plus(period);
			}

			count += 1;
		}

		return date;
	}

	public static ReadablePeriod parsePeriodString(String periodStr) {
		ReadablePeriod period;
		char periodUnit = periodStr.charAt(periodStr.length() - 1);
		if (periodUnit == 'n') {
			return null;
		}

		int periodInt = Integer.parseInt(periodStr.substring(0,
				periodStr.length() - 1));
		switch (periodUnit) {
		case 'M':
			period = Months.months(periodInt);
			break;
		case 'w':
			period = Weeks.weeks(periodInt);
			break;
		case 'd':
			period = Days.days(periodInt);
			break;
		case 'h':
			period = Hours.hours(periodInt);
			break;
		case 'm':
			period = Minutes.minutes(periodInt);
			break;
		case 's':
			period = Seconds.seconds(periodInt);
			break;
		default:
			throw new IllegalArgumentException("Invalid schedule period unit '"
					+ periodUnit);
		}

		return period;
	}

	public static String createPeriodString(ReadablePeriod period) {
		String periodStr = "n";

		if (period == null) {
			return "n";
		}

		if (period.get(DurationFieldType.months()) > 0) {
			int months = period.get(DurationFieldType.months());
			periodStr = months + "M";
		} else if (period.get(DurationFieldType.weeks()) > 0) {
			int weeks = period.get(DurationFieldType.weeks());
			periodStr = weeks + "w";
		} else if (period.get(DurationFieldType.days()) > 0) {
			int days = period.get(DurationFieldType.days());
			periodStr = days + "d";
		} else if (period.get(DurationFieldType.hours()) > 0) {
			int hours = period.get(DurationFieldType.hours());
			periodStr = hours + "h";
		} else if (period.get(DurationFieldType.minutes()) > 0) {
			int minutes = period.get(DurationFieldType.minutes());
			periodStr = minutes + "m";
		} else if (period.get(DurationFieldType.seconds()) > 0) {
			int seconds = period.get(DurationFieldType.seconds());
			periodStr = seconds + "s";
		}

		return periodStr;
	}
	

	public Map<String,Object> optionsToObject() {
		if(flowOptions != null || slaOptions != null) {
			HashMap<String, Object> schedObj = new HashMap<String, Object>();
			
			if(flowOptions != null) {
				schedObj.put("flowOptions", flowOptions.toObject());
			}
			if(slaOptions != null) {
				schedObj.put("slaOptions", slaOptions.toObject());
			}
	
			return schedObj;
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public static FlowOptions createFlowOptionFromObject(Object obj) {
		if(obj != null) {
			Map<String, Object> options = (HashMap<String, Object>) obj;
			if(options.containsKey("flowOptions")) {
				return FlowOptions.fromObject(options.get("flowOptions"));
			}
		}		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public static SlaOptions createSlaOptionFromObject(Object obj) {
		if(obj != null) {
			Map<String, Object> options = (HashMap<String, Object>) obj;
			if(options.containsKey("slaOptions")) {
				return SlaOptions.fromObject(options.get("slaOptions"));
			}
		}		
		return null;
	}

}