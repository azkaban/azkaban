/*
 * Copyright 2010 LinkedIn, Inc
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

import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.utils.Utils;

/**
 * Schedule for a job instance. This is decoupled from the execution.
 * 
 * @author Richard Park
 * 
 */
public class ScheduledFlow {

	// use projectId.flowId to form a unique scheduleId
	private final String scheduleId;
	private final String flowId;
	private final String projectId;

	private final ReadablePeriod period;
	private DateTime nextExecTime;
	private final String user;
	private final String userSubmit;
	private final DateTime submitTime;
	private final DateTime firstSchedTime;

	public static final String DATE_TIME_STRING = "YYYY-MM-dd HH:mm:ss";
	
	
	// private SchedStatus schedStatus;
	

	public enum SchedStatus {
		LASTSUCCESS("lastsuccess"), LASTFAILED("lastfailed"), LASTPAUSED("lastpaused");

		private final String status;

		SchedStatus(String status) {
			this.status = status;
		}

		private String status() {
			return this.status;
		}
	}

	/**
	 * Constructor
	 * 
	 * @param jobId
	 * @param nextExecution
	 * @param period
	 * @param ignoreDependency
	 */
	public ScheduledFlow(
			String scheduleId, 
			String projectId, 
			String flowId, 
			String user, 
			String userSubmit,
			DateTime submitTime, 
			DateTime firstSchedTime,
			DateTime nextExecution, 
			ReadablePeriod period) 
	{
		super();
		this.scheduleId = Utils.nonNull(scheduleId);
		this.projectId = Utils.nonNull(projectId);
		this.flowId = Utils.nonNull(flowId);
		this.user = user;
		this.userSubmit = userSubmit;
		this.submitTime = submitTime;
		this.firstSchedTime = firstSchedTime;
		this.period = period;
		this.nextExecTime = Utils.nonNull(nextExecution);
		// this.schedStatus = SchedStatus.LASTSUCCESS;
	}

	public ScheduledFlow(
			String scheduleId, 
			String projectId, 
			String flowId, 
			String user, 
			String userSubmit,
			DateTime submitTime, 
			DateTime firstSchedTime,
			ReadablePeriod period)
	{
		this(scheduleId, projectId, flowId, user, userSubmit, submitTime, firstSchedTime, new DateTime().withZone(firstSchedTime.getZone()), period);
	}

	public ScheduledFlow(
			String scheduleId, 
			String projectId, 
			String flowId, 
			String user, 
			String userSubmit,
			DateTime submitTime, 
			DateTime firstSchedTime) 
	{
		this(scheduleId, projectId, flowId, user, userSubmit, submitTime, firstSchedTime, new DateTime().withZone(firstSchedTime.getZone()), null);
	}

	/**
	 * Constructor
	 * 
	 * @param jobName
	 *            Unique job name
	 * @param nextExecution
	 *            The next execution time
	 * @param ignoreDependency
	 */
	public ScheduledFlow(String scheduleId, 
						String projectId,
						String flowId,
						String user, 
						String userSubmit,
						DateTime submitTime, 
						DateTime firstSchedTime, 
						DateTime nextExecution)
	{
		this(scheduleId, projectId, flowId, user, userSubmit, submitTime, firstSchedTime, nextExecution, null);
	}
	
	// public SchedStatus getSchedStatus() {
	// return this.schedStatus;
	// }
	//
	// public void setSchedStatus(SchedStatus schedStatus) {
	// this.schedStatus = schedStatus;
	// }

	/**
	 * Updates the time to a future time after 'now' that matches the period
	 * description.
	 * 
	 * @return
	 */
	public boolean updateTime() {
		if (nextExecTime.isAfterNow()) {
			return true;
		}

		if (period != null) {
			DateTime other = getNextRuntime(nextExecTime, period);

			this.nextExecTime = other;
			return true;
		}

		return false;
	}

	/**
	 * Calculates the next runtime by adding the period.
	 * 
	 * @param scheduledDate
	 * @param period
	 * @return
	 */
	private DateTime getNextRuntime(DateTime scheduledDate, ReadablePeriod period) {
		DateTime now = new DateTime();
		DateTime date = new DateTime(scheduledDate);
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

	/**
	 * Returns the unique id of the job to be run.
	 * 
	 * @return
	 */

	/**
	 * Returns true if the job recurrs in the future
	 * 
	 * @return
	 */
	public boolean isRecurring() {
		return this.period != null;
	}

	/**
	 * Returns the recurrance period. Or null if not applicable
	 * 
	 * @return
	 */
	public ReadablePeriod getPeriod() {
		return period;
	}

	public DateTime getFirstSchedTime() {
		return firstSchedTime;
	}

	/**
	 * Returns the next scheduled execution
	 * 
	 * @return
	 */
	public DateTime getNextExecTime() {
		return nextExecTime;
	}

	public String getUserSubmit() {
		return userSubmit;
	}

	public DateTime getSubmitTime() {
		return submitTime;
	}

	@Override
	public String toString() {
		return "ScheduledFlow{"
				+
				// "scheduleStatus=" + schedStatus +
				"nextExecTime=" + nextExecTime + ", period=" + period
				+ ", firstSchedTime=" + firstSchedTime + ", submitTime="
				+ submitTime + ", userSubmit=" + userSubmit + ", user=" + user
				+ ", scheduleId='" + scheduleId + '\'' + '}';
	}

	public String toNiceString() {
		return scheduleId + "," + submitTime + "," + period;
	}
	
	public String getUser() {
		return user;
	}

	public String getScheduleId() {
		return scheduleId;
	}

	public String getFlowId() {
		return flowId;
	}

	public String getProjectId() {
		return projectId;
	}
}
