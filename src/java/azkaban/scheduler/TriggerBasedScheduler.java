package azkaban.scheduler;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.actions.ExecuteFlowAction;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManager;
import azkaban.project.ProjectManager;
import azkaban.sla.SLAManager;
import azkaban.sla.SlaOptions;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import azkaban.utils.Pair;

/**
 * The ScheduleManager stores and executes the schedule. It uses a single thread
 * instead and waits until correct loading time for the flow. It will not remove
 * the flow from the schedule when it is run, which can potentially allow the
 * flow to and overlap each other.
 */
public class TriggerBasedScheduler {
	private static Logger logger = Logger.getLogger(TriggerBasedScheduler.class);

	private final DateTimeFormatter _dateFormat = DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss:SSS");
	private ScheduleLoader loader;

	private Map<Pair<Integer, String>, Schedule> scheduleIDMap = new HashMap<Pair<Integer, String>, Schedule>();
	private Map<Integer, Schedule> idFlowMap = new HashMap<Integer, Schedule>();
	
	/**
	 * Give the schedule manager a loader class that will properly load the
	 * schedule.
	 * 
	 * @param loader
	 */
	public TriggerBasedScheduler(ExecutorManager executorManager,
							ProjectManager projectManager, 
							TriggerManager triggerManager,
							ScheduleLoader loader) 
	{
		this.loader = new TriggerBasedScheduleLoader(triggerManager, executorManager, projectManager, ScheduleManager.triggerSource);

		List<Schedule> scheduleList = null;
		try {
			scheduleList = loader.loadSchedules();
		} catch (ScheduleManagerException e) {
			// TODO Auto-generated catch block
			logger.error("Failed to load schedules" + e.getCause() + e.getMessage());
			e.printStackTrace();
		}
		for(Schedule s : scheduleList) {
			scheduleIDMap.put(s.getScheduleIdentityPair(), s);
			idFlowMap.put(s.getScheduleId(), s);
		}
	}

	/**
	 * Retrieves a copy of the list of schedules.
	 * 
	 * @return
	 */
	public synchronized List<Schedule> getSchedules() {
		return new ArrayList<Schedule>(scheduleIDMap.values());
	}

	/**
	 * Returns the scheduled flow for the flow name
	 * 
	 * @param id
	 * @return
	*/
	public Schedule getSchedule(int projectId, String flowId) {
		return scheduleIDMap.get(new Pair<Integer,String>(projectId, flowId));
	}

	/**
	 * Removes the flow from the schedule if it exists.
	 * 
	 * @param id
	 */
	public synchronized void removeSchedule(int projectId, String flowId) {
		Schedule s = getSchedule(projectId, flowId);
		if(s != null) {
			removeSchedule(s);
		}
	}
	
	/**
	 * Removes the flow from the schedule if it exists.
	 * 
	 * @param id
	 */
	public synchronized void removeSchedule(Schedule sched) {

		Pair<Integer,String> identityPairMap = sched.getScheduleIdentityPair();
		Schedule s = scheduleIDMap.get(identityPairMap);
		if(s != null) {
			scheduleIDMap.remove(sched.getScheduleId());
		}
		
		try {
			loader.removeSchedule(sched);
		} catch (ScheduleManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Schedule scheduleFlow(
			final int scheduleId,
			final int projectId,
			final String projectName,
			final String flowName,
			final String status,
			final long firstSchedTime,
			final DateTimeZone timezone,
			final ReadablePeriod period,
			final long lastModifyTime,
			final long nextExecTime,
			final long submitTime,
			final String submitUser
			) {
		return scheduleFlow(scheduleId, projectId, projectName, flowName, status, firstSchedTime, timezone, period, lastModifyTime, nextExecTime, submitTime, submitUser, null, null);
	}
	
	public Schedule scheduleFlow(
			final int scheduleId,
			final int projectId,
			final String projectName,
			final String flowName,
			final String status,
			final long firstSchedTime,
			final DateTimeZone timezone,
			final ReadablePeriod period,
			final long lastModifyTime,
			final long nextExecTime,
			final long submitTime,
			final String submitUser,
			ExecutionOptions execOptions,
			SlaOptions slaOptions
			) {
		Schedule sched = new Schedule(scheduleId, projectId, projectName, flowName, status, firstSchedTime, timezone, period, lastModifyTime, nextExecTime, submitTime, submitUser, execOptions, slaOptions);
		logger.info("Scheduling flow '" + sched.getScheduleName() + "' for "
				+ _dateFormat.print(firstSchedTime) + " with a period of "
				+ period == null ? "(non-recurring)" : period);

		insertSchedule(sched);
		return sched;
	}


	/**
	 * Adds a flow to the schedule.
	 * 
	 * @param flow
	 */
	public synchronized void insertSchedule(Schedule s) {
		boolean exist = s.getScheduleId() != -1;
		if(s.updateTime()) {
			try {
				if(!exist) {
					loader.insertSchedule(s);
					scheduleIDMap.put(s.getScheduleIdentityPair(), s);
				}
				else{
					loader.updateSchedule(s);
					scheduleIDMap.put(s.getScheduleIdentityPair(), s);
				}
			} catch (ScheduleManagerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			logger.error("The provided schedule is non-recurring and the scheduled time already passed. " + s.getScheduleName());
		}
	}
	
	
}
