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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;

import azkaban.flow.Flow;
import azkaban.jobExecutor.utils.JobExecutionException;
import azkaban.project.Project;
import azkaban.project.ProjectManager;


import azkaban.utils.Pair;


/**
 * The ScheduleManager stores and executes the schedule. It uses a single thread
 * instead and waits until correct loading time for the flow. It will not remove
 * the flow from the schedule when it is run, which can potentially allow the
 * flow to and overlap each other.
 */
public class ScheduleManager {
	private static Logger logger = Logger.getLogger(ScheduleManager.class);

	private final DateTimeFormatter _dateFormat = DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss:SSS");
	private ScheduleLoader loader;

	private Map<Pair<Integer, String>, Schedule> scheduleIDMap = new LinkedHashMap<Pair<Integer, String>, Schedule>();
	private final ScheduleRunner runner;
	private final ExecutorManager executorManager;
	private final ProjectManager projectManager;

	/**
	 * Give the schedule manager a loader class that will properly load the
	 * schedule.
	 * 
	 * @param loader
	 */
	public ScheduleManager(ExecutorManager executorManager,
							ProjectManager projectManager, 
							ScheduleLoader loader) 
	{
		this.executorManager = executorManager;
		this.projectManager = projectManager;
		this.loader = loader;
		this.runner = new ScheduleRunner();

		List<Schedule> scheduleList = null;
		try {
			scheduleList = loader.loadSchedules();
		} catch (ScheduleManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (Schedule sched : scheduleList) {
			internalSchedule(sched);
		}

		this.runner.start();
	}

	/**
	 * Shutdowns the scheduler thread. After shutdown, it may not be safe to use
	 * it again.
	 */
	public void shutdown() {
		this.runner.shutdown();
	}

	/**
	 * Retrieves a copy of the list of schedules.
	 * 
	 * @return
	 */
	public synchronized List<Schedule> getSchedules() {
		return runner.getRunnerSchedules();
	}

	/**
	 * Returns the scheduled flow for the flow name
	 * 
	 * @param id
	 * @return
	*/
	public Schedule getSchedule(Pair<Integer, String> scheduleId) {
		return scheduleIDMap.get(scheduleId);
	}

	/**
	 * Removes the flow from the schedule if it exists.
	 * 
	 * @param id
	 */
	public synchronized void removeSchedule(Pair<Integer, String> scheduleId) {
		Schedule sched = scheduleIDMap.get(scheduleId);
		scheduleIDMap.remove(scheduleId);
		
		runner.removeRunnerSchedule(sched);
		try {
			loader.removeSchedule(sched);
		} catch (ScheduleManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// public synchronized void pauseScheduledFlow(String scheduleId){
	// try{
	// ScheduledFlow flow = scheduleIDMap.get(scheduleId);
	// flow.setSchedStatus(SchedStatus.LASTPAUSED);
	// loader.saveSchedule(getSchedule());
	// }
	// catch (Exception e) {
	// throw new RuntimeException("Error pausing a schedule " + scheduleId);
	// }
	// }
	//
	// public synchronized void resumeScheduledFlow(String scheduleId){
	// try {
	// ScheduledFlow flow = scheduleIDMap.get(scheduleId);
	// flow.setSchedStatus(SchedStatus.LASTSUCCESS);
	// loader.saveSchedule(getSchedule());
	// }
	// catch (Exception e) {
	// throw new RuntimeException("Error resuming a schedule " + scheduleId);
	// }
	// }

	public Schedule scheduleFlow(
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
		Schedule sched = new Schedule(projectId, projectName, flowName, status, firstSchedTime, timezone, period, lastModifyTime, nextExecTime, submitTime, submitUser);
		logger.info("Scheduling flow '" + sched.getScheduleName() + "' for "
				+ _dateFormat.print(firstSchedTime) + " with a period of "
				+ period == null ? "(non-recurring)" : period);

		insertSchedule(sched);
		return sched;
	}

//	/**
//	 * Schedule the flow
//	 * 
//	 * @param flowId
//	 * @param date
//	 * @param ignoreDep
//	 */
//	public Schedule schedule(
//			String scheduleId,
//			String projectId,
//			String flowId,
//			String user, 
//			String userSubmit,
//			DateTime submitTime,
//			DateTime firstSchedTime) 
//	{
//		logger.info("Scheduling flow '" + scheduleId + "' for " + _dateFormat.print(firstSchedTime));
//		ScheduledFlow scheduleFlow = new ScheduledFlow(scheduleId, projectId, flowId, user, userSubmit, submitTime, firstSchedTime);
//		schedule(scheduleFlow);
//		return scheduleFlow;
//	}

	/**
	 * Schedules the flow, but doesn't save the schedule afterwards.
	 * 
	 * @param flow
	 */
	private synchronized void internalSchedule(Schedule s) {
		Schedule existing = scheduleIDMap.get(s.getScheduleId());
		if (existing != null) {
			this.runner.removeRunnerSchedule(existing);
		}
		s.updateTime();
		this.runner.addRunnerSchedule(s);
		scheduleIDMap.put(s.getScheduleId(), s);
	}

	/**
	 * Adds a flow to the schedule.
	 * 
	 * @param flow
	 */
	public synchronized void insertSchedule(Schedule s) {
		boolean exist = scheduleIDMap.containsKey(s.getScheduleId());
		if(s.updateTime()) {
			internalSchedule(s);
			try {
				if(!exist) {
					loader.insertSchedule(s);
				}
				else
					loader.updateSchedule(s);
			} catch (ScheduleManagerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			logger.error("The provided schedule is non-recurring and the scheduled time already passed. " + s.getScheduleName());
		}
	}

//	/**
//	 * Save the schedule
//	 */
//	private void saveSchedule() {
//		loader.saveSchedule(getSchedule());
//	}
	
	/**
	 * Thread that simply invokes the running of flows when the schedule is
	 * ready.
	 * 
	 * @author Richard Park
	 * 
	 */
	public class ScheduleRunner extends Thread {
		private final PriorityBlockingQueue<Schedule> schedules;
		private AtomicBoolean stillAlive = new AtomicBoolean(true);

		// Five minute minimum intervals
		private static final int TIMEOUT_MS = 300000;

		public ScheduleRunner() {
			schedules = new PriorityBlockingQueue<Schedule>(1,new ScheduleComparator());
		}

		public void shutdown() {
			logger.error("Shutting down scheduler thread");
			stillAlive.set(false);
			this.interrupt();
		}

		/**
		 * Return a list of scheduled flow
		 * 
		 * @return
		 */
		public synchronized List<Schedule> getRunnerSchedules() {
			return new ArrayList<Schedule>(schedules);
		}

		/**
		 * Adds the flow to the schedule and then interrupts so it will update
		 * its wait time.
		 * 
		 * @param flow
		 */
		public synchronized void addRunnerSchedule(Schedule s) {
			logger.info("Adding " + s + " to schedule runner.");
			schedules.add(s);
			// MonitorImpl.getInternalMonitorInterface().workflowEvent(null,
			// System.currentTimeMillis(),
			// WorkflowAction.SCHEDULE_WORKFLOW,
			// WorkflowState.NOP,
			// flow.getId());

			this.interrupt();
		}

		/**
		 * Remove scheduled flows. Does not interrupt.
		 * 
		 * @param flow
		 */
		public synchronized void removeRunnerSchedule(Schedule s) {
			logger.info("Removing " + s + " from the schedule runner.");
			schedules.remove(s);
			// MonitorImpl.getInternalMonitorInterface().workflowEvent(null,
			// System.currentTimeMillis(),
			// WorkflowAction.UNSCHEDULE_WORKFLOW,
			// WorkflowState.NOP,
			// flow.getId());
			// Don't need to interrupt, because if this is originally on the top
			// of the queue,
			// it'll just skip it.
		}

		public void run() {
			while (stillAlive.get()) {
				synchronized (this) {
					try {
						// TODO clear up the exception handling
						Schedule s = schedules.peek();

						if (s == null) {
							// If null, wake up every minute or so to see if
							// there's something to do. Most likely there will not be.
							try {
								this.wait(TIMEOUT_MS);
							} catch (InterruptedException e) {
								// interruption should occur when items are added or removed from the queue.
							}
						} else {
							// We've passed the flow execution time, so we will run.
							if (!(new DateTime(s.getNextExecTime())).isAfterNow()) {
								// Run flow. The invocation of flows should be quick.
								Schedule runningSched = schedules.poll();
								
								logger.info("Scheduler attempting to run " + runningSched.getScheduleName() );
								
								// check if it is already running
								if(!executorManager.isFlowRunning(runningSched.getProjectId(), runningSched.getFlowName()))
								{
									// Execute the flow here
									try {
										Project project = projectManager.getProject(runningSched.getProjectId());
										if (project == null) {
											logger.error("Scheduled Project " + runningSched.getProjectId() + " does not exist!");
											throw new RuntimeException("Error finding the scheduled project. "+ runningSched.getProjectId());
										}	
										//TODO It is possible that the project is there, but the flow doesn't exist because upload a version that changes flow structure
	
										Flow flow = project.getFlow(runningSched.getFlowName());
										if (flow == null) {
											logger.error("Flow " + runningSched.getScheduleName() + " cannot be found in project " + project.getName());
											throw new RuntimeException("Error finding the scheduled flow. " + runningSched.getScheduleName());
										}
	
										// Create ExecutableFlow
										ExecutableFlow exflow = new ExecutableFlow(flow);
										exflow.setSubmitUser(runningSched.getSubmitUser());
	
										// TODO make disabled in scheduled flow
										// Map<String, String> paramGroup =
										// this.getParamGroup(req, "disabled");
										// for (Map.Entry<String, String> entry:
										// paramGroup.entrySet()) {
										// boolean nodeDisabled =
										// Boolean.parseBoolean(entry.getValue());
										// exflow.setStatus(entry.getKey(),
										// nodeDisabled ? Status.DISABLED :
										// Status.READY);
										// }
	
										try {
											executorManager.submitExecutableFlow(exflow);
											logger.info("Scheduler has invoked " + exflow.getExecutionId());
										} catch (ExecutorManagerException e) {	
											logger.error("Scheduler invoked flow " + exflow.getExecutionId() + " has failed.");
											logger.error(e.getMessage());
											return;
										}
									} catch (JobExecutionException e) {
										logger.info("Could not run flow. " + e.getMessage());
									}
								}
								
								removeRunnerSchedule(runningSched);


								// Immediately reschedule if it's possible. Let
								// the execution manager
								// handle any duplicate runs.
								if (runningSched.updateTime()) {
									addRunnerSchedule(runningSched);
									loader.updateSchedule(runningSched);
								}
								else {
									removeSchedule(runningSched.getScheduleId());
								}								
							} else {
								// wait until flow run
								long millisWait = Math.max(0, s.getNextExecTime() - (new DateTime()).getMillis());
								try {
									this.wait(Math.min(millisWait, TIMEOUT_MS));
								} catch (InterruptedException e) {
									// interruption should occur when items are
									// added or removed from the queue.
								}
							}
						}
					} catch (Exception e) {
						logger.error("Unexpected exception has been thrown in scheduler", e);
					} catch (Throwable e) {
						logger.error("Unexpected throwable has been thrown in scheduler", e);
					}
				}
			}
		}

		/**
		 * Class to sort the schedule based on time.
		 * 
		 * @author Richard Park
		 */
		private class ScheduleComparator implements Comparator<Schedule> {
			@Override
			public int compare(Schedule arg0, Schedule arg1) {
				long first = arg1.getNextExecTime();
				long second = arg0.getNextExecTime();

				if (first == second) {
					return 0;
				} else if (first < second) {
					return 1;
				}

				return -1;
			}
		}
	}
}
