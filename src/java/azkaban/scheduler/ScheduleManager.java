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

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.sla.SLA.SlaAction;
import azkaban.sla.SLA.SlaRule;
import azkaban.sla.SLA.SlaSetting;
import azkaban.sla.SLAManager;
import azkaban.sla.SlaOptions;
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

	private Map<Pair<Integer, String>, Set<Schedule>> scheduleIdentityPairMap = new LinkedHashMap<Pair<Integer, String>, Set<Schedule>>();
	private Map<Integer, Schedule> scheduleIDMap = new LinkedHashMap<Integer, Schedule>();
	
	private final ExecutorManager executorManager;
	private final ProjectManager projectManager;
	private final SLAManager slaManager;
	
	private final boolean useExternalRunner;
	private final ScheduleRunner runner;
	
	// Used for mbeans to query Scheduler status
	

	/**
	 * Give the schedule manager a loader class that will properly load the
	 * schedule.
	 * 
	 * @param loader
	 */
	public ScheduleManager(ExecutorManager executorManager,
							ProjectManager projectManager, 
							SLAManager slaManager,
							ScheduleLoader loader,
							boolean useExternalRunner) 
	{
		this.executorManager = executorManager;
		this.projectManager = projectManager;
		this.slaManager = slaManager;
		this.loader = loader;
		this.useExternalRunner = useExternalRunner;
		
		

		List<Schedule> scheduleList = null;
		try {
			scheduleList = loader.loadSchedules();
		} catch (ScheduleManagerException e) {
			// TODO Auto-generated catch block
			logger.error("Failed to load schedules" + e.getCause() + e.getMessage());
			e.printStackTrace();
		}

		for (Schedule sched : scheduleList) {
			internalSchedule(sched);
		}

		if(!useExternalRunner) {
			this.runner = new ScheduleRunner();
			this.runner.start();
		} else {
			this.runner = null;
		}
	}

	/**
	 * Shutdowns the scheduler thread. After shutdown, it may not be safe to use
	 * it again.
	 */
	public void shutdown() {
		if(!useExternalRunner) {
			this.runner.shutdown();
		}
	}

	/**
	 * Retrieves a copy of the list of schedules.
	 * 
	 * @return
	 */
	public synchronized List<Schedule> getSchedules() {
		//return runner.getRunnerSchedules();
		return new ArrayList<Schedule>(scheduleIDMap.values());
	}

	/**
	 * Returns the scheduled flow for the flow name
	 * 
	 * @param id
	 * @return
	*/
	public Set<Schedule> getSchedules(int projectId, String flowId) {
		return scheduleIdentityPairMap.get(new Pair<Integer,String>(projectId, flowId));
	}

	/**
	 * Returns the scheduled flow for the scheduleId
	 * 
	 * @param id
	 * @return
	*/
	public Schedule getSchedule(int scheduleId) {
		return scheduleIDMap.get(scheduleId);
	}


	/**
	 * Removes the flow from the schedule if it exists.
	 * 
	 * @param id
	 */
	public synchronized void removeSchedules(int projectId, String flowId) {
		Set<Schedule> schedules = getSchedules(projectId, flowId);
		if(schedules != null) {
			for(Schedule sched : schedules) {
				removeSchedule(sched);
			}
		}
	}
	/**
	 * Removes the flow from the schedule if it exists.
	 * 
	 * @param id
	 */
	public synchronized void removeSchedule(Schedule sched) {
		
		Pair<Integer,String> identityPairMap = sched.getScheduleIdentityPair();
		Set<Schedule> schedules = scheduleIdentityPairMap.get(identityPairMap);
		if(schedules != null) {
			schedules.remove(sched);
			if(schedules.size() == 0) {
				scheduleIdentityPairMap.remove(identityPairMap);
			}
		}
		scheduleIDMap.remove(sched.getScheduleId());
		
		try {
			loader.removeSchedule(sched);
		} catch (ScheduleManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(!useExternalRunner) {
			runner.removeRunnerSchedule(sched);
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
	 * Schedules the flow, but doesn't save the schedule afterwards.
	 * 
	 * @param flow
	 */
	private synchronized void internalSchedule(Schedule s) {
		Schedule existing = scheduleIDMap.get(s.getScheduleId());
		if(!useExternalRunner) {
			if (existing != null) {
				this.runner.removeRunnerSchedule(existing);
			}
			s.updateTime();
			this.runner.addRunnerSchedule(s);
		}
		scheduleIDMap.put(s.getScheduleId(), s);
		Set<Schedule> schedules = scheduleIdentityPairMap.get(s.getScheduleIdentityPair());
		if(schedules == null) {
			schedules = new HashSet<Schedule>();
			scheduleIdentityPairMap.put(s.getScheduleIdentityPair(), schedules);
		}
		schedules.add(s);
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
					internalSchedule(s);
				}
				else{
					loader.updateSchedule(s);
					internalSchedule(s);
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
		
		private long lastCheckTime = -1;
		private long nextWakupTime = -1;
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

		public long getLastCheckTime() {
			return lastCheckTime;
		}
		
		public long getNextWakeupTime() {
			return nextWakupTime;
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
						lastCheckTime = System.currentTimeMillis();
						// TODO clear up the exception handling
						Schedule s = schedules.peek();

						if (s == null) {
							// If null, wake up every minute or so to see if
							// there's something to do. Most likely there will not be.
							try {
								logger.info("Nothing scheduled to run. Checking again soon.");
								nextWakupTime = System.currentTimeMillis() + TIMEOUT_MS;
								this.wait(TIMEOUT_MS);
							} catch (InterruptedException e) {
								// interruption should occur when items are added or removed from the queue.
							}
						} else {
							// We've passed the flow execution time, so we will run.
							if (!(new DateTime(s.getNextExecTime())).isAfterNow()) {
								// Run flow. The invocation of flows should be quick.
								Schedule runningSched = schedules.poll();

								logger.info("Scheduler ready to run " + runningSched.toString());
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
									System.out.println("ScheduleManager: creating schedule: " +runningSched.getScheduleId());
									exflow.setScheduleId(runningSched.getScheduleId());
									exflow.setSubmitUser(runningSched.getSubmitUser());
									exflow.addAllProxyUsers(project.getProxyUsers());
									
									ExecutionOptions flowOptions = runningSched.getExecutionOptions();
									if(flowOptions == null) {
										flowOptions = new ExecutionOptions();
										flowOptions.setConcurrentOption(ExecutionOptions.CONCURRENT_OPTION_SKIP);
									}
									exflow.setExecutionOptions(flowOptions);
									
									if (!flowOptions.isFailureEmailsOverridden()) {
										flowOptions.setFailureEmails(flow.getFailureEmails());
									}
									if (!flowOptions.isSuccessEmailsOverridden()) {
										flowOptions.setSuccessEmails(flow.getSuccessEmails());
									}
									
									try {
										executorManager.submitExecutableFlow(exflow);
										logger.info("Scheduler has invoked " + exflow.getExecutionId());
									} 
									catch (ExecutorManagerException e) {
										throw e;
									}
									catch (Exception e) {	
										e.printStackTrace();
										throw new ScheduleManagerException("Scheduler invoked flow " + exflow.getExecutionId() + " has failed.", e);
									}
									
									SlaOptions slaOptions = runningSched.getSlaOptions();
									if(slaOptions != null) {
										logger.info("Submitting SLA checkings for " + runningSched.getFlowName());
										// submit flow slas
										List<SlaSetting> jobsettings = new ArrayList<SlaSetting>();
										for(SlaSetting set : slaOptions.getSettings()) {
											if(set.getId().equals("")) {
												DateTime checkTime = new DateTime(runningSched.getNextExecTime()).plus(set.getDuration());
												slaManager.submitSla(exflow.getExecutionId(), "", checkTime, slaOptions.getSlaEmails(), set.getActions(), null, set.getRule());
											}
											else {
												jobsettings.add(set);
											}
										}
										if(jobsettings.size() > 0) {
											slaManager.submitSla(exflow.getExecutionId(), "", DateTime.now(), slaOptions.getSlaEmails(), new ArrayList<SlaAction>(), jobsettings, SlaRule.WAITANDCHECKJOB);
										}
									}
									
								} 
								catch (ExecutorManagerException e) {
									if (e.getReason() != null && e.getReason() == ExecutorManagerException.Reason.SkippedExecution) {
										logger.info(e.getMessage());
									}
									else {
										e.printStackTrace();
									}
								}
								catch (Exception e) {
									logger.info("Scheduler failed to run job. " + e.getMessage() + e.getCause());
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
									removeSchedule(runningSched);
								}								
							} else {
								// wait until flow run
								long millisWait = Math.max(0, s.getNextExecTime() - (new DateTime()).getMillis());
								try {
									nextWakupTime = System.currentTimeMillis() + millisWait;
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
	
	public long getLastCheckTime() {
		if(useExternalRunner) {
			return -1;
		} else {
			return runner.getLastCheckTime();
		}
	}
	
	public long getNextUpdateTime() {
		if(useExternalRunner) {
			return -1;
		} else {
			return runner.getNextWakeupTime();
		}
	}
	
	public State getThreadState() {
		if(useExternalRunner) {
			return null;
		} else {
			return runner.getState();
		}
	}
	
	public boolean isThreadActive() {
		if(useExternalRunner) {
			return false;
		} else {
			return runner.isAlive();
		}
	}
}
