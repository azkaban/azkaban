package azkaban.scheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormat;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.flow.Flow;
import azkaban.jobExecutor.utils.JobExecutionException;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.scheduler.ScheduledFlow.SchedStatus;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Props;



/**
 * The ScheduleManager stores and executes the schedule. It uses a single thread instead
 * and waits until correct loading time for the flow. It will not remove the flow from the
 * schedule when it is run, which can potentially allow the flow to and overlap each other.
 * 
 * @author Richard
 */
public class ScheduleManager {
	private static Logger logger = Logger.getLogger(ScheduleManager.class);

    private final DateTimeFormatter _dateFormat = DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss:SSS");
	private ScheduleLoader loader;
    private Map<String, ScheduledFlow> scheduleIDMap = new LinkedHashMap<String, ScheduledFlow>(); 
    private final ScheduleRunner runner;
    private final ExecutorManager executorManager;
    private final ProjectManager projectManager;
    
    /**
     * Give the schedule manager a loader class that will properly load the schedule.
     * 
     * @param loader
     */
    public ScheduleManager(
    		ExecutorManager executorManager,
    		ProjectManager projectManager,
    		ScheduleLoader loader) 
    {
    	this.executorManager = executorManager;
    	this.projectManager = projectManager;
    	this.loader = loader;
    	this.runner = new ScheduleRunner();
    	
    	List<ScheduledFlow> scheduleList = loader.loadSchedule();
    	for (ScheduledFlow flow: scheduleList) {
    		internalSchedule(flow);
    	}
    	
    	this.runner.start();
    }
    
    /**
     * Shutdowns the scheduler thread. After shutdown, it may not be safe to use it again.
     */
    public void shutdown() {
    	this.runner.shutdown();
    }
    
    /**
     * Retrieves a copy of the list of schedules.
     * 
     * @return
     */
    public synchronized List<ScheduledFlow> getSchedule() {
    	return runner.getSchedule();
    }

    /**
     * Returns the scheduled flow for the flow name
     * 
     * @param id
     * @return
     */
    public ScheduledFlow getSchedule(String scheduleId) {
    	return scheduleIDMap.get(scheduleId);
    }
    
    /**
     * Removes the flow from the schedule if it exists.
     * 
     * @param id
     */
    public synchronized void removeScheduledFlow(String scheduleId) {
    	ScheduledFlow flow = scheduleIDMap.get(scheduleId);
    	scheduleIDMap.remove(scheduleId);
    	runner.removeScheduledFlow(flow);
    	
    	loader.saveSchedule(getSchedule());
    }
    
//    public synchronized void pauseScheduledFlow(String scheduleId){
//    	try{
//        	ScheduledFlow flow = scheduleIDMap.get(scheduleId);
//        	flow.setSchedStatus(SchedStatus.LASTPAUSED);
//        	loader.saveSchedule(getSchedule());
//    	}
//    	catch (Exception e) {
//    		throw new RuntimeException("Error pausing a schedule " + scheduleId);
//		}
//    }
//    
//    public synchronized void resumeScheduledFlow(String scheduleId){
//    	try {
//    		ScheduledFlow flow = scheduleIDMap.get(scheduleId);
//        	flow.setSchedStatus(SchedStatus.LASTSUCCESS);
//        	loader.saveSchedule(getSchedule());			
//		} 
//    	catch (Exception e) {
//			throw new RuntimeException("Error resuming a schedule " + scheduleId);
//		}
//    }
    
    public void schedule(final String scheduleId,
    		final String user,
    		final String userSubmit,
    		final DateTime submitTime,
    		final DateTime firstSchedTime,
    		final ReadablePeriod period
            ) {
    	//TODO: should validate projectId and flowId?
		logger.info("Scheduling flow '" + scheduleId + "' for " + _dateFormat.print(firstSchedTime)
		+ " with a period of " + PeriodFormat.getDefault().print(period));
		schedule(new ScheduledFlow(scheduleId, user, userSubmit, submitTime, firstSchedTime, period));
	}
    
    /**
     * Schedule the flow
     * @param flowId
     * @param date
     * @param ignoreDep
     */
    public void schedule(String scheduleId, String user, String userSubmit, DateTime submitTime, DateTime firstSchedTime) {
        logger.info("Scheduling flow '" + scheduleId + "' for " + _dateFormat.print(firstSchedTime));
        schedule(new ScheduledFlow(scheduleId, user, userSubmit, submitTime, firstSchedTime));
    }
    
    /**
     * Schedules the flow, but doesn't save the schedule afterwards.
     * @param flow
     */
    private synchronized void internalSchedule(ScheduledFlow flow) {
    	ScheduledFlow existing = scheduleIDMap.get(flow.getScheduleId());
    	flow.updateTime();
    	if (existing != null) {
    		this.runner.removeScheduledFlow(existing);
    	}
    	
		this.runner.addScheduledFlow(flow);
    	scheduleIDMap.put(flow.getScheduleId(), flow);
    }
    
    /**
     * Adds a flow to the schedule.
     * 
     * @param flow
     */
    public synchronized void schedule(ScheduledFlow flow) {
    	internalSchedule(flow);
    	saveSchedule();
    }
    
    /**
     * Save the schedule
     */
    private void saveSchedule() {
    	loader.saveSchedule(getSchedule());
    }
    
    
    /**
     * Thread that simply invokes the running of flows when the schedule is
     * ready.
     * 
     * @author Richard Park
     *
     */
    public class ScheduleRunner extends Thread {
    	private final PriorityBlockingQueue<ScheduledFlow> schedule;
    	private AtomicBoolean stillAlive = new AtomicBoolean(true);

        	// Five minute minimum intervals
    	private static final int TIMEOUT_MS = 300000;
    	
    	public ScheduleRunner() {
    		schedule = new PriorityBlockingQueue<ScheduledFlow>(1, new ScheduleComparator());
    	}
    	
    	public void shutdown() {
    		logger.error("Shutting down scheduler thread");
    		stillAlive.set(false);
    		this.interrupt();
    	}
    	
    	/**
    	 * Return a list of scheduled flow
    	 * @return
    	 */
    	public synchronized List<ScheduledFlow> getSchedule() {
    		return new ArrayList<ScheduledFlow>(schedule);
    	}
    	
    	/**
    	 * Adds the flow to the schedule and then interrupts so it will update its wait time.
    	 * @param flow
    	 */
        public synchronized void addScheduledFlow(ScheduledFlow flow) {
        	logger.info("Adding " + flow + " to schedule.");
        	schedule.add(flow);
//            MonitorImpl.getInternalMonitorInterface().workflowEvent(null, 
//                    System.currentTimeMillis(),
//                    WorkflowAction.SCHEDULE_WORKFLOW, 
//                    WorkflowState.NOP,
//                    flow.getId());

        	this.interrupt();
        }

        /**
         * Remove scheduled flows. Does not interrupt.
         * 
         * @param flow
         */
        public synchronized void removeScheduledFlow(ScheduledFlow flow) {
        	logger.info("Removing " + flow + " from the schedule.");
        	schedule.remove(flow);
//            MonitorImpl.getInternalMonitorInterface().workflowEvent(null, 
//                    System.currentTimeMillis(),
//                    WorkflowAction.UNSCHEDULE_WORKFLOW,
//                    WorkflowState.NOP,
//                    flow.getId());
        	// Don't need to interrupt, because if this is originally on the top of the queue,
        	// it'll just skip it.
        }
        
        public void run() {
        	while(stillAlive.get()) {
        		synchronized (this) {
        			try {
        				//TODO clear up the exception handling
        				ScheduledFlow schedFlow = schedule.peek();
	    	    		
	    	    		if (schedFlow == null) {
	    	    			// If null, wake up every minute or so to see if there's something to do.
	    	    			// Most likely there will not be.
	    	    			try {
	    	    				this.wait(TIMEOUT_MS);
	    	    			} catch (InterruptedException e) {
	    	    				// interruption should occur when items are added or removed from the queue.
	    	    			}
	    	    		}
	    	    		else {
	    	    			// We've passed the flow execution time, so we will run.
	    	    			if (!schedFlow.getNextExecTime().isAfterNow()) {
	    	    				// Run flow. The invocation of flows should be quick.
	    	    				ScheduledFlow runningFlow = schedule.poll();
	    	    				logger.info("Scheduler attempting to run " + runningFlow.getScheduleId());
	
	    	    				// Execute the flow here
	    	    				try {
	    	    					Project project = projectManager.getProject(runningFlow.getProjectId());
	    	    					if (project == null) {
	    	    						logger.error("Scheduled Project "+runningFlow.getProjectId()+" does not exist!");
	    	    						throw new RuntimeException("Error finding the scheduled project. " + runningFlow.getScheduleId());
	    	    					}
	    	    					
	    	    					Flow flow = project.getFlow(runningFlow.getFlowId());
	    	    					if (flow == null) {
	    	    						logger.error("Flow " + runningFlow.getFlowId() + " cannot be found in project " + project.getName());
	    	    						throw new RuntimeException("Error finding the scheduled flow. " + runningFlow.getScheduleId());
	    	    					}
	    	    					
	    	    					HashMap<String, Props> sources;
	    	    					try {
	    	    						sources = projectManager.getAllFlowProperties(project, runningFlow.getFlowId());
	    	    					}
	    	    					catch (ProjectManagerException e) {
	    	    						logger.error(e.getMessage());
	    	    						throw new RuntimeException("Error getting the flow resources. " + runningFlow.getScheduleId());
	    	    					}
	    	    				    	    					
	    	    					// Create ExecutableFlow
	    	    					ExecutableFlow exflow = executorManager.createExecutableFlow(flow);
	    	    					exflow.setSubmitUser(runningFlow.getUser());
	    	    					//TODO make disabled in scheduled flow
//	    	    					Map<String, String> paramGroup = this.getParamGroup(req, "disabled");
//	    	    					for (Map.Entry<String, String> entry: paramGroup.entrySet()) {
//	    	    						boolean nodeDisabled = Boolean.parseBoolean(entry.getValue());
//	    	    						exflow.setStatus(entry.getKey(), nodeDisabled ? Status.DISABLED : Status.READY);
//	    	    					}
	    	    					
	    	    					// Create directory
	    	    					try {
	    	    						executorManager.setupExecutableFlow(exflow);
	    	    					} catch (ExecutorManagerException e) {
	    	    						try {
	    	    							executorManager.cleanupAll(exflow);
	    	    						} catch (ExecutorManagerException e1) {
	    	    							e1.printStackTrace();
	    	    						}
	    	    						logger.error(e.getMessage());
	    	    						return;
	    	    					}

	    	    					// Copy files to the source.
	    	    					File executionDir = new File(exflow.getExecutionPath());
	    	    					try {
	    	    						projectManager.copyProjectSourceFilesToDirectory(project, executionDir);
	    	    					} catch (ProjectManagerException e) {
	    	    						try {
	    	    							executorManager.cleanupAll(exflow);
	    	    						} catch (ExecutorManagerException e1) {
	    	    							e1.printStackTrace();
	    	    						}
	    	    						logger.error(e.getMessage());
	    	    						return;
	    	    					}
	    	    					

	    	    					try {
	    	    						executorManager.executeFlow(exflow);
	    	    					} catch (ExecutorManagerException e) {
	    	    						try {
	    	    							executorManager.cleanupAll(exflow);
	    	    						} catch (ExecutorManagerException e1) {
	    	    							e1.printStackTrace();
	    	    						}
	    	    						
	    	    						logger.error(e.getMessage());
	    	    						return;
	    	    					}
	    	    				} catch (JobExecutionException e) {
	    	    					logger.info("Could not run flow. " + e.getMessage());
	    	    				}
	    	    	        	schedule.remove(runningFlow);
	    	    				
	    	    				// Immediately reschedule if it's possible. Let the execution manager
	    	    				// handle any duplicate runs.
	    	    				if (runningFlow.updateTime()) {
	    	    					schedule.add(runningFlow);
	    	    				}
    	    					saveSchedule();
	    	    			}
	    	    			else {
	    	    				// wait until flow run
	    	    				long millisWait = Math.max(0, schedFlow.getNextExecTime().getMillis() - (new DateTime()).getMillis());
	    	    				try {
	    							this.wait(Math.min(millisWait, TIMEOUT_MS));
	    						} catch (InterruptedException e) {
	    							// interruption should occur when items are added or removed from the queue.
	    						}
	    	    			}
	    	    		}
        			}
        			catch (Exception e) {
        				logger.error("Unexpected exception has been thrown in scheduler", e);
        			}
        			catch (Throwable e) {
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
        private class ScheduleComparator implements Comparator<ScheduledFlow>{
    		@Override
    		public int compare(ScheduledFlow arg0, ScheduledFlow arg1) {
    			DateTime first = arg1.getNextExecTime();
    			DateTime second = arg0.getNextExecTime();
    			
    			if (first.isEqual(second)) {
    				return 0;
    			}
    			else if (first.isBefore(second)) {
    				return 1;
    			}
    			
    			return -1;
    		}	
        }
    }
}