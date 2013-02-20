package azkaban.sla;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.velocity.runtime.parser.node.GetExecutor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorMailer;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.sla.SLA.SlaAction;
import azkaban.sla.SLA.SlaRule;
import azkaban.sla.SLA.SlaSetting;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;

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



/**
 * The SLAManager stores and checks the SLA (Service Level Agreement). It uses a single thread
 * instead and waits until correct check time for the flow, and individual jobs in the flow if their SLA is set. 
 */
public class SLAManager {
	private static Logger logger = Logger.getLogger(SLAManager.class);

	private SLALoader loader;

	private final SLARunner runner;
	private final SLAPreRunner prerunner;
	private final ExecutorManager executorManager;
	private SlaMailer mailer;

	private long lastCheckTime = -1;
	
	/**
	 * Give the sla manager a loader class that will properly load the
	 * sla.
	 * 
	 * @param loader
	 * @throws SLAManagerException 
	 */
	public SLAManager(ExecutorManager executorManager,
							SLALoader loader,
							Props props) throws SLAManagerException 
	{
		this.executorManager = executorManager;
		this.loader = loader;
		this.mailer = new SlaMailer(props);
		this.runner = new SLARunner();
		this.prerunner = new SLAPreRunner();

		List<SLA> SLAList = null;
		try {
			SLAList = loader.loadSLAs();
		} catch (SLAManagerException e) {
			// TODO Auto-generated catch block
			throw e;
		}

		for (SLA sla : SLAList) {
			runner.addRunnerSLA(sla);
		}

		this.runner.start();
	}

	/**
	 * Shutdowns the sla thread. After shutdown, it may not be safe to use
	 * it again.
	 */
	public void shutdown() {
		this.runner.shutdown();
		this.prerunner.shutdown();
	}

	/**
	 * Removes the flow from the SLA if it exists.
	 * 
	 * @param id
	 * @throws SLAManagerException 
	 */
	public void removeSLA(SLA s) throws SLAManagerException {
		logger.info("Removing SLA " + s.toString());
		runner.removeRunnerSLA(s);
		loader.removeSLA(s);
	}

	public void submitSla(
			int execId, 
			String id,
			DateTime checkTime, 
			List<String> emails,
			List<SlaAction> slaActions,
			List<SlaSetting> jobSettings,
			SlaRule slaRule
			) throws SLAManagerException {
		SLA s = new SLA(execId, id, checkTime, emails, slaActions, jobSettings, slaRule);
		logger.info("Submitting SLA " + s.toString());
		try {
			loader.insertSLA(s);
			runner.addRunnerSLA(s);
		}
		catch (SLAManagerException e) {
			throw new SLAManagerException("Failed to add new SLA!" + e.getCause());
		}
	}
	
	/**
	 * Thread that simply invokes the checking of flows when the SLA is
	 * ready.
	 * 
	 */
	public class SLARunner extends Thread {
		private final PriorityBlockingQueue<SLA> SLAs;
		private AtomicBoolean stillAlive = new AtomicBoolean(true);

		// Five minute minimum intervals
		private static final int TIMEOUT_MS = 60000;

		public SLARunner() {
			SLAs = new PriorityBlockingQueue<SLA>(1,new SLAComparator());
		}

		public void shutdown() {
			logger.error("Shutting down SLA runner thread");
			stillAlive.set(false);
			this.interrupt();
		}

		/**
		 * Return a list of flow with SLAs
		 * 
		 * @return
		 */
		protected synchronized List<SLA> getRunnerSLAs() {
			return new ArrayList<SLA>(SLAs);
		}

		/**
		 * Adds SLA into runner and then interrupts so it will update
		 * its wait time.
		 * 
		 * @param flow
		 */
		public synchronized void addRunnerSLA(SLA s) {
			logger.info("Adding " + s + " to SLA runner.");
			SLAs.add(s);
			this.interrupt();
		}
		
		/**
		 * Remove runner SLA. Does not interrupt.
		 * 
		 * @param flow
		 * @throws SLAManagerException 
		 */
		public synchronized void removeRunnerSLA(SLA s) {
			logger.info("Removing " + s + " from the SLA runner.");
			SLAs.remove(s);
		}

		public void run() {
			while (stillAlive.get()) {
				synchronized (this) {
					try {
						lastCheckTime = System.currentTimeMillis();
						
						// TODO clear up the exception handling
						SLA s = SLAs.peek();

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
							if (!(new DateTime(s.getCheckTime())).isAfterNow()) {
								// Run flow. The invocation of flows should be quick.
								SLA runningSLA = SLAs.poll();
								
								logger.info("Checking sla " + runningSLA.toString() );
								
								int execId = s.getExecId();
								ExecutableFlow exflow = executorManager.getExecutableFlow(execId);
								
								if(runningSLA.getJobName().equals("") && runningSLA.getRule().equals(SlaRule.WAITANDCHECKJOB)) {
									// do the checking of potential jobsla submissions
									List<SlaSetting> jobSettings = runningSLA.getJobSettings();
									List<SlaSetting> removeSettings = new ArrayList<SLA.SlaSetting>();
									for(SlaSetting set : jobSettings) {
										ExecutableNode node = exflow.getExecutableNode(set.getId());
										if(node != null) {
											if(node.getStartTime() != -1 || executorManager.isFinished(exflow)) {
												submitSla(execId, set.getId(), new DateTime(node.getStartTime()).plus(set.getDuration()), runningSLA.getEmails(), set.getActions(), null, set.getRule());
												removeSettings.add(set);
												logger.info("Job " + set.getId() + " already started, monitoring SLA.");
											}
										}
										else {
											mailer.sendSlaEmail(s, "The SLA setting for flow/job is no longer valid as flow structure has changed. Execution " + s.getExecId());
											removeSettings.add(set);
											
										}
									}
									for(SlaSetting remove : removeSettings) {
										jobSettings.remove(remove);
									}
									if(jobSettings.size() == 0) {
										removeRunnerSLA(runningSLA);
										loader.removeSLA(runningSLA);
									}
									else {
										removeRunnerSLA(runningSLA);
										loader.removeSLA(runningSLA);
										runningSLA.setCheckTime(runningSLA.getCheckTime().plusMillis(TIMEOUT_MS));
										addRunnerSLA(runningSLA);
										loader.insertSLA(runningSLA);
									}
								}
								else {
									if(!metSla(runningSLA, exflow)) {
										takeSLAFailActions(runningSLA, exflow);
									}
									else {
										takeSLASuccessActions(runningSLA, exflow);
									}


									removeRunnerSLA(runningSLA);
									loader.removeSLA(runningSLA);
								}
							} else {
								// wait until flow run
								long millisWait = Math.max(0, s.getCheckTime().getMillis() - (new DateTime()).getMillis());
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

		private boolean metSla(SLA s, ExecutableFlow exflow) {
			SlaRule rule = s.getRule();
			long finishTime;
			Status status;
			if(s.getJobName().equals("")) {
				finishTime = exflow.getEndTime();
				status = exflow.getStatus();
			}
			else {
				ExecutableNode exnode = exflow.getExecutableNode(s.getJobName());
				finishTime = exnode.getEndTime();
				status = exnode.getStatus();
			}
			
			switch(rule) {
				case FINISH:	// check finish time
					return finishTime != -1 && finishTime < s.getCheckTime().getMillis();
				case SUCCESS: 	// check finish and successful
					return status == Status.SUCCEEDED && finishTime < s.getCheckTime().getMillis();
				default: 
					logger.error("Unknown SLA rules!");
					return false;
			}
		}

		/**
		 * Class to sort the sla based on time.
		 * 
		 */
		private class SLAComparator implements Comparator<SLA> {
			@Override
			public int compare(SLA arg0, SLA arg1) {
				long first = arg1.getCheckTime().getMillis();
				long second = arg0.getCheckTime().getMillis();

				if (first == second) {
					return 0;
				} else if (first < second) {
					return 1;
				}

				return -1;
			}
		}
	}

	private void takeSLAFailActions(SLA s, ExecutableFlow exflow) {
		logger.info("SLA " + s.toString() + " missed! Taking predefined actions");
		List<SlaAction> actions = s.getActions();
		for(SlaAction act : actions) {
			if(act.equals(SlaAction.EMAIL)) {
				try {
					sendSlaAlertEmail(s, exflow);
				}
				catch (Exception e) {
					logger.error("Failed to send out SLA alert email. " + e.getCause());
				}
			} else if(act.equals(SlaAction.KILL)) {
				try {
					executorManager.cancelFlow(exflow, "azkaban");
					sendSlaKillEmail(s, exflow);
				} catch (ExecutorManagerException e) {
					// TODO Auto-generated catch block
					logger.error("Cancel flow failed." + e.getCause());
				}
			}
		}
	}
	
	private void takeSLASuccessActions(SLA s, ExecutableFlow exflow) {
		sendSlaSuccessEmail(s, exflow);
		
	}
	
	private void sendSlaAlertEmail(SLA s, ExecutableFlow exflow) {
		String message = null;
		ExecutableNode exnode;
		switch(s.getRule()) {
			case FINISH:
				if(s.getJobName().equals("")) {
					message = "Flow " + exflow.getFlowId() + " failed to finish with set SLA." + String.format("%n");
					message += "Flow started at " + new DateTime(exflow.getStartTime()).toDateTimeISO() + String.format("%n");
					message += "Flow status at " + s.getCheckTime().toDateTimeISO() + " is " + exflow.getStatus();
				}
				else {
					exnode = exflow.getExecutableNode(s.getJobName());
					message = "Job " + s.getJobName() + " of flow " + exflow.getFlowId() + " failed to finish with set SLA." + String.format("%n");
					message += "Job started at " + new DateTime(exnode.getStartTime()).toDateTimeISO() + String.format("%n");
					message += "Job status at " + s.getCheckTime().toDateTimeISO() + " is " + exnode.getStatus();
				}
				break;
			case SUCCESS:
				if(s.getJobName().equals("")) {
					message = "Flow " + exflow.getFlowId() + " didn't finish successfully with set SLA. " + String.format("%n");
					message += "Flow started at " + new DateTime(exflow.getStartTime()).toDateTimeISO() + String.format("  %n");
					message += "Flow status at " + s.getCheckTime().toDateTimeISO() + " is " + exflow.getStatus();
				}
				else {
					exnode = exflow.getExecutableNode(s.getJobName());
					message = "Job " + s.getJobName() + " of flow " + exflow.getFlowId() + " didn't finish successfully with set SLA." + String.format("%n"); 
					message += "Job started at " + new DateTime(exnode.getStartTime()).toDateTimeISO() + String.format("%n");
					message += "Job status at " + s.getCheckTime().toDateTimeISO() + " is " + exnode.getStatus();
				}
				break;
			default:
				logger.error("Unknown SLA rules!");
				message = "Unknown SLA was not met!";
				break;
		}
		mailer.sendSlaEmail(s, message);
	}
	
	private void sendSlaSuccessEmail(SLA s, ExecutableFlow exflow) {
		String message = null;
		ExecutableNode exnode;
		switch(s.getRule()) {
			case FINISH:
				if(s.getJobName().equals("")) {
					message = "Flow " + exflow.getFlowId() + " finished within the set SLA." + String.format("%n");
					message += "Flow started at " + new DateTime(exflow.getStartTime()).toDateTimeISO() + String.format("%n");
					message += "Flow status at " + s.getCheckTime().toDateTimeISO() + " is " + exflow.getStatus();
				}
				else {
					exnode = exflow.getExecutableNode(s.getJobName());
					message = "Job " + s.getJobName() + " of flow " + exflow.getFlowId() + " finished within the set SLA." + String.format("%n");
					message += "Job started at " + new DateTime(exnode.getStartTime()).toDateTimeISO() + String.format("%n");
					message += "Job status at " + s.getCheckTime().toDateTimeISO() + " is " + exnode.getStatus();
				}
				break;
			case SUCCESS:
				if(s.getJobName().equals("")) {
					message = "Flow " + exflow.getFlowId() + " successfully finished within the set SLA." + String.format("%n");
					message += "Flow started at " + new DateTime(exflow.getStartTime()).toDateTimeISO() + String.format("  %n");
					message += "Flow status at " + s.getCheckTime().toDateTimeISO() + " is " + exflow.getStatus();
				}
				else {
					exnode = exflow.getExecutableNode(s.getJobName());
					message = "Job " + s.getJobName() + " of flow " + exflow.getFlowId() + " successfully finished within the set SLA." + String.format("%n"); 
					message += "Job started at " + new DateTime(exnode.getStartTime()).toDateTimeISO() + String.format("%n");
					message += "Job status at " + s.getCheckTime().toDateTimeISO() + " is " + exnode.getStatus();
				}
				break;
			default:
				logger.error("Unknown SLA rules!");
				message = "Unknown SLA was not met!";
				break;
		}
		mailer.sendSlaEmail(s, message);
	}
	
	private void sendSlaKillEmail(SLA s, ExecutableFlow exflow) {
		String message = null;
		ExecutableNode exnode;
		switch(s.getRule()) {
			case FINISH:
				if(s.getJobName().equals("")) {
					message = "Flow " + exflow.getFlowId() + " failed to finish with set SLA and is killed. " + String.format("%n");
					message += "Flow started at " + new DateTime(exflow.getStartTime()).toDateTimeISO() + String.format("%n");
					message += "Flow status at " + s.getCheckTime().toDateTimeISO() + " is " + exflow.getStatus();
				}
				else {
					exnode = exflow.getExecutableNode(s.getJobName());
						message = "Job " + s.getJobName() + " of flow " + exflow.getFlowId() + " failed to finish with set SLA and is killed. " + String.format("%n");
						message += "Job started at " + new DateTime(exnode.getStartTime()).toDateTimeISO() + String.format("%n");
						message += "Job status at " + s.getCheckTime().toDateTimeISO() + " is " + exnode.getStatus();
				}
				break;
			case SUCCESS:
				if(s.getJobName().equals("")) {
					message = "Flow " + exflow.getFlowId() + " didn't finish successfully with set SLA and is killed. " + String.format("%n");
					message += "Flow started at " + new DateTime(exflow.getStartTime()).toDateTimeISO() + String.format("  %n");
					message += "Flow status at " + s.getCheckTime().toDateTimeISO() + " is " + exflow.getStatus();
				}
				else {
					exnode = exflow.getExecutableNode(s.getJobName());
					message = "Job " + s.getJobName() + " of flow " + exflow.getFlowId() + " didn't finish successfully with set SLA and is killed. " + String.format("%n"); 
					message += "Job started at " + new DateTime(exnode.getStartTime()).toDateTimeISO() + String.format("%n");
					message += "Job status at " + s.getCheckTime().toDateTimeISO() + " is " + exnode.getStatus();
				}
				break;
			default:
				logger.error("Unknown SLA rules!");
				message = "Unknown SLA was not met!";
				break;
		}
		mailer.sendSlaEmail(s, message);
	}
	
	
	public class SLAPreRunner extends Thread {
		private final List<SLA> preSlas;
		private AtomicBoolean stillAlive = new AtomicBoolean(true);

		// Five minute minimum intervals
		private static final int TIMEOUT_MS = 300000;

		public SLAPreRunner() {
			preSlas = new ArrayList<SLA>();
		}

		public void shutdown() {
			logger.error("Shutting down pre-sla checker thread");
			stillAlive.set(false);
			this.interrupt();
		}

		/**
		 * Return a list of flow with SLAs
		 * 
		 * @return
		 */
		protected synchronized List<SLA> getPreSlas() {
			return new ArrayList<SLA>(preSlas);
		}

		/**
		 * Adds SLA into runner and then interrupts so it will update
		 * its wait time.
		 * 
		 * @param flow
		 */
		public synchronized void addCheckerPreSla(SLA s) {
			logger.info("Adding " + s + " to pre-sla checker.");
			preSlas.add(s);
			this.interrupt();
		}
		
		/**
		 * Remove runner SLA. Does not interrupt.
		 * 
		 * @param flow
		 * @throws SLAManagerException 
		 */
		public synchronized void removeCheckerPreSla(SLA s) {
			logger.info("Removing " + s + " from the pre-sla checker.");
			preSlas.remove(s);
		}

		public void run() {
			while (stillAlive.get()) {
				synchronized (this) {
					try {
						// TODO clear up the exception handling

						if (preSlas.size() == 0) {
							try {
								this.wait(TIMEOUT_MS);
							} catch (InterruptedException e) {
								// interruption should occur when items are added or removed from the queue.
							}
						} else {
							for(SLA s : preSlas) {
								ExecutableFlow exflow = executorManager.getExecutableFlow(s.getExecId());
								String id = s.getJobName();
								if(!s.equals("")) {
									ExecutableNode exnode = exflow.getExecutableNode(id);
									if(exnode.getStartTime() != -1) {
										
									}
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
	}

	public int getNumActiveSLA() {
		return runner.getRunnerSLAs().size();
	}
	
	public State getSLAThreadState() {
		return runner.getState();
	}
	
	public boolean isThreadActive() {
		return runner.isAlive();
	}
	
	public List<SLA> getSLAList() {
		return runner.getRunnerSLAs();
	}
	
	public long getLastCheckTime() {
		return lastCheckTime;
	}
}
