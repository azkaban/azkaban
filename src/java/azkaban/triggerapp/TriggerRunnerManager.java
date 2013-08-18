package azkaban.triggerapp;

import java.io.IOException;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import azkaban.trigger.ActionTypeLoader;
import azkaban.trigger.CheckerTypeLoader;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerLoader;
import azkaban.trigger.TriggerManagerException;
import azkaban.trigger.TriggerStatus;
import azkaban.utils.Props;

public class TriggerRunnerManager {
	private static Logger logger = Logger.getLogger(TriggerRunnerManager.class);
	private static final long DEFAULT_SCANNER_INTERVAL_MS = 60000;

	private static Map<Integer, Trigger> triggerIdMap = new HashMap<Integer, Trigger>();
	
	private CheckerTypeLoader checkerTypeLoader;
	private ActionTypeLoader actionTypeLoader;
	private TriggerLoader triggerLoader;
	
	private Props globalProps;
	
	private final Props azkabanProps;
	
	private final TriggerScannerThread runnerThread;
	private long lastRunnerThreadCheckTime = -1;
	private long runnerThreadIdleTime = -1;
	
			
	public TriggerRunnerManager(Props props, TriggerLoader triggerLoader) throws IOException {
		
		azkabanProps = props;

		this.triggerLoader = triggerLoader;
		
		long scannerInterval = props.getLong("trigger.scan.interval", DEFAULT_SCANNER_INTERVAL_MS);
		runnerThread = new TriggerScannerThread(scannerInterval);

		checkerTypeLoader = new CheckerTypeLoader();
		actionTypeLoader = new ActionTypeLoader();
		
	}

	public void init() {
		try{
			checkerTypeLoader.init(azkabanProps);
			actionTypeLoader.init(azkabanProps);
		} catch(Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
		Condition.setCheckerLoader(checkerTypeLoader);
		Trigger.setActionTypeLoader(actionTypeLoader);

	}
	
	public void start() {
		
		try{
			// expect loader to return valid triggers
			List<Trigger> triggers = triggerLoader.loadTriggers();
			for(Trigger t : triggers) {
				runnerThread.addTrigger(t);
				triggerIdMap.put(t.getTriggerId(), t);
			}
		}catch(Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
		runnerThread.start();
	}

	public Props getGlobalProps() {
		return globalProps;
	}
	
	public void setGlobalProps(Props globalProps) {
		this.globalProps = globalProps;
	}
	
	public CheckerTypeLoader getCheckerLoader() {
		return checkerTypeLoader;
	}

	public ActionTypeLoader getActionLoader() {
		return actionTypeLoader;
	}

	public synchronized void insertTrigger(Trigger t) throws TriggerManagerException {
		
		triggerLoader.addTrigger(t);
		triggerIdMap.put(t.getTriggerId(), t);
		runnerThread.addTrigger(t);
	}
	
	public synchronized void removeTrigger(int id) throws TriggerManagerException {
		Trigger t = triggerIdMap.get(id);
		if(t != null) {
			removeTrigger(triggerIdMap.get(id));
		}
	}
	
	public synchronized void updateTrigger(int triggerId) throws TriggerManagerException {
		
		
		Trigger t = triggerIdMap.get(triggerId);
		
		updateTrigger(t);
	}
	
	public synchronized void updateTrigger(Trigger t) throws TriggerManagerException {
		if(!triggerIdMap.containsKey(t.getTriggerId())) {
			throw new TriggerManagerException("The trigger to update doesn't exist!");
		}
		runnerThread.deleteTrigger(t);

		Trigger t2 = triggerLoader.loadTrigger(t.getTriggerId());
		runnerThread.addTrigger(t2);
		triggerIdMap.put(t2.getTriggerId(), t2);

	}

	public synchronized void removeTrigger(Trigger t) throws TriggerManagerException {
		t.stopCheckers();
		triggerLoader.removeTrigger(t);
		runnerThread.deleteTrigger(t);
		triggerIdMap.remove(t.getTriggerId());		
	}
	
	public List<Trigger> getTriggers() {
		return new ArrayList<Trigger>(triggerIdMap.values());
	}
	
	public Map<String, Class<? extends ConditionChecker>> getSupportedCheckers() {
		return checkerTypeLoader.getSupportedCheckers();
	}
	
	private class TriggerScannerThread extends Thread {
		private BlockingQueue<Trigger> triggers;
		private boolean shutdown = false;
		//private AtomicBoolean stillAlive = new AtomicBoolean(true);
		private final long scannerInterval;
		
		public TriggerScannerThread(long scannerInterval) {
			triggers = new LinkedBlockingDeque<Trigger>();
			this.setName("TriggerRunnerManager-Trigger-Scanner-Thread");
			this.scannerInterval = scannerInterval;;
		}

		@SuppressWarnings("unused")
		public void shutdown() {
			logger.error("Shutting down trigger manager thread " + this.getName());
			shutdown = true;
			//stillAlive.set(false);
			this.interrupt();
		}
		
		public synchronized List<Trigger> getTriggers() {
			return new ArrayList<Trigger>(triggers);
		}
		
		public synchronized void addTrigger(Trigger t) {
			triggers.add(t);
		}
		
		public synchronized void deleteTrigger(Trigger t) {
			triggers.remove(t);
		}

		public void run() {
			//while(stillAlive.get()) {
			while(!shutdown) {
				synchronized (this) {
					try{
						lastRunnerThreadCheckTime = System.currentTimeMillis();
						
						try{
							checkAllTriggers();
						} catch(Exception e) {
							e.printStackTrace();
							logger.error(e.getMessage());
						} catch(Throwable t) {
							t.printStackTrace();
							logger.error(t.getMessage());
						}
						
						runnerThreadIdleTime = scannerInterval - (System.currentTimeMillis() - getLastRunnerThreadCheckTime());
						if(runnerThreadIdleTime < 0) {
							logger.error("Trigger manager thread " + this.getName() + " is too busy!");
						} else {
							wait(runnerThreadIdleTime);
						}
					} catch(InterruptedException e) {
						logger.info("Interrupted. Probably to shut down.");
					}
					
				}
			}
		}
		
		private void checkAllTriggers() throws TriggerManagerException {
			for(Trigger t : triggers) {
				logger.info("Checking trigger " + t.getDescription());
				if(t.getStatus().equals(TriggerStatus.READY)) {
					if(t.triggerConditionMet()) {
						onTriggerTrigger(t);
					} else if (t.expireConditionMet()) {
						onTriggerExpire(t);
					}
				}
			}
		}
		
		private void onTriggerTrigger(Trigger t) throws TriggerManagerException {
			List<TriggerAction> actions = t.getTriggerActions();
			for(TriggerAction action : actions) {
				try {
					logger.info("Doing trigger actions");
					action.doAction();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					//throw new TriggerManagerException("action failed to execute", e);
					logger.error("Failed to do action " + action.getDescription(), e);
				} catch (Throwable th) {
					logger.error("Failed to do action " + action.getDescription(), th);
				}
			}
			if(t.isResetOnTrigger()) {
				t.resetTriggerConditions();
				t.resetExpireCondition();
			} else {
				t.setStatus(TriggerStatus.EXPIRED);
			}
			triggerLoader.updateTrigger(t);
//			updateAgent(t);
		}
		
		private void onTriggerExpire(Trigger t) throws TriggerManagerException {
			List<TriggerAction> expireActions = t.getExpireActions();
			for(TriggerAction action : expireActions) {
				try {
					logger.info("Doing expire actions");
					action.doAction();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					//throw new TriggerManagerException("action failed to execute", e);
					logger.error("Failed to do expire action " + action.getDescription(), e);
				} catch (Throwable th) {
					logger.error("Failed to do expire action " + action.getDescription(), th);
				}
			}
			if(t.isResetOnExpire()) {
				t.resetTriggerConditions();
				t.resetExpireCondition();
//				updateTrigger(t);
			} else {
				t.setStatus(TriggerStatus.EXPIRED);
			}
			triggerLoader.updateTrigger(t);
		}
	}
	
	public synchronized Trigger getTrigger(int triggerId) {
		return triggerIdMap.get(triggerId);
	}

	public void expireTrigger(int triggerId) {
		Trigger t = getTrigger(triggerId);
		t.setStatus(TriggerStatus.EXPIRED);
//		updateAgent(t);
	}

	public List<Trigger> getTriggers(String triggerSource) {
		List<Trigger> triggers = new ArrayList<Trigger>();
		for(Trigger t : triggerIdMap.values()) {
			if(t.getSource().equals(triggerSource)) {
				triggers.add(t);
			}
		}
		return triggers;
	}

	public List<Trigger> getUpdatedTriggers(String triggerSource, long lastUpdateTime) {
		List<Trigger> triggers = new ArrayList<Trigger>();
		for(Trigger t : triggerIdMap.values()) {
			if(t.getSource().equals(triggerSource) && t.getLastModifyTime().getMillis() > lastUpdateTime) {
				triggers.add(t);
			}
		}
		return triggers;
	}
	
	public List<Integer> getUpdatedTriggers(long lastUpdateTime) {
		List<Integer> triggers = new ArrayList<Integer>();
		for(Trigger t : triggerIdMap.values()) {
			if(t.getLastModifyTime().getMillis() > lastUpdateTime) {
				triggers.add(t.getTriggerId());
			}
		}
		return triggers;
	}

	public long getLastRunnerThreadCheckTime() {
		return lastRunnerThreadCheckTime;
	}

	public boolean isRunnerThreadActive() {
		return runnerThread.isAlive();
	}


	public State getRunnerThreadState() {
		return this.runnerThread.getState();
	}

	public void loadTrigger(int triggerId) throws TriggerManagerException {
		Trigger t = triggerLoader.loadTrigger(triggerId);
		if(t.getStatus().equals(TriggerStatus.PREPARING)) {
			triggerIdMap.put(t.getTriggerId(), t);
			runnerThread.addTrigger(t);
			t.setStatus(TriggerStatus.READY);
		}
	}

	public int getNumTriggers() {
		return triggerIdMap.size();
	}

	public String getTriggerSources() {
		Set<String> sources = new HashSet<String>();
		for(Trigger t : triggerIdMap.values()) {
			sources.add(t.getSource());
		}
		return sources.toString();
	}

	public String getTriggerIds() {
		return triggerIdMap.keySet().toString();
	}

	public long getScannerIdleTime() {
		return runnerThreadIdleTime;
	}

}
