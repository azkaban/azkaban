package azkaban.trigger;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class TriggerManager {
	private static Logger logger = Logger.getLogger(TriggerManager.class);
	
	private static final String TRIGGER_SUFFIX = ".trigger";
	
	private static Map<Integer, Trigger> triggerIdMap = new HashMap<Integer, Trigger>();
	
	private CheckerTypeLoader checkerLoader;
	private ActionTypeLoader actionLoader;
	
	private static TriggerLoader triggerLoader;
	
	private static TriggerScannerThread scannerThread;
	
	private Map<String, TriggerAgent> triggerAgents = new HashMap<String, TriggerAgent>();
	
	public TriggerManager(Props props, TriggerLoader triggerLoader) {
		
		TriggerManager.triggerLoader = triggerLoader;
		checkerLoader = new CheckerTypeLoader();
		actionLoader = new ActionTypeLoader();
		
		// load plugins
		try{
			checkerLoader.init(props);
			actionLoader.init(props);
		} catch(Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
		Condition.setCheckerLoader(checkerLoader);
		Trigger.setActionTypeLoader(actionLoader);
		
		checkerLoader = new CheckerTypeLoader();
		actionLoader = new ActionTypeLoader();
		
		long scannerInterval = props.getLong("trigger.scan.interval", TriggerScannerThread.DEFAULT_SCAN_INTERVAL_MS);
		scannerThread = new TriggerScannerThread(scannerInterval);
		scannerThread.setName("TriggerScannerThread");
		
	}
	
	private static class SuffixFilter implements FileFilter {
		private String suffix;
		
		public SuffixFilter(String suffix) {
			this.suffix = suffix;
		}

		@Override
		public boolean accept(File pathname) {
			String name = pathname.getName();
			
			return pathname.isFile() && !pathname.isHidden() && name.length() > suffix.length() && name.endsWith(suffix);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void loadTriggerFromDir(File baseDir, Props props) throws Exception {
		File[] triggerFiles = baseDir.listFiles(new SuffixFilter(TRIGGER_SUFFIX));
		
		for(File triggerFile : triggerFiles) {
			Props triggerProps = new Props(props, triggerFile);
			String triggerType = triggerProps.getString("trigger.type");
			TriggerAgent agent = triggerAgents.get(triggerType);
			if(agent != null) {
				agent.loadTriggerFromProps(triggerProps);
			} else {
				throw new Exception("Trigger " + triggerType + " is not supported.");
			}
		}
	}
	
	public void addTriggerAgent(String triggerSource, TriggerAgent agent) throws TriggerManagerException {
		if(triggerAgents.containsKey(triggerSource)) {
			throw new TriggerManagerException("Trigger agent " + triggerSource + " already exists!" );
		}
		this.triggerAgents.put(triggerSource, agent);
	}
	
	public void start() {
		
		try{
			// expect loader to return valid triggers
			List<Trigger> triggers = triggerLoader.loadTriggers();
			for(Trigger t : triggers) {
				scannerThread.addTrigger(t);
				triggerIdMap.put(t.getTriggerId(), t);
			}
		}catch(Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
		for(TriggerAgent agent : triggerAgents.values()) {
			agent.load();
		}
		
		scannerThread.start();
	}
	
	public CheckerTypeLoader getCheckerLoader() {
		return checkerLoader;
	}

	public ActionTypeLoader getActionLoader() {
		return actionLoader;
	}

	public synchronized void insertTrigger(Trigger t) throws TriggerManagerException {
		
		triggerLoader.addTrigger(t);
		triggerIdMap.put(t.getTriggerId(), t);
		scannerThread.addTrigger(t);
	}
	
	public synchronized void removeTrigger(int id) throws TriggerManagerException {
		Trigger t = triggerIdMap.get(id);
		if(t != null) {
			removeTrigger(triggerIdMap.get(id));
		}
	}
	
	//TODO: update corresponding agents
	public synchronized void updateTrigger(Trigger t) throws TriggerManagerException {
		if(!triggerIdMap.containsKey(t.getTriggerId())) {
			throw new TriggerManagerException("The trigger to update doesn't exist!");
		}
		
		scannerThread.deleteTrigger(t);
		scannerThread.addTrigger(t);
		triggerIdMap.put(t.getTriggerId(), t);
		
		triggerLoader.updateTrigger(t);
	}

	//TODO: update corresponding agents
	public synchronized void removeTrigger(Trigger t) throws TriggerManagerException {
		t.stopCheckers();
		triggerLoader.removeTrigger(t);
		scannerThread.deleteTrigger(t);
		triggerIdMap.remove(t.getTriggerId());		
	}
	
	public List<Trigger> getTriggers() {
		return new ArrayList<Trigger>(triggerIdMap.values());
	}
	
	public Map<String, Class<? extends ConditionChecker>> getSupportedCheckers() {
		return checkerLoader.getSupportedCheckers();
	}

//	private void updateAgent(Trigger t) {
//		TriggerAgent agent = triggerAgents.get(t.getSource());
//		if(agent != null) {
//			agent.updateLocal(t);
//		}
//		
//	}
	
	//trigger scanner thread
	public class TriggerScannerThread extends Thread {
		
		//public static final long DEFAULT_SCAN_INTERVAL_MS = 300000;
		public static final long DEFAULT_SCAN_INTERVAL_MS = 60000;
		
		private final BlockingQueue<Trigger> triggers;
		private AtomicBoolean stillAlive = new AtomicBoolean(true);
		private long lastCheckTime = -1;
		private final long scanInterval;
		
		// Five minute minimum intervals
		
		public TriggerScannerThread(){
			triggers = new LinkedBlockingDeque<Trigger>();
			this.scanInterval = DEFAULT_SCAN_INTERVAL_MS;
		}

		public TriggerScannerThread(long interval){
			triggers = new LinkedBlockingDeque<Trigger>();
			this.scanInterval = interval;
		}
		
		public void shutdown() {
			logger.error("Shutting down trigger manager thread " + this.getName());
			stillAlive.set(false);
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
			while(stillAlive.get()) {
				synchronized (this) {
					try{
						lastCheckTime = System.currentTimeMillis();
						
						try{
							checkAllTriggers();
						} catch(Exception e) {
							e.printStackTrace();
							logger.error(e.getMessage());
						} catch(Throwable t) {
							t.printStackTrace();
							logger.error(t.getMessage());
						}
						
						long timeRemaining = scanInterval - (System.currentTimeMillis() - lastCheckTime);
						if(timeRemaining < 0) {
							logger.error("Trigger manager thread " + this.getName() + " is too busy!");
						} else {
							wait(timeRemaining);
						}
					} catch(InterruptedException e) {
						logger.info("Interrupted. Probably to shut down.");
					}
					
				}
			}
		}
		
		private void checkAllTriggers() throws TriggerManagerException {
			for(Trigger t : triggers) {
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
					action.doAction();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					throw new TriggerManagerException("action failed to execute", e);
				}
			}
			if(t.isResetOnTrigger()) {
				t.resetTriggerConditions();
				t.resetExpireCondition();
				updateTrigger(t);
			} else {
				t.setStatus(TriggerStatus.EXPIRED);
			}
//			updateAgent(t);
		}
		
		private void onTriggerExpire(Trigger t) throws TriggerManagerException {
			if(t.isResetOnExpire()) {
				t.resetTriggerConditions();
				t.resetExpireCondition();
				updateTrigger(t);
			} else {
				t.setStatus(TriggerStatus.EXPIRED);
			}
//			updateAgent(t);
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

}
