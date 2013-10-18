package azkaban.trigger;

import java.util.List;
import java.util.Map;


public interface TriggerManagerAdapter {
	
	public void insertTrigger(Trigger t, String user) throws TriggerManagerException;
	
	public void removeTrigger(int id, String user) throws TriggerManagerException;
	
	public void updateTrigger(Trigger t, String user) throws TriggerManagerException;

	public List<Trigger> getAllTriggerUpdates(long lastUpdateTime) throws TriggerManagerException;
	
	public List<Trigger> getTriggerUpdates(String triggerSource, long lastUpdateTime) throws TriggerManagerException;
	
	public List<Trigger> getTriggers(String trigegerSource);

	public void start() throws TriggerManagerException;
	
	public void shutdown();

	public void registerCheckerType(String name, Class<? extends ConditionChecker> checker);
	
	public void registerActionType(String name, Class<? extends TriggerAction> action);
	
	public TriggerJMX getJMX();
	
	public interface TriggerJMX {
		public long getLastRunnerThreadCheckTime();
		public boolean isRunnerThreadActive();
		public String getPrimaryServerHost();
		public int getNumTriggers();
		public String getTriggerSources();
		public String getTriggerIds();
		public long getScannerIdleTime();
		public Map<String, Object> getAllJMXMbeans();
		public String getScannerThreadStage();
	}
	
}
