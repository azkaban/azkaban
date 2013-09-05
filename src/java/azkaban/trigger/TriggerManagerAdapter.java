package azkaban.trigger;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTimeZone;

import azkaban.triggerapp.TriggerRunnerManagerException;

public interface TriggerManagerAdapter {
	public void insertTrigger(Trigger t, String user) throws TriggerManagerException;
	
	public void removeTrigger(int id, String user) throws TriggerManagerException;
	
	public void updateTrigger(int triggerId, String user) throws TriggerManagerException;
	
	void updateTrigger(Trigger t, String user) throws TriggerManagerException;

	public void insertTrigger(int triggerId, String user) throws TriggerManagerException;

	public List<Integer> getTriggerUpdates(long lastUpdateTime) throws TriggerManagerException;
	
	public List<Trigger> getTriggerUpdates(String triggerSource, long lastUpdateTime) throws TriggerManagerException;

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
	}
	
}
