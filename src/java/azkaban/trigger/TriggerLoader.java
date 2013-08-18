package azkaban.trigger;

import java.util.List;

public interface TriggerLoader {

	public void addTrigger(Trigger t) throws TriggerManagerException;	

	public void removeTrigger(Trigger s) throws TriggerManagerException;
	
	public void updateTrigger(Trigger t) throws TriggerManagerException;
	
	public List<Trigger> loadTriggers() throws TriggerManagerException;

	public Trigger loadTrigger(int triggerId) throws TriggerManagerException;

	public List<Trigger> getUpdatedTriggers(long lastUpdateTime) throws TriggerManagerException;
	
}
