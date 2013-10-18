package azkaban.trigger;

import java.util.List;

public interface TriggerLoader {

	public void addTrigger(Trigger t) throws TriggerLoaderException;	

	public void removeTrigger(Trigger s) throws TriggerLoaderException;
	
	public void updateTrigger(Trigger t) throws TriggerLoaderException;
	
	public List<Trigger> loadTriggers() throws TriggerLoaderException;

	public Trigger loadTrigger(int triggerId) throws TriggerLoaderException;

	public List<Trigger> getUpdatedTriggers(long lastUpdateTime) throws TriggerLoaderException;
	
}
