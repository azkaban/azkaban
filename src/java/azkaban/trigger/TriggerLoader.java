package azkaban.trigger;

import java.util.List;
import java.util.Map;



public interface TriggerLoader {

	public void addTrigger(Trigger t) throws TriggerManagerException;	

	public void removeTrigger(Trigger s) throws TriggerManagerException;
	
	public void updateTrigger(Trigger t) throws TriggerManagerException;
	
	public List<Trigger> loadTriggers() throws TriggerManagerException;	
	
}
