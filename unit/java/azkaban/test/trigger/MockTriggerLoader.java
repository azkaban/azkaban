package azkaban.test.trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerLoader;
import azkaban.trigger.TriggerLoaderException;

public class MockTriggerLoader implements TriggerLoader {

	Map<Integer, Trigger> triggers = new HashMap<Integer, Trigger>();
	int triggerCount = 0;
	
	@Override
	public synchronized void addTrigger(Trigger t) throws TriggerLoaderException {
		t.setTriggerId(triggerCount);
		t.setLastModifyTime(System.currentTimeMillis());
		triggers.put(t.getTriggerId(), t);
		triggerCount++;
	}

	@Override
	public synchronized void removeTrigger(Trigger s) throws TriggerLoaderException {
		triggers.remove(s);
	}

	@Override
	public synchronized void updateTrigger(Trigger t) throws TriggerLoaderException {
		t.setLastModifyTime(System.currentTimeMillis());
		triggers.put(t.getTriggerId(), t);
	}

	@Override
	public synchronized List<Trigger> loadTriggers() throws TriggerLoaderException {
		return new ArrayList<Trigger>(triggers.values());
	}

	@Override
	public synchronized Trigger loadTrigger(int triggerId) throws TriggerLoaderException {
		return triggers.get(triggerId);
	}

	@Override
	public List<Trigger> getUpdatedTriggers(long lastUpdateTime)
			throws TriggerLoaderException {
		// TODO Auto-generated method stub
		return null;
	}

}
