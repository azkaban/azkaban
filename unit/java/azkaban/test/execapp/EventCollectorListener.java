package azkaban.test.execapp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import azkaban.execapp.event.Event;
import azkaban.execapp.event.Event.Type;
import azkaban.execapp.event.EventListener;

public class EventCollectorListener implements EventListener {
	private ArrayList<Event> eventList = new ArrayList<Event>();
	private HashSet<Event.Type> filterOutTypes = new HashSet<Event.Type>();
	
	public void setEventFilterOut(Event.Type ... types) {
		filterOutTypes.addAll(Arrays.asList(types)); 
	}
	
	@Override
	public void handleEvent(Event event) {
		if (!filterOutTypes.contains(event.getType())) {
			eventList.add(event);
		}
	}

	public ArrayList<Event> getEventList() {
		return eventList;
	}
	
	public void writeAllEvents() {
		for (Event event: eventList) {
			System.out.print(event.getType());
			System.out.print(",");
		}
	}
	
	public boolean checkOrdering() {
		long time = 0;
		for (Event event: eventList) {
			if (time > event.getTime()) {
				return false;
			}
		}
		
		return true;
	}

	public void checkEventExists(Type[] types) {
		int index = 0;
		for (Event event: eventList) {
			if (event.getRunner() == null) {
				continue;
			}
			
			if (index >= types.length) {
				throw new RuntimeException("More events than expected. Got " + event.getType());
			}
			Type type = types[index++];

			if (type != event.getType()) {
				throw new RuntimeException("Got " + event.getType() + ", expected " + type + " index:" + index);
			}
		}
		
		if (types.length != index) {
			throw new RuntimeException("Not enough events.");
		}
	}
}
