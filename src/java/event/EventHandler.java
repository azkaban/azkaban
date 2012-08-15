package event;

import java.util.HashSet;

public class EventHandler {
	private HashSet<EventListener> listeners = new HashSet<EventListener>();
	
	public EventHandler() {
	}

	public void addListener(EventListener listener) {
		listeners.add(listener);
	}
	
	public void fireEventListeners(Event event) {
		for (EventListener listener: listeners) {
			listener.handleEvent(event);
		}
	}
}
