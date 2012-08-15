package azkaban.executor.event;

public interface EventListener {
	public void handleEvent(Event event);
}
