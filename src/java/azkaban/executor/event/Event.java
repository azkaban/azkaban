package azkaban.executor.event;

public class Event {
	public enum Type {
		FLOW_STARTED,
		FLOW_FINISHED,
		FLOW_FAILED_FINISHING,
		JOB_STARTED,
		JOB_SUCCEEDED,
		JOB_FAILED,
		JOB_KILLED,
		JOB_SKIPPED
	}
	
	private final Object runner;
	private final Type type;
	private final Object eventData;
	private final long time;
	
	private Event(Object runner, Type type, Object eventData) {
		this.runner = runner;
		this.type = type;
		this.eventData = eventData;
		this.time = System.currentTimeMillis();
	}
	
	public Object getRunner() {
		return runner;
	}
	
	public Type getType() {
		return type;
	}
	
	public long getTime() {
		return time;
	}
	
	public Object getData() {
		return eventData;
	}
	
	public static Event create(Object runner, Type type) {
		return new Event(runner, type, null);
	}
	
	public static Event create(Object runner, Type type, Object eventData) {
		return new Event(runner, type, eventData);
	}
}
