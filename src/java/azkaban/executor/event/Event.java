package azkaban.executor.event;

import azkaban.executor.FlowRunner;

public class Event {
	public enum Type {
		FLOW_STARTED,
		FLOW_FINISHED,
		JOB_STARTED,
		JOB_COMPLETE,
		JOB_FAILED
	}
	
	private final FlowRunner runner;
	private final Type type;
	private final Object eventData;
	private final long time;
	
	public Event(FlowRunner runner, Type type, Object eventData) {
		this.runner = runner;
		this.type = type;
		this.eventData = eventData;
		this.time = System.currentTimeMillis();
	}
	
	public FlowRunner getFlowRunner() {
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
}
