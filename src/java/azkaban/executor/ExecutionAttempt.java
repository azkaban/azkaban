package azkaban.executor;

import java.util.HashMap;
import java.util.Map;

import azkaban.utils.JSONUtils;

public class ExecutionAttempt {
	public static final String ATTEMPT_PARAM = "attempt";
	public static final String STATUS_PARAM = "status";
	public static final String STARTTIME_PARAM = "startTime";
	public static final String ENDTIME_PARAM = "endTime";
	
	private int attempt = 0;
	private long startTime = -1;
	private long endTime = -1;
	private Status status;
	
	public ExecutionAttempt(int attempt, ExecutableNode executable) {
		this.attempt = attempt;
		this.startTime = executable.getStartTime();
		this.endTime = executable.getEndTime();
		this.status = executable.getStatus();
	}
	
	public ExecutionAttempt(int attempt, long startTime, long endTime, Status status) {
		this.attempt = attempt;
		this.startTime = startTime;
		this.endTime = endTime;
		this.status = status;
	}
	
	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public int getAttempt() {
		return attempt;
	}
	
	public static ExecutionAttempt fromObject(Object obj) {
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>)obj;
		int attempt = (Integer)map.get(ATTEMPT_PARAM);
		long startTime = JSONUtils.getLongFromObject(map.get(STARTTIME_PARAM));
		long endTime = JSONUtils.getLongFromObject(map.get(ENDTIME_PARAM));
		Status status = Status.valueOf((String)map.get(STATUS_PARAM));
		
		return new ExecutionAttempt(attempt, startTime, endTime, status);
	}
	
	public Map<String, Object> toObject() {
		HashMap<String,Object> attempts = new HashMap<String,Object>();
		attempts.put(ATTEMPT_PARAM, attempt);
		attempts.put(STARTTIME_PARAM, startTime);
		attempts.put(ENDTIME_PARAM, endTime);
		attempts.put(STATUS_PARAM, status.toString());
		return attempts;
	}
}