package azkaban.scheduler;

import java.io.IOException;
import java.util.ArrayList;

import org.joda.time.DateTime;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;
//import azkaban.flow.FlowExecutionHolder;
import azkaban.executor.ExecutorManager;

public class MockJobExecutorManager extends ExecutorManager {
	
	private ArrayList<ExecutionRecord> executionList = new ArrayList<ExecutionRecord>();
	private RuntimeException throwException = null;
	
	public MockJobExecutorManager() throws IOException, ExecutorManagerException 
	{
		super(null);
	}
	
	public void setThrowException(RuntimeException throwException) {
		this.throwException = throwException;
	}
	
//	@Override
//    public void execute(String id, boolean ignoreDep) {
//		DateTime time = new DateTime();
//    	executionList.add(new ExecutionRecord(id, ignoreDep, time, throwException));
//    	System.out.println("Running " + id + " at time " + time);
//    	if (throwException != null) {
//    		throw throwException;
//    	}
//    }

	@Override
    public void executeFlow(ExecutableFlow flow) {
		//System.out.println("Did not expect");
		DateTime nextExecTime = new DateTime();
		DateTime submitTime = new DateTime();
		DateTime firstSchedTime = new DateTime();
		String scheduleId = flow.getProjectId()+"."+flow.getFlowId();
    	executionList.add(new ExecutionRecord(scheduleId, "pymk", "cyu", submitTime, firstSchedTime, nextExecTime, throwException));
    	System.out.println("Running " + scheduleId + " at time " + nextExecTime);
    	if (throwException != null) {
    		throw throwException;
    	}
	}

//	@Override
////    public void execute(FlowExecutionHolder holder) {
////		System.out.println("Did not expect");
////    }
	
	public ArrayList<ExecutionRecord> getExecutionList() {
		return executionList;
	}
	
	public void clearExecutionList() {
		executionList.clear();
	}
	
    public class ExecutionRecord {
    	private final String scheduleId;
    	private final String user;
    	private final String userSubmit;
    	private final DateTime submitTime;
    	private final DateTime firstSchedTime;
    	private final DateTime nextExecTime;
    	private final Exception throwException;
    	
    	public ExecutionRecord(String scheduleId, String user, String userSubmit, DateTime submitTime, DateTime firstSchedTime, DateTime nextExecTime) {
    		this(scheduleId, user, userSubmit, submitTime, firstSchedTime, nextExecTime, null);
    	}
    	
    	public ExecutionRecord(String scheduleId, String user, String userSubmit, DateTime submitTime, DateTime firstSchedTime, DateTime nextExecTime, Exception throwException) {
    		this.scheduleId = scheduleId;
    		this.user = user;
    		this.userSubmit = userSubmit;
    		this.submitTime = submitTime;
    		this.firstSchedTime = firstSchedTime;
    		this.nextExecTime = nextExecTime;
    		this.throwException = throwException;
    	}

		public String getScheduleId() {
			return scheduleId;
		}

		public DateTime getNextExecTime() {
			return nextExecTime;
		}

		public Exception getThrowException() {
			return throwException;
		}
    }
}