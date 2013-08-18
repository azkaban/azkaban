package azkaban.trigger.builtin;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.trigger.ConditionChecker;

public class ExecutionChecker implements ConditionChecker{
	public static final String type = "ExecutableFlowStatusChecker";
	private static Logger logger = Logger.getLogger(ExecutionChecker.class);
	private int execId;
	private String target;
	private String id;
	private static ExecutorManager executorManager;
	private String jobName = null;
	
	public static final String TARGET_FINISHED = "finished";
	public static final String TARGET_SUCCEED = "succeeded";
	public static final String TARGET_STARTED = "started";
	
	
	public ExecutionChecker(String id, int execId, String target, String jobName) {
		this.execId = execId;
		this.target = target;
		this.id = id;
		this.jobName = null;
	}
	
	public static void setExecutorManager(ExecutorManager em) {
		executorManager = em;
	}
	
	@Override
	public Object eval() {
		ExecutableFlow exflow;
		try {
			exflow = executorManager.fetchExecutableFlow(execId);
		} catch (ExecutorManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Failed to get executable flow status.");
			return Boolean.FALSE;
		}
//		Status flowStatus = exflow.getStatus();
//		return flowStatus.equals(status);
//		if(jobName != null) {
//			Status jobStatus = exflow.getExecutableNode(jobName).getStatus();
//			return jobStatus.equals(target);
//		} else {
//			Status flowStatus = exflow.getStatus();
//			return flowStatus.equals(target);
//		}
		Status status;
		if(jobName != null) {
			status = exflow.getExecutableNode(jobName).getStatus();
		} else {
			status = exflow.getStatus();
		}
		if(target.equals(TARGET_SUCCEED)) {
			return status.equals(Status.SUCCEEDED);
		} else if (target.equals(TARGET_FINISHED)) {
			return status.equals(Status.SUCCEEDED) || status.equals(Status.FAILED);
		} else if (target.equals("TARGET_STARTED")) {
			return !(status.equals(Status.READY) && status.equals(Status.PREPARING));
		} else {
			logger.error("Unknown Execution Check target.");
			return false;
		}
	}

	@Override
	public Object getNum() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return type;
	}

	@Override
	public ExecutionChecker fromJson(Object obj) throws Exception {
		return createFromJson(obj);
	}

	@SuppressWarnings("unchecked")
	public static ExecutionChecker createFromJson(Object obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new Exception("Cannot create checker of " + type + " from " + jsonObj.get("type"));
		}
		String id = (String) jsonObj.get("id");
		int execId = Integer.valueOf((String) jsonObj.get("execId"));
		String target = (String) jsonObj.get("target");
		String jobName = null;
		if(jsonObj.containsKey("jobName")) {
			jobName = (String) jsonObj.get("jobName");
		}
		return new ExecutionChecker(id, execId, target, jobName);
	}
	
	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("type", type);
		jsonObj.put("execId", String.valueOf(execId));
		jsonObj.put("target", target);
		jsonObj.put("id", id);
		if(jobName != null) {
			jsonObj.put("jobName", jobName);
		}
		return jsonObj;
	}

	@Override
	public void stopChecker() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setContext(Map<String, Object> context) {
		// TODO Auto-generated method stub
		
	}

	
}
