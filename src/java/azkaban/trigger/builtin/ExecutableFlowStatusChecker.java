package azkaban.trigger.builtin;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.trigger.ConditionChecker;

public class ExecutableFlowStatusChecker implements ConditionChecker{
	public static final String type = "ExecutableFlowStatusChecker";
	private static Logger logger = Logger.getLogger(ExecutableFlowStatusChecker.class);
	private int execId;
	private Status status;
	private String id;
	private static ExecutorManager executorManager;
	
	public ExecutableFlowStatusChecker(int execId, Status status, String id) {
		this.execId = execId;
		this.status = status;
		this.id = id;
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
		Status flowStatus = exflow.getStatus();
		return flowStatus.equals(status);
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
	public ExecutableFlowStatusChecker fromJson(Object obj) throws Exception {
		return createFromJson(obj);
	}

	@SuppressWarnings("unchecked")
	public static ExecutableFlowStatusChecker createFromJson(Object obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new Exception("Cannot create checker of " + type + " from " + jsonObj.get("type"));
		}
		String id = (String) jsonObj.get("id");
		int execId = Integer.valueOf((String) jsonObj.get("execId"));
		Status status = Status.valueOf((String) jsonObj.get("status"));
		return new ExecutableFlowStatusChecker(execId, status, id);
	}
	
	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("type", type);
		jsonObj.put("execId", String.valueOf(execId));
		jsonObj.put("status", status.toString());
		jsonObj.put("id", id);
		return jsonObj;
	}

	@Override
	public void stopChecker() {
		// TODO Auto-generated method stub
		
	}

	
}
