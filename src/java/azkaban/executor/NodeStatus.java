package azkaban.executor;

import java.util.HashMap;

import azkaban.executor.ExecutableFlow.Status;
import azkaban.utils.JSONUtils;

// Will need to remove these as we roll out database instead
public class NodeStatus {
	private final String execId;
	private final String projectId;
	private final String jobId;
	private final String flowId;
	private Status status;
	private long startTime;
	private long endTime;
	
	private NodeStatus(String execId, String projectId, String jobId, String flowId) {
		this.execId = execId;
		this.projectId = projectId;
		this.jobId = jobId;
		this.flowId = flowId;
	}
	
	public NodeStatus(ExecutableNode node) {
		this.execId = node.getFlow().getExecutionId();
		this.projectId = node.getFlow().getProjectId();
		this.jobId = node.getId();
		this.status = node.getStatus();
		this.startTime = node.getStartTime();
		this.endTime = node.getEndTime();
		this.flowId = node.getFlow().getFlowId();
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getExecId() {
		return execId;
	}

	public String getProjectId() {
		return projectId;
	}

	public String getJobId() {
		return jobId;
	}
	
	public String getFlowId() {
		return flowId;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	
	public Object toObject() {
		HashMap<String, Object> objMap = new HashMap<String, Object>();
		objMap.put("execId", execId);
		objMap.put("jobId", jobId);
		objMap.put("status", status.toString());
		objMap.put("startTime", startTime);
		objMap.put("endTime", endTime);
		objMap.put("flowId", flowId);
		return objMap;
	}
	
	public static NodeStatus createNodeFromObject(Object obj) {
		@SuppressWarnings("unchecked")
		HashMap<String, Object> objMap = (HashMap<String, Object>)obj;
		String execId = (String)objMap.get("execId");
		String projectId = (String)objMap.get("projectId");
		String jobId = (String)objMap.get("jobId");
		String flowId = (String)objMap.get("flowId");
		
		NodeStatus nodeStatus = new NodeStatus(execId, projectId, jobId, flowId);
		Status status = Status.valueOf((String)objMap.get("status"));
		long startTime = JSONUtils.getLongFromObject(objMap.get("startTime"));
		long endTime = JSONUtils.getLongFromObject(objMap.get("endTime"));
		
		nodeStatus.status = status;
		nodeStatus.startTime = startTime;
		nodeStatus.endTime = endTime;
		
		return nodeStatus;
	}
}