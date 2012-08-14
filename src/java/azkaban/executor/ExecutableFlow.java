package azkaban.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;
import azkaban.utils.Props;

public class ExecutableFlow {
	private String executionId;
	private String flowId;
	private String projectId;
	private String executionPath;
	private HashMap<String, FlowProps> flowProps = new HashMap<String, FlowProps>();
	private HashMap<String, ExecutableNode> executableNodes = new HashMap<String, ExecutableNode>();;
	private ArrayList<String> startNodes = new ArrayList<String>();
	
	private long submitTime = -1;
	private long startTime = -1;
	private long endTime = -1;
	
	private Status flowStatus = Status.UNKNOWN;

	private String submitUser;
	
	public enum Status {
		FAILED, SUCCEEDED, RUNNING, WAITING, IGNORED, READY, UNKNOWN
	}
	
	public ExecutableFlow(String id, Flow flow) {
		this.executionId = id;
		this.projectId = flow.getProjectId();
		this.flowId = flow.getId();
		
		this.setFlow(flow);
	}
	
	public ExecutableFlow() {
	}
	
	public List<ExecutableNode> getExecutableNodes() {
		return new ArrayList<ExecutableNode>(executableNodes.values());
	}
	
	private void setFlow(Flow flow) {
		for (Node node: flow.getNodes()) {
			String id = node.getId();
			ExecutableNode exNode = new ExecutableNode(node);
			executableNodes.put(id, exNode);
		}
		
		for (Edge edge: flow.getEdges()) {
			ExecutableNode sourceNode = executableNodes.get(edge.getSourceId());
			ExecutableNode targetNode = executableNodes.get(edge.getTargetId());
			
			sourceNode.addOutNode(edge.getTargetId());
			targetNode.addInNode(edge.getSourceId());
		}
		
		for (ExecutableNode node : executableNodes.values()) {
			if (node.getInNodes().size()==0) {
				startNodes.add(node.id);
			}
		}
		
		flowProps.putAll(flow.getAllFlowProps());
	}
	
	public void run() {
		
	}

	public void setStatus(String nodeId, Status status) {
		ExecutableNode exNode = executableNodes.get(nodeId);
		exNode.setStatus(status);
	}
	
	public String getExecutionId() {
		return executionId;
	}

	public void setExecutionId(String executionId) {
		this.executionId = executionId;
	}

	public String getFlowId() {
		return flowId;
	}

	public void setFlowId(String flowId) {
		this.flowId = flowId;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getExecutionPath() {
		return executionPath;
	}

	public void setExecutionPath(String executionPath) {
		this.executionPath = executionPath;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public void setStartTime(long time) {
		this.startTime = time;
	}
	
	public long getEndTime() {
		return endTime;
	}
	
	public void setEndTime(long time) {
		this.endTime = time;
	}
	
	public long getSubmitTime() {
		return submitTime;
	}
	
	public void setSubmitTime(long time) {
		this.submitTime = time;
	}
	
	public Status getStatus() {
		return flowStatus;
	}

	public void setStatus(Status flowStatus) {
		this.flowStatus = flowStatus;
	}
	
	public Map<String,Object> toObject() {
		HashMap<String, Object> flowObj = new HashMap<String, Object>();
		flowObj.put("type", "executableflow");
		flowObj.put("executionId", executionId);
		flowObj.put("executionPath", executionPath);
		flowObj.put("flowId", flowId);
		flowObj.put("projectId", projectId);
		flowObj.put("submitTime", submitTime);
		flowObj.put("startTime", startTime);
		flowObj.put("endTime", endTime);
		flowObj.put("status", flowStatus.toString());
		flowObj.put("submitUser", submitUser);
		
		ArrayList<Object> nodes = new ArrayList<Object>();
		for (ExecutableNode node: executableNodes.values()) {
			nodes.add(node.toObject());
		}
		flowObj.put("nodes", nodes);

		return flowObj;
	}

	@SuppressWarnings("unchecked")
	public static ExecutableFlow createExecutableFlowFromObject(Object obj) {
		ExecutableFlow exFlow = new ExecutableFlow();
		
		HashMap<String, Object> flowObj = (HashMap<String,Object>)obj;
		exFlow.executionId = (String)flowObj.get("executionId");
		exFlow.executionPath = (String)flowObj.get("executionPath");
		exFlow.flowId = (String)flowObj.get("flowId");
		exFlow.projectId = (String)flowObj.get("projectId");
		exFlow.submitTime = getLongFromObject(flowObj.get("submitTime"));
		exFlow.startTime = getLongFromObject(flowObj.get("startTime"));
		exFlow.endTime = getLongFromObject(flowObj.get("endTime"));
		exFlow.flowStatus = Status.valueOf((String)flowObj.get("status"));
		exFlow.submitUser = (String)flowObj.get("submitUser");
				
		List<Object> nodes = (List<Object>)flowObj.get("nodes");
		for (Object nodeObj: nodes) {
			ExecutableNode node = ExecutableNode.createNodeFromObject(nodeObj);
			exFlow.executableNodes.put(node.getId(), node);
		}
		
		return exFlow;
	}
	
	private static long getLongFromObject(Object obj) {
		if (obj instanceof Integer) {
			return Long.valueOf((Integer)obj);
		}
		
		return (Long)obj;
	}
	
	@SuppressWarnings("unchecked")
	public void updateExecutableFlowFromObject(Object obj) {
		HashMap<String, Object> flowObj = (HashMap<String,Object>)obj;

		submitTime = (Long)flowObj.get("submitTime");
		startTime = (Long)flowObj.get("startTime");
		endTime = (Long)flowObj.get("endTime");
		flowStatus = Status.valueOf((String)flowObj.get("status"));
		
		List<Object> nodes = (List<Object>)flowObj.get("nodes");
		for (Object nodeObj: nodes) {
			HashMap<String, Object> nodeHash= (HashMap<String, Object>)nodeObj;
			String nodeId = (String)nodeHash.get("id");
			ExecutableNode node = executableNodes.get(nodeId);
			if (nodeId == null) {
				throw new RuntimeException("Node " + nodeId + " doesn't exist in flow.");
			}
			
			node.updateNodeFromObject(nodeObj);
		}
	}
	
	public Set<String> getSources() {
		HashSet<String> set = new HashSet<String>();
		for (ExecutableNode exNode: executableNodes.values()) {
			set.add(exNode.getJobPropsSource());
		}
		
		for (FlowProps props: flowProps.values()) {
			set.add(props.getSource());
		}
		return set;
	}
	
	public String getSubmitUser() {
		return submitUser;
	}

	public void setSubmitUser(String submitUser) {
		this.submitUser = submitUser;
	}

	public static class ExecutableNode {
		private String id;

		private String type;
		private String jobPropsSource;
		private String inheritPropsSource;
		private Status status = Status.READY;
		private long startTime = -1;
		private long endTime = -1;
		private int level = 0;

		private Set<String> inNodes = new HashSet<String>();
		private Set<String> outNodes = new HashSet<String>();
		
		private ExecutableNode(Node node) {
			id = node.getId();
			type = node.getType();
			jobPropsSource = node.getJobSource();
			inheritPropsSource = node.getPropsSource();
			status = Status.READY;
			level = node.getLevel();
		}
		
		private ExecutableNode() {
			
		}
		
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
		
		public void addInNode(String exNode) {
			inNodes.add(exNode);
		}
		
		public void addOutNode(String exNode) {
			outNodes.add(exNode);
		}
		
		public Set<String> getOutNodes() {
			return outNodes;
		}
		
		public Set<String> getInNodes() {
			return inNodes;
		}
		
		public Status getStatus() {
			return status;
		}
		
		public void setStatus(Status status) {
			this.status = status;
		}
		
		public Object toObject() {
			HashMap<String, Object> objMap = new HashMap<String, Object>();
			objMap.put("id", id);
			objMap.put("jobSource", jobPropsSource);
			objMap.put("propSource", inheritPropsSource);
			objMap.put("jobType", type);
			objMap.put("status", status.toString());
			objMap.put("inNodes", inNodes);
			objMap.put("outNodes", outNodes);
			objMap.put("startTime", startTime);
			objMap.put("endTime", endTime);
			objMap.put("level", level);
			return objMap;
		}

		@SuppressWarnings("unchecked")
		public static ExecutableNode createNodeFromObject(Object obj) {
			ExecutableNode exNode = new ExecutableNode();
			
			HashMap<String, Object> objMap = (HashMap<String,Object>)obj;
			exNode.id = (String)objMap.get("id");
			exNode.jobPropsSource = (String)objMap.get("jobSource");
			exNode.inheritPropsSource = (String)objMap.get("propSource");
			exNode.type = (String)objMap.get("jobType");
			exNode.status = Status.valueOf((String)objMap.get("status"));
			
			exNode.inNodes.addAll( (List<String>)objMap.get("inNodes") );
			exNode.outNodes.addAll( (List<String>)objMap.get("outNodes") );
			
			exNode.startTime = getLongFromObject(objMap.get("startTime"));
			exNode.endTime = getLongFromObject(objMap.get("endTime"));
			exNode.level = (Integer)objMap.get("level");
			
			return exNode;
		}
		
		@SuppressWarnings("unused")
		public void updateNodeFromObject(Object obj) {
			HashMap<String, Object> objMap = (HashMap<String,Object>)obj;
			status = Status.valueOf((String)objMap.get("status"));

			startTime = (Long)objMap.get("startTime");
			endTime = (Long)objMap.get("endTime");
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
		
		public String getJobPropsSource() {
			return jobPropsSource;
		}

		public String getPropsSource() {
			return inheritPropsSource;
		}
		
		public int getLevel() {
			return level;
		}
	}
}
