package azkaban.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;

public class ExecutableFlow {
	private String executionId;
	private String flowId;
	private String projectId;
	private String executionPath;
	private long lastCheckedTime;
	
	private HashMap<String, FlowProps> flowProps = new HashMap<String, FlowProps>();
	private HashMap<String, ExecutableNode> executableNodes = new HashMap<String, ExecutableNode>();;
	private ArrayList<String> startNodes;
	private ArrayList<String> endNodes;
	
	private ArrayList<String> failureEmails;
	private ArrayList<String> successEmails;
	
	private long submitTime = -1;
	private long startTime = -1;
	private long endTime = -1;
	
	private int updateNumber = 0;
	private Status flowStatus = Status.UNKNOWN;
	private String submitUser;
	private boolean submitted = false;
	private boolean notifyOnFirstFailure = true;
	private boolean notifyOnLastFailure = false;
	
	private Integer pipelineLevel = null;
	private Map<String, String> flowParameters = new HashMap<String, String>();
	
	public enum FailureAction {
		FINISH_CURRENTLY_RUNNING,
		CANCEL_ALL,
		FINISH_ALL_POSSIBLE
	}
	
	private FailureAction failureAction = FailureAction.FINISH_CURRENTLY_RUNNING;
	
	public enum Status {
		FAILED, FAILED_FINISHING, SUCCEEDED, RUNNING, WAITING, KILLED, DISABLED, READY, UNKNOWN, PAUSED, SKIPPED
	}
	
	public ExecutableFlow(String id, Flow flow) {
		this.executionId = id;
		this.projectId = flow.getProjectId();
		this.flowId = flow.getId();
		
		this.setFlow(flow);
	}
	
	public ExecutableFlow() {
	}
	
	public long getLastCheckedTime() {
		return lastCheckedTime;
	}
	
	public void setLastCheckedTime(long lastCheckedTime) {
		this.lastCheckedTime = lastCheckedTime;
	}
	
	public List<ExecutableNode> getExecutableNodes() {
		return new ArrayList<ExecutableNode>(executableNodes.values());
	}
	
	public ExecutableNode getExecutableNode(String id) {
		return executableNodes.get(id);
	}
	
	public Collection<FlowProps> getFlowProps() {
		return flowProps.values();
	}
	
	public int getUpdateNumber() {
		return updateNumber;
	}
	
	public void setUpdateNumber(int number) {
		updateNumber = number;
	}
	
	public void addFlowParameters(Map<String, String> param) {
		flowParameters.putAll(param);
	}
	
	public Map<String, String> getFlowParameters() {
		return flowParameters;
	}
	
	private void setFlow(Flow flow) {
		for (Node node: flow.getNodes()) {
			String id = node.getId();
			ExecutableNode exNode = new ExecutableNode(node, this);
			executableNodes.put(id, exNode);
		}
		
		for (Edge edge: flow.getEdges()) {
			ExecutableNode sourceNode = executableNodes.get(edge.getSourceId());
			ExecutableNode targetNode = executableNodes.get(edge.getTargetId());
			
			sourceNode.addOutNode(edge.getTargetId());
			targetNode.addInNode(edge.getSourceId());
		}
		
		successEmails = new ArrayList<String>(flow.getSuccessEmails());
		failureEmails = new ArrayList<String>(flow.getFailureEmails());
		flowProps.putAll(flow.getAllFlowProps());
	}

	public List<String> getStartNodes() {
		if (startNodes == null) {
			startNodes = new ArrayList<String>();
			for (ExecutableNode node: executableNodes.values()) {
				if (node.getInNodes().isEmpty()) {
					startNodes.add(node.id);
				}
			}
		}
		
		return startNodes;
	}
	
	public List<String> getEndNodes() {
		if (endNodes == null) {
			endNodes = new ArrayList<String>();
			for (ExecutableNode node: executableNodes.values()) {
				if (node.getOutNodes().isEmpty()) {
					endNodes.add(node.id);
				}
			}
		}
		
		return endNodes;
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
	
	public void setFailureEmails(List<String> emails) {
		this.failureEmails = emails == null ? new ArrayList<String>() : new ArrayList<String>(emails);
	}
	
	public void setSuccessEmails(List<String> emails) {
		this.successEmails = emails == null ? new ArrayList<String>() : new ArrayList<String>(emails);
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
		flowObj.put("flowParameters", this.flowParameters);
		flowObj.put("notifyOnFirstFailure", this.notifyOnFirstFailure);
		flowObj.put("notifyOnLastFailure", this.notifyOnLastFailure);
		flowObj.put("successEmails", successEmails);
		flowObj.put("failureEmails", failureEmails);
		flowObj.put("failureAction", failureAction.toString());
		flowObj.put("pipelineLevel", pipelineLevel);
		
		ArrayList<Object> props = new ArrayList<Object>();
		for (FlowProps fprop: flowProps.values()) {
			HashMap<String, Object> propObj = new HashMap<String, Object>();
			String source = fprop.getSource();
			String inheritedSource = fprop.getInheritedSource();
			
			propObj.put("source", source);
			if (inheritedSource != null) {
				propObj.put("inherited", inheritedSource);
			}
			props.add(propObj);
		}
		flowObj.put("properties", props);
		
		ArrayList<Object> nodes = new ArrayList<Object>();
		for (ExecutableNode node: executableNodes.values()) {
			nodes.add(node.toObject());
		}
		flowObj.put("nodes", nodes);

		return flowObj;
	}

	public void setFailureAction(FailureAction action) {
		failureAction = action;
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
		exFlow.flowParameters = new HashMap<String, String>((Map<String,String>)flowObj.get("flowParameters"));
		
		// Failure notification
		if (flowObj.containsKey("notifyOnFirstFailure")) {
			exFlow.notifyOnFirstFailure = (Boolean)flowObj.get("notifyOnFirstFailure");
		}
		if (flowObj.containsKey("notifyOnLastFailure")) {
			exFlow.notifyOnLastFailure = (Boolean)flowObj.get("notifyOnLastFailure");
		}
		
		// Failure action
		if (flowObj.containsKey("failureAction")) {
			exFlow.failureAction = FailureAction.valueOf((String)flowObj.get("failureAction"));
		}
		exFlow.pipelineLevel = (Integer)flowObj.get("pipelineLevel");
		
		// Copy nodes
		List<Object> nodes = (List<Object>)flowObj.get("nodes");
		for (Object nodeObj: nodes) {
			ExecutableNode node = ExecutableNode.createNodeFromObject(nodeObj, exFlow);
			exFlow.executableNodes.put(node.getId(), node);
		}

		List<Object> properties = (List<Object>)flowObj.get("properties");
		for (Object propNode : properties) {
			HashMap<String, Object> fprop = (HashMap<String, Object>)propNode;
			String source = (String)fprop.get("source");
			String inheritedSource = (String)fprop.get("inherited");
			
			FlowProps flowProps = new FlowProps(inheritedSource, source);
			exFlow.flowProps.put(source, flowProps);
		}
		
		// Success emails
		exFlow.setSuccessEmails((List<String>)flowObj.get("successEmails"));
		// Failure emails
		exFlow.setFailureEmails((List<String>)flowObj.get("failureEmails"));
		
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

		submitTime = getLongFromObject(flowObj.get("submitTime"));
		startTime = getLongFromObject(flowObj.get("startTime"));
		endTime = getLongFromObject(flowObj.get("endTime"));
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

	public boolean isSubmitted() {
		return submitted;
	}

	public void setSubmitted(boolean submitted) {
		this.submitted = submitted;
	}

	public void setPipelineLevel(int level) {
		pipelineLevel = level;
	}
	
	public void setNotifyOnFirstFailure(boolean notify) {
		this.notifyOnFirstFailure = notify;
	}
	
	public void setNotifyOnLastFailure(boolean notify) {
		this.notifyOnLastFailure = notify;
	}
	
	public static class ExecutableNode {
		private String id;

		private String type;
		private String jobPropsSource;
		private String inheritPropsSource;
		private String outputPropsSource;
		private Status status = Status.READY;
		private long startTime = -1;
		private long endTime = -1;
		private int level = 0;
		private ExecutableFlow flow;
		
		private Set<String> inNodes = new HashSet<String>();
		private Set<String> outNodes = new HashSet<String>();
		
		private ExecutableNode(Node node, ExecutableFlow flow) {
			id = node.getId();
			type = node.getType();
			jobPropsSource = node.getJobSource();
			inheritPropsSource = node.getPropsSource();
			status = Status.READY;
			level = node.getLevel();
			this.flow = flow;
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
			
			if (outputPropsSource != null) {
				objMap.put("outputSource", outputPropsSource);
			}
			
			return objMap;
		}

		@SuppressWarnings("unchecked")
		public static ExecutableNode createNodeFromObject(Object obj, ExecutableFlow flow) {
			ExecutableNode exNode = new ExecutableNode();
			
			HashMap<String, Object> objMap = (HashMap<String,Object>)obj;
			exNode.id = (String)objMap.get("id");
			exNode.jobPropsSource = (String)objMap.get("jobSource");
			exNode.inheritPropsSource = (String)objMap.get("propSource");
			exNode.outputPropsSource = (String)objMap.get("outputSource");
			exNode.type = (String)objMap.get("jobType");
			exNode.status = Status.valueOf((String)objMap.get("status"));
			
			exNode.inNodes.addAll( (List<String>)objMap.get("inNodes") );
			exNode.outNodes.addAll( (List<String>)objMap.get("outNodes") );
			
			exNode.startTime = getLongFromObject(objMap.get("startTime"));
			exNode.endTime = getLongFromObject(objMap.get("endTime"));
			exNode.level = (Integer)objMap.get("level");
			
			exNode.flow = flow;
			
			return exNode;
		}
		
		@SuppressWarnings("unchecked")
		public void updateNodeFromObject(Object obj) {
			HashMap<String, Object> objMap = (HashMap<String,Object>)obj;
			status = Status.valueOf((String)objMap.get("status"));

			startTime = getLongFromObject(objMap.get("startTime"));
			endTime = getLongFromObject(objMap.get("endTime"));
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
		
		public ExecutableFlow getFlow() {
			return flow;
		}
	}
}
