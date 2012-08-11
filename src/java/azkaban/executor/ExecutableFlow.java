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
	private HashMap<String, ExecutableNode> executableNodes;
	private ArrayList<String> startNodes = new ArrayList<String>();
	
	public enum Status {
		FAILED, SUCCEEDED, RUNNING, WAITING, IGNORED, READY
	}
	
	public ExecutableFlow(String id, Flow flow) {
		this.executionId = id;
		this.projectId = flow.getProjectId();
		this.flowId = flow.getId();
		
		this.setFlow(flow);
	}
	
	private void setFlow(Flow flow) {
		executableNodes = new HashMap<String, ExecutableNode>();
		
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

	public Map<String,Object> toObject() {
		HashMap<String, Object> flowObj = new HashMap<String, Object>();
		flowObj.put("type", "executableflow");
		flowObj.put("execution.id", executionId);
		flowObj.put("execution.path", executionPath);
		flowObj.put("flow.id", flowId);
		flowObj.put("project.id", projectId);
		
		ArrayList<Object> nodes = new ArrayList<Object>();
		for (ExecutableNode node: executableNodes.values()) {
			nodes.add(node.toObject());
		}
		flowObj.put("nodes", nodes);

		return flowObj;
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
	
	private static class ExecutableNode {
		private String id;
		private String type;
		private String jobPropsSource;
		private String inheritPropsSource;
		private Status status;
		private long startTime = -1;
		private long endTime = -1;
		
		private Set<String> inNodes = new HashSet<String>();
		private Set<String> outNodes = new HashSet<String>();
		
		private ExecutableNode(Node node) {
			id = node.getId();
			type = node.getType();
			jobPropsSource = node.getJobSource();
			inheritPropsSource = node.getPropsSource();
			status = Status.READY;
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
			objMap.put("job.source", jobPropsSource);
			objMap.put("prop.source", inheritPropsSource);
			objMap.put("job.type", type);
			objMap.put("status", status.toString());
			objMap.put("in.nodes", inNodes);
			objMap.put("out.nodes", outNodes);
			objMap.put("start.time", startTime);
			objMap.put("end.time", endTime);
			
			return objMap;
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
	}
}
