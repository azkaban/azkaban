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
	private HashMap<String, Props> sourceProps = new HashMap<String, Props>();
	private HashMap<String, FlowProps> flowProps = new HashMap<String, FlowProps>();
	private HashMap<String, ExecutableNode> executableNodes;
	private ArrayList<String> startNodes = new ArrayList<String>();
	
	public enum Status {
		FAILED, SUCCEEDED, RUNNING, WAITING, IGNORED, READY
	}
	
	private ExecutableFlow() {
		
	}
	
	public void run() {
		
	}
	
	public void setProps(String source, Props props) {
		sourceProps.put(source, props);
	}
	
	public void setStatus(String nodeId, Status status) {
		ExecutableNode exNode = executableNodes.get(nodeId);
		exNode.setStatus(status);
	}
	
	public static ExecutableFlow createExecutableFlow(Flow flow, HashMap<String, Props> sourceProps) {
		ExecutableFlow exflow = new ExecutableFlow();
		exflow.flowId = flow.getId();
		
		// We make a copy so that it's effectively immutable
		exflow.sourceProps = new HashMap<String, Props>();
		exflow.sourceProps.putAll(sourceProps);
		
		HashMap<String, ExecutableNode> nodeMap = new HashMap<String, ExecutableNode>();
		
		for (Node node: flow.getNodes()) {
			String id = node.getId();
			ExecutableNode exNode = new ExecutableNode(node);
			nodeMap.put(id, exNode);
		}
		
		for (Edge edge: flow.getEdges()) {
			ExecutableNode sourceNode = nodeMap.get(edge.getSourceId());
			ExecutableNode targetNode = nodeMap.get(edge.getTargetId());
			
			sourceNode.addOutNode(edge.getTargetId());
			targetNode.addInNode(edge.getSourceId());
		}
		
		for (ExecutableNode node : nodeMap.values()) {
			if (node.getInNodes().size()==0) {
				exflow.startNodes.add(node.id);
			}
		}
		
		exflow.executableNodes = nodeMap;
		return exflow;
	}
	
	public String getExecutionId() {
		return executionId;
	}

	public void setExecutionId(String executionId) {
		this.executionId = executionId;
	}

	private static class ExecutableNode {
		private String id;
		private String jobPropsSource;
		private String inheritPropsSource;
		private Status status;
		
		private Set<String> inNodes = new HashSet<String>();
		private Set<String> outNodes = new HashSet<String>();
		
		private ExecutableNode(Node node) {
			id = node.getId();
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
	}
}
