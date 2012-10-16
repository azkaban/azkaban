package azkaban.executor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import azkaban.executor.ExecutableFlow.Status;
import azkaban.flow.Node;
import azkaban.utils.JSONUtils;

public class ExecutableNode {
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
	
	public ExecutableNode(Node node, ExecutableFlow flow) {
		id = node.getId();
		type = node.getType();
		jobPropsSource = node.getJobSource();
		inheritPropsSource = node.getPropsSource();
		status = Status.READY;
		level = node.getLevel();
		this.flow = flow;
	}
	
	public ExecutableNode() {
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
		
		exNode.startTime = JSONUtils.getLongFromObject(objMap.get("startTime"));
		exNode.endTime = JSONUtils.getLongFromObject(objMap.get("endTime"));
		exNode.level = (Integer)objMap.get("level");
		
		exNode.flow = flow;
		
		return exNode;
	}
	
	@SuppressWarnings("unchecked")
	public void updateNodeFromObject(Object obj) {
		HashMap<String, Object> objMap = (HashMap<String,Object>)obj;
		status = Status.valueOf((String)objMap.get("status"));

		startTime = JSONUtils.getLongFromObject(objMap.get("startTime"));
		endTime = JSONUtils.getLongFromObject(objMap.get("endTime"));
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