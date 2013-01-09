/*
 * Copyright 2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import azkaban.executor.ExecutableFlow.Status;
import azkaban.flow.Node;
import azkaban.utils.JSONUtils;

public class ExecutableNode {
	private String jobId;
	private int executionId;
	private String type;
	private String jobPropsSource;
	private String inheritPropsSource;
	private String outputPropsSource;
	private Status status = Status.READY;
	private long startTime = -1;
	private long endTime = -1;
	private long updateTime = -1;
	private int level = 0;
	private ExecutableFlow flow;
	
	private Set<String> inNodes = new HashSet<String>();
	private Set<String> outNodes = new HashSet<String>();
	
	public ExecutableNode(Node node, ExecutableFlow flow) {
		jobId = node.getId();
		executionId = flow.getExecutionId();
		type = node.getType();
		jobPropsSource = node.getJobSource();
		inheritPropsSource = node.getPropsSource();
		status = Status.READY;
		level = node.getLevel();
		this.flow = flow;
	}
	
	public ExecutableNode() {
	}
	
	public void setExecutableFlow(ExecutableFlow flow) {
		this.flow = flow;
	}
	
	public void setExecutionId(int id) {
		executionId = id;
	}
	
	public int getExecutionId() {
		return executionId;
	}
	
	public String getJobId() {
		return jobId;
	}

	public void setJobId(String id) {
		this.jobId = id;
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
		objMap.put("id", jobId);
		objMap.put("jobSource", jobPropsSource);
		objMap.put("propSource", inheritPropsSource);
		objMap.put("jobType", type);
		objMap.put("status", status.toString());
		objMap.put("inNodes", new ArrayList<String>(inNodes));
		objMap.put("outNodes", new ArrayList<String>(outNodes));
		objMap.put("startTime", startTime);
		objMap.put("endTime", endTime);
		objMap.put("updateTime", updateTime);
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
		exNode.executionId = flow == null ? 0 : flow.getExecutionId();
		exNode.jobId = (String)objMap.get("id");
		exNode.jobPropsSource = (String)objMap.get("jobSource");
		exNode.inheritPropsSource = (String)objMap.get("propSource");
		exNode.outputPropsSource = (String)objMap.get("outputSource");
		exNode.type = (String)objMap.get("jobType");
		exNode.status = Status.valueOf((String)objMap.get("status"));
		
		exNode.inNodes.addAll( (List<String>)objMap.get("inNodes") );
		exNode.outNodes.addAll( (List<String>)objMap.get("outNodes") );
		
		exNode.startTime = JSONUtils.getLongFromObject(objMap.get("startTime"));
		exNode.endTime = JSONUtils.getLongFromObject(objMap.get("endTime"));
		exNode.updateTime = JSONUtils.getLongFromObject(objMap.get("updateTime"));
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

	public long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}
}