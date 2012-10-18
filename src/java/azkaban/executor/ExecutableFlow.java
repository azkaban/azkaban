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
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class ExecutableFlow {
	private String executionId;
	private String flowId;
	private String projectId;
	private String executionPath;
	
	private HashMap<String, FlowProps> flowProps = new HashMap<String, FlowProps>();
	private HashMap<String, ExecutableNode> executableNodes = new HashMap<String, ExecutableNode>();
	private ArrayList<String> startNodes;
	private ArrayList<String> endNodes;
	
	private ArrayList<String> failureEmails = new ArrayList<String>();
	private ArrayList<String> successEmails = new ArrayList<String>();
	
	private long submitTime = -1;
	private long startTime = -1;
	private long endTime = -1;
	private long updateTime = -1;
	
	private int updateNumber = 0;
	private Status flowStatus = Status.READY;
	private String submitUser;
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
	
	public long getUpdateTime() {
		return updateTime;
	}
	
	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
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
		
		if (flow.getSuccessEmails() != null) {
			successEmails = new ArrayList<String>(flow.getSuccessEmails());
		}
		if (flow.getFailureEmails() != null) {
			failureEmails = new ArrayList<String>(flow.getFailureEmails());
		}
		flowProps.putAll(flow.getAllFlowProps());
	}

	public List<String> getStartNodes() {
		if (startNodes == null) {
			startNodes = new ArrayList<String>();
			for (ExecutableNode node: executableNodes.values()) {
				if (node.getInNodes().isEmpty()) {
					startNodes.add(node.getId());
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
					endNodes.add(node.getId());
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
	
	public List<String> getFailureEmails() {
		return this.failureEmails;
	}
	
	public void setSuccessEmails(List<String> emails) {
		this.successEmails = emails == null ? new ArrayList<String>() : new ArrayList<String>(emails);
	}
	
	public List<String> getSuccessEmails() {
		return this.successEmails;
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
	
	public FailureAction getFailureAction() {
		return failureAction;
	}
	
	@SuppressWarnings("unchecked")
	public static ExecutableFlow createExecutableFlowFromObject(Object obj) {
		ExecutableFlow exFlow = new ExecutableFlow();
		
		HashMap<String, Object> flowObj = (HashMap<String,Object>)obj;
		exFlow.executionId = (String)flowObj.get("executionId");
		exFlow.executionPath = (String)flowObj.get("executionPath");
		exFlow.flowId = (String)flowObj.get("flowId");
		exFlow.projectId = (String)flowObj.get("projectId");
		exFlow.submitTime = JSONUtils.getLongFromObject(flowObj.get("submitTime"));
		exFlow.startTime = JSONUtils.getLongFromObject(flowObj.get("startTime"));
		exFlow.endTime = JSONUtils.getLongFromObject(flowObj.get("endTime"));
		exFlow.flowStatus = Status.valueOf((String)flowObj.get("status"));
		exFlow.submitUser = (String)flowObj.get("submitUser");
		if (flowObj.containsKey("flowParameters")) {
			exFlow.flowParameters = new HashMap<String, String>((Map<String,String>)flowObj.get("flowParameters"));
		}
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
	
	@SuppressWarnings("unchecked")
	public void updateExecutableFlowFromObject(Object obj) {
		HashMap<String, Object> flowObj = (HashMap<String,Object>)obj;

		submitTime = JSONUtils.getLongFromObject(flowObj.get("submitTime"));
		startTime = JSONUtils.getLongFromObject(flowObj.get("startTime"));
		endTime = JSONUtils.getLongFromObject(flowObj.get("endTime"));
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

	public void setPipelineLevel(int level) {
		pipelineLevel = level;
	}
	
	public void setNotifyOnFirstFailure(boolean notify) {
		this.notifyOnFirstFailure = notify;
	}
	
	public void setNotifyOnLastFailure(boolean notify) {
		this.notifyOnLastFailure = notify;
	}
	
	public boolean getNotifyOnFirstFailure() {
		return this.notifyOnFirstFailure;
	}
	
	public boolean getNotifyOnLastFailure() {
		return this.notifyOnLastFailure;
	}
}
