/*
 * Copyright 2012 LinkedIn Corp.
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

import azkaban.executor.ExecutableNode.Attempt;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;
import azkaban.utils.JSONUtils;

public class ExecutableFlow {
	private int executionId = -1;
	private String flowId;
	private int scheduleId = -1;
	private int projectId;
	private int version;

	private String executionPath;

	private HashMap<String, FlowProps> flowProps = new HashMap<String, FlowProps>();
	private HashMap<String, ExecutableNode> executableNodes = new HashMap<String, ExecutableNode>();
	private ArrayList<String> startNodes;
	private ArrayList<String> endNodes;

	private long submitTime = -1;
	private long startTime = -1;
	private long endTime = -1;
	private long updateTime = -1;

	private Status flowStatus = Status.READY;
	private String submitUser;

	private HashSet<String> proxyUsers = new HashSet<String>();
	private ExecutionOptions executionOptions;

	public ExecutableFlow(Flow flow) {
		this.projectId = flow.getProjectId();
		this.scheduleId = -1;
		this.flowId = flow.getId();
		this.version = flow.getVersion();
		this.setFlow(flow);
	}

	public ExecutableFlow(int executionId, Flow flow) {
		this.projectId = flow.getProjectId();
		this.scheduleId = -1;
		this.flowId = flow.getId();
		this.version = flow.getVersion();
		this.executionId = executionId;

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

	public void addAllProxyUsers(Collection<String> proxyUsers) {
		this.proxyUsers.addAll(proxyUsers);
	}

	public Set<String> getProxyUsers() {
		return new HashSet<String>(this.proxyUsers);
	}

	public void setExecutionOptions(ExecutionOptions options) {
		executionOptions = options;
	}

	public ExecutionOptions getExecutionOptions() {
		return executionOptions;
	}

	private void setFlow(Flow flow) {
		executionOptions = new ExecutionOptions();

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
			executionOptions.setSuccessEmails(flow.getSuccessEmails());
		}
		if (flow.getFailureEmails() != null) {
			executionOptions.setFailureEmails(flow.getFailureEmails());
		}
		executionOptions.setMailCreator(flow.getMailCreator());

		flowProps.putAll(flow.getAllFlowProps());
	}

	public List<String> getStartNodes() {
		if (startNodes == null) {
			startNodes = new ArrayList<String>();
			for (ExecutableNode node: executableNodes.values()) {
				if (node.getInNodes().isEmpty()) {
					startNodes.add(node.getJobId());
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
					endNodes.add(node.getJobId());
				}
			}
		}

		return endNodes;
	}

	public boolean setNodeStatus(String nodeId, Status status) {
		ExecutableNode exNode = executableNodes.get(nodeId);
		if (exNode == null) {
			return false;
		}
		exNode.setStatus(status);
		return true;
	}

	public void setProxyNodes(int externalExecutionId, String nodeId) {
		ExecutableNode exNode = executableNodes.get(nodeId);
		if (exNode == null) {
			return;
		}

		exNode.setExternalExecutionId(externalExecutionId);
	}

	public int getExecutionId() {
		return executionId;
	}

	public void setExecutionId(int executionId) {
		this.executionId = executionId;

		for (ExecutableNode node: executableNodes.values()) {
			node.setExecutionId(executionId);
		}
	}

	public String getFlowId() {
		return flowId;
	}

	public void setFlowId(String flowId) {
		this.flowId = flowId;
	}

	public int getProjectId() {
		return projectId;
	}

	public void setProjectId(int projectId) {
		this.projectId = projectId;
	}

	public int getScheduleId() {
		return scheduleId;
	}

	public void setScheduleId(int scheduleId) {
		this.scheduleId = scheduleId;
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

	public Map<String, Object> toObject() {
		HashMap<String, Object> flowObj = new HashMap<String, Object>();
		flowObj.put("type", "executableflow");
		flowObj.put("executionId", executionId);
		flowObj.put("executionPath", executionPath);
		flowObj.put("flowId", flowId);
		flowObj.put("projectId", projectId);

		if (scheduleId >= 0) {
			flowObj.put("scheduleId", scheduleId);
		}
		flowObj.put("submitTime", submitTime);
		flowObj.put("startTime", startTime);
		flowObj.put("endTime", endTime);
		flowObj.put("status", flowStatus.toString());
		flowObj.put("submitUser", submitUser);
		flowObj.put("version", version);

		flowObj.put("executionOptions", this.executionOptions.toObject());
		flowObj.put("version", version);

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

		ArrayList<String> proxyUserList = new ArrayList<String>(proxyUsers);
		flowObj.put("proxyUsers", proxyUserList);

		return flowObj;
	}

	public Object toUpdateObject(long lastUpdateTime) {
		Map<String, Object> updateData = new HashMap<String, Object>();
		updateData.put("execId", this.executionId);
		updateData.put("status", this.flowStatus.getNumVal());
		updateData.put("startTime", this.startTime);
		updateData.put("endTime", this.endTime);
		updateData.put("updateTime", this.updateTime);

		List<Map<String, Object>> updatedNodes = new ArrayList<Map<String, Object>>();
		for (ExecutableNode node: executableNodes.values()) {

			if (node.getUpdateTime() > lastUpdateTime) {
				Map<String, Object> updatedNodeMap = new HashMap<String, Object>();
				updatedNodeMap.put("jobId", node.getJobId());
				updatedNodeMap.put("status", node.getStatus().getNumVal());
				updatedNodeMap.put("startTime", node.getStartTime());
				updatedNodeMap.put("endTime", node.getEndTime());
				updatedNodeMap.put("updateTime", node.getUpdateTime());
				updatedNodeMap.put("attempt", node.getAttempt());

				if (node.getAttempt() > 0) {
					ArrayList<Map<String, Object>> pastAttempts = new ArrayList<Map<String, Object>>();
					for (Attempt attempt: node.getPastAttemptList()) {
						pastAttempts.add(attempt.toObject());
					}
					updatedNodeMap.put("pastAttempts", pastAttempts);
				}

				updatedNodes.add(updatedNodeMap);
			}
		}

		updateData.put("nodes", updatedNodes);
		return updateData;
	}

	@SuppressWarnings("unchecked")
	public void applyUpdateObject(Map<String, Object> updateData) {
		List<Map<String, Object>> updatedNodes = (List<Map<String, Object>>)updateData.get("nodes");
		for (Map<String, Object> node: updatedNodes) {
			String jobId = (String)node.get("jobId");
			Status status = Status.fromInteger((Integer)node.get("status"));
			long startTime = JSONUtils.getLongFromObject(node.get("startTime"));
			long endTime = JSONUtils.getLongFromObject(node.get("endTime"));
			long updateTime = JSONUtils.getLongFromObject(node.get("updateTime"));

			ExecutableNode exNode = executableNodes.get(jobId);
			exNode.setEndTime(endTime);
			exNode.setStartTime(startTime);
			exNode.setUpdateTime(updateTime);
			exNode.setStatus(status);

			int attempt = 0;
			if (node.containsKey("attempt")) {
				attempt = (Integer)node.get("attempt");
				if (attempt > 0) {
					exNode.updatePastAttempts((List<Object>)node.get("pastAttempts"));
				}
			}

			exNode.setAttempt(attempt);
		}

		this.flowStatus = Status.fromInteger((Integer)updateData.get("status"));

		this.startTime = JSONUtils.getLongFromObject(updateData.get("startTime"));
		this.endTime = JSONUtils.getLongFromObject(updateData.get("endTime"));
		this.updateTime = JSONUtils.getLongFromObject(updateData.get("updateTime"));
	}

	@SuppressWarnings("unchecked")
	public static ExecutableFlow createExecutableFlowFromObject(Object obj) {
		ExecutableFlow exFlow = new ExecutableFlow();

		HashMap<String, Object> flowObj = (HashMap<String, Object>)obj;
		exFlow.executionId = (Integer)flowObj.get("executionId");
		exFlow.executionPath = (String)flowObj.get("executionPath");
		exFlow.flowId = (String)flowObj.get("flowId");
		exFlow.projectId = (Integer)flowObj.get("projectId");
		if (flowObj.containsKey("scheduleId")) {
			exFlow.scheduleId = (Integer)flowObj.get("scheduleId");
		}
		exFlow.submitTime = JSONUtils.getLongFromObject(flowObj.get("submitTime"));
		exFlow.startTime = JSONUtils.getLongFromObject(flowObj.get("startTime"));
		exFlow.endTime = JSONUtils.getLongFromObject(flowObj.get("endTime"));
		exFlow.flowStatus = Status.valueOf((String)flowObj.get("status"));
		exFlow.submitUser = (String)flowObj.get("submitUser");
		exFlow.version = (Integer)flowObj.get("version");

		if (flowObj.containsKey("executionOptions")) {
			exFlow.executionOptions = ExecutionOptions.createFromObject(flowObj.get("executionOptions"));
		}
		else {
			// for backawards compatibility should remove in a few versions.
			exFlow.executionOptions = ExecutionOptions.createFromObject(flowObj);
		}

		// Copy nodes
		List<Object> nodes = (List<Object>)flowObj.get("nodes");
		for (Object nodeObj: nodes) {
			ExecutableNode node = ExecutableNode.createNodeFromObject(nodeObj, exFlow);
			exFlow.executableNodes.put(node.getJobId(), node);
		}

		List<Object> properties = (List<Object>)flowObj.get("properties");
		for (Object propNode: properties) {
			HashMap<String, Object> fprop = (HashMap<String, Object>)propNode;
			String source = (String)fprop.get("source");
			String inheritedSource = (String)fprop.get("inherited");

			FlowProps flowProps = new FlowProps(inheritedSource, source);
			exFlow.flowProps.put(source, flowProps);
		}

		if (flowObj.containsKey("proxyUsers")) {
			ArrayList<String> proxyUserList = (ArrayList<String>)flowObj.get("proxyUsers");
			exFlow.addAllProxyUsers(proxyUserList);
		}

		return exFlow;
	}

	@SuppressWarnings("unchecked")
	public void updateExecutableFlowFromObject(Object obj) {
		HashMap<String, Object> flowObj = (HashMap<String, Object>)obj;

		submitTime = JSONUtils.getLongFromObject(flowObj.get("submitTime"));
		startTime = JSONUtils.getLongFromObject(flowObj.get("startTime"));
		endTime = JSONUtils.getLongFromObject(flowObj.get("endTime"));
		flowStatus = Status.valueOf((String)flowObj.get("status"));

		List<Object> nodes = (List<Object>)flowObj.get("nodes");
		for (Object nodeObj: nodes) {
			HashMap<String, Object> nodeHash = (HashMap<String, Object>)nodeObj;
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

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}
	
	public static boolean isFinished(ExecutableFlow flow) {
		switch(flow.getStatus()) {
		case SUCCEEDED:
		case FAILED:
		case KILLED:
			return true;
		default:
			return false;
		}
	}
}
