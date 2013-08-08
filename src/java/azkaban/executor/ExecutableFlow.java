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
import java.util.Map;
import java.util.Set;

import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.project.Project;
import azkaban.utils.JSONUtils;

public class ExecutableFlow extends ExecutableFlowBase {
	public static final String EXECUTIONID_PARAM = "executionId";
	public static final String EXECUTIONPATH_PARAM ="executionPath";
	public static final String EXECUTIONOPTIONS_PARAM ="executionOptions";
	public static final String PROJECTID_PARAM ="projectId";
	public static final String SCHEDULEID_PARAM ="scheduleId";
	public static final String SUBMITUSER_PARAM = "submitUser";
	public static final String SUBMITTIME_PARAM = "submitUser";
	public static final String VERSION_PARAM = "version";
	public static final String PROXYUSERS_PARAM = "proxyUsers";
	
	private int executionId = -1;
	private int scheduleId = -1;
	private int projectId;
	private int version;
	private long submitTime = -1;
	private String submitUser;
	private String executionPath;
	private HashMap<String, FlowProps> flowProps = new HashMap<String, FlowProps>();
	
	private HashSet<String> proxyUsers = new HashSet<String>();
	private ExecutionOptions executionOptions;
	
	public ExecutableFlow(Project project, Flow flow) {
		this.projectId = project.getId();
		this.version = project.getVersion();
		this.scheduleId = -1;

		this.setFlow(project, flow);
	}
	
	public ExecutableFlow(Flow flow) {
		this.setFlow(null, flow);
	}
	
	public ExecutableFlow() {
	}

	@Override
	public String getId() {
		return getFlowId();
	}
	
	@Override
	public ExecutableFlow getExecutableFlow() {
		return this;
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
	
	private void setFlow(Project project, Flow flow) {
		super.setFlow(project, flow, null);
		executionOptions = new ExecutionOptions();

		if (flow.getSuccessEmails() != null) {
			executionOptions.setSuccessEmails(flow.getSuccessEmails());
		}
		if (flow.getFailureEmails() != null) {
			executionOptions.setFailureEmails(flow.getFailureEmails());
		}
	}
	
	public int getExecutionId() {
		return executionId;
	}

	public void setExecutionId(int executionId) {
		this.executionId = executionId;
	}

	@Override
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
	
	public String getSubmitUser() {
		return submitUser;
	}

	public void setSubmitUser(String submitUser) {
		this.submitUser = submitUser;
	}
	
	@Override
	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}
	
	public long getSubmitTime() {
		return submitTime;
	}
	
	public void setSubmitTime(long submitTime) {
		this.submitTime = submitTime;
	}
	
	public Map<String,Object> toObject() {
		HashMap<String, Object> flowObj = new HashMap<String, Object>();
		fillMapFromExecutable(flowObj);
		
		flowObj.put(EXECUTIONID_PARAM, executionId);
		flowObj.put(EXECUTIONPATH_PARAM, executionPath);		
		flowObj.put(PROJECTID_PARAM, projectId);
		
		if(scheduleId >= 0) {
			flowObj.put(SCHEDULEID_PARAM, scheduleId);
		}

		flowObj.put(SUBMITUSER_PARAM, submitUser);
		flowObj.put(VERSION_PARAM, version);
		
		flowObj.put(EXECUTIONOPTIONS_PARAM, this.executionOptions.toObject());
		flowObj.put(VERSION_PARAM, version);
		
		ArrayList<String> proxyUserList = new ArrayList<String>(proxyUsers);
		flowObj.put(PROXYUSERS_PARAM, proxyUserList);

		flowObj.put(SUBMITTIME_PARAM, submitTime);
		
		return flowObj;
	}
	
	@SuppressWarnings("unchecked")
	public static ExecutableFlow createExecutableFlowFromObject(Object obj) {
		ExecutableFlow exFlow = new ExecutableFlow();
		HashMap<String, Object> flowObj = (HashMap<String,Object>)obj;
		
		exFlow.fillExecutableFromMapObject(flowObj);
		exFlow.executionId = (Integer)flowObj.get(EXECUTIONID_PARAM);
		exFlow.executionPath = (String)flowObj.get(EXECUTIONPATH_PARAM);

		exFlow.projectId = (Integer)flowObj.get(PROJECTID_PARAM);
		if (flowObj.containsKey(SCHEDULEID_PARAM)) {
			exFlow.scheduleId = (Integer)flowObj.get(SCHEDULEID_PARAM);
		}
		exFlow.submitUser = (String)flowObj.get(SUBMITUSER_PARAM);
		exFlow.version = (Integer)flowObj.get(VERSION_PARAM);
		
		exFlow.submitTime = JSONUtils.getLongFromObject(flowObj.get(SUBMITTIME_PARAM));
		
		if (flowObj.containsKey(EXECUTIONOPTIONS_PARAM)) {
			exFlow.executionOptions = ExecutionOptions.createFromObject(flowObj.get(EXECUTIONOPTIONS_PARAM));
		}
		else {
			// for backwards compatibility should remove in a few versions.
			exFlow.executionOptions = ExecutionOptions.createFromObject(flowObj);
		}
		
		if(flowObj.containsKey(PROXYUSERS_PARAM)) {
			ArrayList<String> proxyUserList = (ArrayList<String>) flowObj.get(PROXYUSERS_PARAM);
			exFlow.addAllProxyUsers(proxyUserList);
		}
		
		return exFlow;
	}
	
	public Map<String, Object> toUpdateObject(long lastUpdateTime) {
		Map<String, Object> updateData = super.toUpdateObject(lastUpdateTime);
		updateData.put(EXECUTIONID_PARAM, this.executionId);
		return updateData;
	}
}